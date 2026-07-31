"""Extract 階段：爬取全臺夜市清單，並向 Google Maps API 取得地理資訊。"""

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception, stop_after_attempt

from src.util.crawling_utils import RETRY_ATTEMPTS, RETRY_WAIT, is_transient
from src.util.logger_crtx import get_logger
from src.util.paths import RAW_DATA_DIR

logger = get_logger(__name__)

# 指定要爬取的網址
night_markets_wiki_url = "https://zh.wikipedia.org/zh-tw/%E8%87%BA%E7%81%A3%E5%A4%9C%E5%B8%82%E5%88%97%E8%A1%A8"

# 準備headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
}

cities_per_region = {
    "北部": ["臺北市", "新北市", "基隆市", "桃園市", "新竹市", "新竹縣"],
    "中部": ["臺中市", "彰化縣", "南投縣", "雲林縣", "苗栗縣"],
    "南部": ["臺南市", "高雄市", "屏東縣", "嘉義市", "嘉義縣"],
    "東部": ["花蓮縣", "臺東縣", "宜蘭縣"],
    "離島": ["澎湖縣", "金門縣", "連江縣"],
}


def find_tw_night_markets_list(url: str, headers: dict, cities_per_region: dict) -> str:
    """Request the url Wikipedia to get the list of night markets in Taiwan.

    The obtained list will be saved in a new csv file of which the file path is
    return value.

    :param url: URL of Wikipedia summarizing the night markets in Taiwan.
    :type url: str
    :param headers: Headers required for Request.get() function.
    :type headers: dict
    :param cities_per_region: Dict to map the region that a night market is located
    (north, south, west, east, outlying islands).
    :type cities_per_region: dict

    :returns: String of the path of generated csv file. 回傳值代表該檔案確實已寫出
    且含有夜市資料；任何失敗都會拋出例外而非回傳路徑（ADR-0005）。
    :rtype: str

    :raises requests.exceptions.RequestException: 抓取失敗或回應非 2xx。
    :raises ValueError: 頁面解析不到任何夜市。
    """
    # 變數宣告
    response = None

    # 定義存檔路徑，並確保資料夾存在（路徑基準見 ADR-0007）
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    csvfile_name = RAW_DATA_DIR / f"Taiwan_night_markets_list_{today}.csv"

    try:
        response = requests.get(url, headers=headers, timeout=120)
        # 非 2xx 直接轉成 HTTPError，交由下方的 except 分類後原樣拋出（ADR-0005）。
        response.raise_for_status()
        logger.info(f"====成功訪問{url}====")
        soup = BeautifulSoup(response.text, "html.parser")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout while fetching from {url}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error while fetching from {url}")
        raise
    except requests.exceptions.HTTPError:
        logger.error(f"HTTP error while fetching from {url}")
        raise
    except Exception:
        logger.error(f"Unexpected error while fetching from {url}")
        raise
    else:
        regionlst = []
        citylst = []
        nm_namelst = []
        nm_addresslst = []
        city_name = soup.find_all("h3")  # 基隆市、臺北市、......、連江縣
        # print(len(city_name)) # 22個縣市
        tables = soup.find_all("table", class_="wikitable")
        # print(len(tables)) # 22個表格
        for i in range(len(tables)):
            table = tables[i]
            rows = table.find_all("tr")
            for row in rows[1:]:  # r = 一處夜市、row[1:]代表跳過表格標題列
                tds = row.find_all("td")  # tds = 表格所有欄位

                # 取得夜市名稱
                nightmarket_name = tds[0].text.strip()
                if "夜市" not in nightmarket_name and "商圈" not in nightmarket_name:
                    continue

                # 合法夜市名稱才能列入清單
                nm_namelst.append(nightmarket_name)

                # 取得夜市所屬街道地址
                nightmarket_address = tds[1].text.strip()
                nm_addresslst.append(nightmarket_address)

                # 補上所屬縣市
                citylst.append(city_name[i].text.strip())

                # 補上縣市所屬分區(北、中、南、....)
                for region, cities in cities_per_region.items():
                    if city_name[i].text.strip() in cities:
                        regionlst.append(region)

        # 裝成DataFrame
        df = pd.DataFrame(
            {
                "Region": regionlst,
                "City": citylst,
                "Night_market_name": nm_namelst,
                "Night_market_address": nm_addresslst,
            }
        )
        # 解析不到任何夜市代表頁面結構已變，不可產出空 CSV 讓下游繼續（ADR-0005）。
        if df.empty:
            raise ValueError(f"自 {url} 解析不到任何夜市，請檢查爬蟲程式")

        df.to_csv(csvfile_name, sep=",", encoding="utf-8-sig")
        logger.info(f"====Save the file successfully! {csvfile_name}====")
        return str(csvfile_name)


# 讀取 .env
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAP_API_KEY")

# 夜市查不到是常態（維基名稱不一定對得上 Google Maps），但大量查不到代表
# 系統性問題（金鑰失效、頁面改版）。門檻可調，並非算出來的值。
MAX_FAILURE_RATE = 0.5


def _require_api_key() -> None:
    """呼叫 Places API 前確認金鑰存在（ADR-0008）。

    沒有金鑰時 API 會回 REQUEST_DENIED，經 ADR-0006 的分類後訊息會說
    「金鑰無效」—— 但實際上是根本沒有金鑰，兩者的排查方向不同。

    Raises:
        ValueError: `GOOGLE_MAP_API_KEY` 未設定。
    """
    if not API_KEY:
        raise ValueError("未設定 GOOGLE_MAP_API_KEY，請檢查環境變數設置")


class PlacesAPIError(RuntimeError):
    """Google Places API 以 HTTP 200 回報的失敗。

    傳輸層成功（狀態碼 200），失敗寫在 body 的 status 欄位，
    因此無法用 `requests` 的例外體系表達。
    """


class TransientPlacesAPIError(PlacesAPIError):
    """暫時性失敗（配額超限、伺服器錯誤），值得重試。"""


class PermanentPlacesAPIError(PlacesAPIError):
    """永久性失敗（授權或參數問題），重試只會浪費配額並延後告警。"""


# Google Places API 的失敗寫在 body 的 status 欄位，HTTP 狀態碼一律是 200，
# 因此必須顯式分類，否則所有失敗都會被誤判成「找不到地點」（ADR-0006）。
# 不在此表中的 status 都不是故障：OK 代表有結果，ZERO_RESULTS 與
# NOT_FOUND（Place Details 專有，place_id 已失效）代表查無資料。
# status 定義見：
# https://developers.google.com/maps/documentation/places/web-service/legacy/search-find-place#PlacesSearchStatus
API_STATUS_ERRORS: dict[str, tuple[type[PlacesAPIError], str]] = {
    "OVER_QUERY_LIMIT": (TransientPlacesAPIError, "配額或 QPS 超限"),
    "UNKNOWN_ERROR": (TransientPlacesAPIError, "伺服器暫時錯誤"),
    "REQUEST_DENIED": (
        PermanentPlacesAPIError,
        "請求遭拒，通常是 API 金鑰無效、未啟用對應的 Places API，或超出金鑰的授權範圍",
    ),
    "INVALID_REQUEST": (
        PermanentPlacesAPIError,
        "請求參數有誤，通常是缺少必要參數（如 input、inputtype 或 place_id），"
        "屬於呼叫端的程式錯誤",
    ),
}

# 查無資料的兩個 status：不是故障，僅供 `_has_data()` 內部判斷。
_NOT_FOUND_STATUSES = frozenset({"ZERO_RESULTS", "NOT_FOUND"})


def _has_data(data: dict, context: str) -> bool:
    """檢查 Places API 回應的 status，故障一律拋出（ADR-0006）。

    Parameters:
        data (dict): API 回應解碼後的 dict。
        context (str): 用於錯誤訊息的查詢對象（夜市名稱或 place_id）。

    Returns:
        bool: True 代表回應含可用資料；False 代表查無資料（不是故障）。

    Raises:
        TransientPlacesAPIError: 配額超限或伺服器錯誤，由 tenacity 的
        retry_on_transient_api wrapper 重試。
        PermanentPlacesAPIError: 授權或參數錯誤，重試無用，立即失敗。
    """
    status = data.get("status", "UNKNOWN_ERROR")

    if status in API_STATUS_ERRORS:
        error_type, hint = API_STATUS_ERRORS[status]
        raise error_type(
            f"Google Places API 回報 {status}（查詢對象：{context}）：{hint}"
        )

    # 如果不需要 raise 暫時性失敗或是永久性失敗，則改檢查是否為空資料後，回傳布林值。
    return status not in _NOT_FOUND_STATUSES


def _should_retry(exc: BaseException) -> bool:
    """這兩支 API 函式有兩種暫時性失敗來源，都值得重試（ADR-0006）。

    - 傳輸層：連線斷、Google 回 5xx／429 → `requests` 的例外，由 `is_transient` 認得
    - body 層：HTTP 200 但 status 是 OVER_QUERY_LIMIT → `TransientPlacesAPIError`

    兩者併成單一判斷式，而不是疊兩層裝飾器 —— 疊起來雖然條件互斥、
    行為正確，但重試上限會變成 3×3 的隱患，且讀者要推敲兩層的交互作用。
    """
    # TransientPlaceAPIError 代表 status_code = 200 但是 status 欄位暗示有錯誤；
    # is_transient() 代表 status_code == 429/5xx、TimeoutError 或 ConnectionError。
    # 意即，TransientPlaceAPIError、TimeoutError 、 ConnectionError、429/5xx 都會回傳 True。
    return isinstance(exc, TransientPlacesAPIError) or is_transient(exc)


# 使用 _should_retry 判斷是否為暫時性失敗，因為暫時性失敗適合重試。
retry_on_transient_api = retry(
    retry=retry_if_exception(_should_retry),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=RETRY_WAIT,
    reraise=True,
)


@retry_on_transient_api
def search_place_id(place_name: str) -> None | str:
    """Call the GoogleMap Place API to request the place IDs of each

    interested location.

    回傳 ``None`` 只代表 Google Maps 查無此地點（``ZERO_RESULTS``）；
    配額超限會重試，金鑰或參數錯誤則立即拋出（ADR-0006）。

    :param place_name: location name or shop name (e.g. night market name)
    :type place_name: str

    :returns: The place ID, or None when the place is genuinely not found.
    :rtype: None or str

    :raises TransientPlacesAPIError: 配額超限或伺服器錯誤，重試耗盡後拋出。
    :raises PermanentPlacesAPIError: 授權或參數錯誤。
    :raises requests.exceptions.RequestException: 傳輸層失敗，重試耗盡後拋出。
    """
    # 參考文件: https://developers.google.com/maps/documentation/places/web-service/legacy/search-find-place
    _require_api_key()

    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": place_name,
        "inputtype": "textquery",
        "fields": "place_id",
        "language": "zh-TW",
        "key": API_KEY,
    }
    try:
        response = requests.get(base_url, params=params, timeout=120)
        response.raise_for_status()
    except requests.exceptions.Timeout:  # 被 _should_entry() 判定為可重試
        logger.error(f"Timeout while fetching from {place_name}")
        raise
    except requests.exceptions.ConnectionError:  # 被 _should_entry() 判定為可重試
        logger.error(f"Connection error while fetching from {place_name}")
        raise
    except requests.exceptions.HTTPError:
        logger.error(f"HTTP error while fetching from {place_name}")
        raise
    except Exception:
        logger.error(f"Unexpected error while fetching from {place_name}")
        raise
    else:
        data = response.json()
        # 若 _has_data() raise TransientPlacesAPIError，則被 _should_entry() 判定為可重試
        if not _has_data(data, place_name):
            return None
        return data["candidates"][0]["place_id"]


@retry_on_transient_api
def get_place_details(place_id: str) -> dict | None:
    """Use the place ID and call the GoogleMap Place API to get detailed information

    of a location, including name, rating, formatted_address, opening_hours, URL to GoogleMap
    and geometry.

    回傳 ``None`` 只代表查無此 place_id 的細節（``ZERO_RESULTS``）；
    配額超限會重試，金鑰或參數錯誤則立即拋出（ADR-0006）。

    :param place_id: place ID registered in GoogleMap API
    :type place_id: str

    :returns: A python dict decoded from the json response, or None when not found.
    :rtype: dict or None

    :raises TransientPlacesAPIError: 配額超限或伺服器錯誤，重試耗盡後拋出。
    :raises PermanentPlacesAPIError: 授權或參數錯誤。
    :raises requests.exceptions.RequestException: 傳輸層失敗，重試耗盡後拋出。
    """
    _require_api_key()

    base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,rating,formatted_address,opening_hours,url,geometry",
        "language": "zh-TW",
        "key": API_KEY,
    }
    try:
        response = requests.get(base_url, params=params, timeout=120)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error(f"Timeout while fetching from {place_id}")
        raise
    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error while fetching from {place_id}")
        raise
    except requests.exceptions.HTTPError:
        logger.error(f"HTTP error while fetching from {place_id}")
        raise
    except Exception:
        logger.error(f"Unexpected error while fetching from {place_id}")
        raise
    else:
        data = response.json()
        if not _has_data(data, place_id):
            return None
        return data


def e_crawling_nightmarket(csvfile_path: str | Path) -> str:
    """Extracting data:

    Open the csv file that containing the night market name.
    With them, call the GoogleMap API by the function get_place_id() and get_place_details()
    to get the place details ot night markets in Taiwan. The place details is saved in new
    json file.

    :param csvfile_path: csv file path to open.
    :type csvfile_path: str | Path

    :returns: If requests.Exception,no founding ID/details or file I/O exceptioon.
    Otherwise, the path of generated json file is returned.
    :rtype: str | Path
    """
    _require_api_key()

    # 讀取csv，取得所有夜市名稱
    df_markets = pd.read_csv(Path(csvfile_path), sep=",")
    nm_names = df_markets["Night_market_name"]

    all_details_json = []  # 用來儲存所有夜市的原始 details(decoded-json)
    failure_id_list = []
    failure_detail_list = []
    for name in nm_names:
        logger.info(f"====正在查詢：{name} 的place ID...====")
        place_id = search_place_id(name)
        if not place_id:
            logger.info(f"找不到 {name} 的place ID")
            failure_id_list.append(name)
            continue

        logger.info(f"====已取得{name}的place_id，正在進一步查詢地理位置細節...====")
        details = get_place_details(place_id)

        if not details:
            logger.info(f"找不到{name}的地理位置細節")
            failure_detail_list.append(name)
            continue

        logger.info(f"====已取得{name}的地理位置細節====")
        all_details_json.append(details)

    # 統計查到空資料的筆數
    failure_count = len(failure_id_list) + len(failure_detail_list)
    failure_rate = failure_count / len(nm_names) if len(nm_names) > 0 else 1.0
    if failure_rate > MAX_FAILURE_RATE:
        raise ValueError(
            f"夜市查詢失敗率 {failure_rate:.0%}（{failure_count}/{len(nm_names)}）"
            f"超過門檻 {MAX_FAILURE_RATE:.0%}，請檢查夜市名稱來源與 API 設定"
        )

    # 合併儲存所有夜市 details 到同一個json
    # 定義存檔路徑，並確保資料夾存在（路徑基準見 ADR-0007）
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    jsonfile_name = RAW_DATA_DIR / f"Taiwan_night_markets_from_map_api_{today}.json"
    try:
        with open(jsonfile_name, "w", encoding="utf-8") as f:
            # 將字典 dict 型別的資料，寫入本機json檔案。
            json.dump(all_details_json, f, ensure_ascii=False, indent=4)
    except Exception:
        logger.error("Error on writing into JSON file.")
        raise
    else:
        logger.info(
            f"全部夜市地理資訊已成功輸出到：{jsonfile_name}，"
            f"總計找到了: {len(all_details_json)}個夜市資訊。"
            f"失敗率: {(len(failure_detail_list) + len(failure_id_list))} / {len(nm_names)}"
        )
        return str(jsonfile_name)
