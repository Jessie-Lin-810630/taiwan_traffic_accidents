"""Extract 階段：爬取全臺夜市清單，並向 Google Maps API 取得地理資訊。

分兩步：先從維基百科的「臺灣夜市列表」抓下夜市名稱與地址存成 CSV，再逐一
拿名稱去 Google Places API 查 place ID 與詳細資料（座標、評分、營業時間、
地圖網址），彙整成一份 JSON。兩份檔案都落在 `data/raw`。

Places API 的失敗寫在回應內容的 `status` 欄位而非 HTTP 狀態碼，因此本模組
自訂 `PlacesAPIError` 例外類別，並向內分成暫時性與永久性兩類例外，
然後再與傳輸層的暫時性故障合併成單一重試判斷式。

設定取自環境變數:

- 必填: `GOOGLE_MAP_API_KEY`，缺少時在送出請求的那一刻拋出

Notes:
    API 失敗的分類方式參考 ADR-0006，金鑰的檢查時機參考 ADR-0008。
"""

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
    """從維基百科抓下全臺夜市清單，存成 CSV 並回傳檔案路徑。

    頁面上每個縣市各一張表格，逐列取出夜市名稱與地址，只保留名稱含「夜市」或
    「商圈」的列，並依縣市補上所屬地區。解析不到任何夜市時拋出而不是產出空
    CSV，因為那代表頁面結構已變；回傳路徑即代表該檔案確實寫出且含有資料。

    Args:
        url (str): 維基百科「臺灣夜市列表」頁面網址。
        headers (dict): 請求標頭。
        cities_per_region (dict): 地區對應縣市清單的字典，用來替每個夜市標上
            北部、中部、南部、東部或離島。

    Returns:
        str: 產出的 CSV 檔案路徑，形如
            `"data/raw/Taiwan_night_markets_list_2026-08-05.csv"`。
            檔案內容形如：

                Region  City    Night_market_name  Night_market_address
                北部    臺北市  士林夜市           臺北市士林區大東路
                北部    臺北市  饒河街觀光夜市     臺北市松山區饒河街

    Raises:
        requests.exceptions.RequestException: 抓取失敗、逾時或回應非 2xx。
        ValueError: 頁面解析不到任何夜市，代表頁面結構已變。

    Notes:
        「不回傳空結果」的取捨參考 ADR-0005。
    """
    # 變數宣告
    response = None

    # 定義存檔路徑，並確保資料夾存在
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    csvfile_name = RAW_DATA_DIR / f"Taiwan_night_markets_list_{today}.csv"

    try:
        response = requests.get(url, headers=headers, timeout=120)
        # 非 2xx 直接轉成 HTTPError，交由下方的 except 分類後原樣拋出。
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
        city_name = soup.find_all("h3")  # 基隆市、臺北市、......、連江縣，22 個縣市
        tables = soup.find_all("table", class_="wikitable")  # 22個表格
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
        # 解析不到任何夜市代表頁面結構已變。
        if df.empty:
            raise ValueError(f"自 {url} 解析不到任何夜市，請檢查爬蟲程式")

        df.to_csv(csvfile_name, sep=",", encoding="utf-8-sig")
        logger.info(f"====Save the file successfully! {csvfile_name}====")
        return str(csvfile_name)


load_dotenv()
API_KEY = os.getenv("GOOGLE_MAP_API_KEY")


def _require_api_key() -> None:
    """呼叫 Places API 前確認金鑰存在。

    先檢查再送出請求，錯誤訊息才會直指缺少的環境變數；
    否則 API 只會回 `REQUEST_DENIED`，此訊息說的是「金鑰無效」，
    而這與「自己根本沒有帶金鑰」的排查方向不完全相同。

    Raises:
        ValueError: `GOOGLE_MAP_API_KEY` 未設定。

    Notes:
        參考 ADR-0008。
    """
    if not API_KEY:
        raise ValueError("未設定 GOOGLE_MAP_API_KEY，請檢查環境變數設置")


class PlacesAPIError(RuntimeError):
    """Google Places API 以 HTTP 200 回報的失敗。

    傳輸層成功（狀態碼 200），失敗寫在 body 的 status 欄位，
    因此無法用 `requests` 的例外類別體系表達，得自訂。
    """


class TransientPlacesAPIError(PlacesAPIError):
    """暫時性故障（配額超限、伺服器錯誤），值得重試。"""


class PermanentPlacesAPIError(PlacesAPIError):
    """永久性故障（授權或參數問題），重試只會浪費配額並延後告警。"""


# Google Places API 的失敗寫在 body 的 status 欄位，但 HTTP 狀態碼一律是 200，
# 因此必須顯式分類，否則所有失敗都會在解析 json 後，被模糊成「無資料、找不到地點」。
# 不在此字典中的 key 在本專案都不認為是故障，包含三種：
# - OK：有結果
# - ZERO_RESULTS 與 NOT_FOUND（Place Details 專有，place_id 已失效）：確實查無資料。
# status 完整定義見：
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

# 僅供 `_has_data()` 內部判斷。
_NOT_FOUND_STATUSES = frozenset({"ZERO_RESULTS", "NOT_FOUND"})


def _has_data(data: dict, context: str) -> bool:
    """檢查 Places API 回應的 `status`，確認是否查到空資料，若為故障則改 raise。

    Args:
        data (dict): API 回應解碼後的字典。
        context (str): 寫進錯誤訊息的查詢對象，夜市名稱或 place ID。

    Returns:
        bool: `True` 代表有查到可用資料， `False` 代表查無資料。

    Raises:
        TransientPlacesAPIError: 配額超限或伺服器錯誤，此類型例外可在外層函式搭配一個重試裝飾器接住後執行重試。
        PermanentPlacesAPIError: 授權或參數錯誤，重試無用，立即失敗。

    Notes:
        參考 ADR-0006。
    """
    status = data.get("status", "UNKNOWN_ERROR")

    if status in API_STATUS_ERRORS:
        error_type, hint = API_STATUS_ERRORS[status]
        raise error_type(
            f"Google Places API 回報 {status}（查詢對象：{context}）：{hint}"
        )

    # 如果不需要 raise 暫時性故障或是永久性故障，則改檢查是否為空資料後，回傳布林值。
    return status not in _NOT_FOUND_STATUSES


def _should_retry(exc: BaseException) -> bool:
    """判斷例外是否值得重試，有兩種暫時性故障來源適合重試。

    來源一: 由 `is_transient()` 筐列的連線中斷、逾時、429 與 5xx。
    來源二: 為 GOOGLE MAP API 自訂包裝的 `TransientPlacesAPIError`，
    且由 `_has_data()` 拋出。
    兩來源合併成單一判斷式，再傳給 tenacity 重試裝飾器 `retry`。

    Args:
        exc (BaseException): 要判斷的例外。

    Returns:
        bool: 值得重試為 `True`，否則為 `False`。

    Notes:
        參考 ADR-0006。
    """
    # TransientPlaceAPIError、TimeoutError、ConnectionError、429/5xx 都會回傳 True。
    # 其中 TransientPlaceAPIError 代表 status_code = 200 但是 status 欄位暗示有錯誤。
    return isinstance(exc, TransientPlacesAPIError) or is_transient(exc)


# 改用 _should_retry 判斷是否該重試，而非單用 is_transient 判斷
retry_on_transient_api = retry(
    retry=retry_if_exception(_should_retry),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=RETRY_WAIT,
    reraise=True,
)


@retry_on_transient_api
def search_place_id(place_name: str) -> None | str:
    """以地點名稱向 Places API 查詢 place ID。

    回傳 `None` 只代表 Google 地圖上查無此地點，那是正常結果；配額超限會自動
    重試，金鑰或參數錯誤則立即拋出。

    Args:
        place_name (str): 地點或店家名稱，這裡傳入的是夜市名稱。

    Returns:
        str | None: 查到的 place ID，形如
            `"ChIJc0rOM6yqQjQRrjfLzHVzOOs"`；查無此地點時為 `None`。

    Raises:
        ValueError: `GOOGLE_MAP_API_KEY` 未設定。
        TransientPlacesAPIError: 配額超限或伺服器錯誤，重試耗盡後拋出。
        PermanentPlacesAPIError: 授權或參數錯誤，不重試。
        requests.exceptions.RequestException: 傳輸層失敗，重試耗盡後拋出。

    References:
        https://developers.google.com/maps/documentation/places/web-service/legacy/search-find-place
    """
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
    """以 place ID 向 Places API 查詢地點詳細資料。

    取回的欄位有名稱、評分、格式化地址、營業時間、地圖網址與座標範圍。
    回傳 `None` 只代表查無此 place ID 的細節；配額超限會自動重試，金鑰或參數
    錯誤則立即拋出。

    Args:
        place_id (str): Places API 的地點識別碼。

    Returns:
        dict | None: 回應解碼後的字典，查無資料時為 `None`。內容形如：

            {
                "status": "OK",
                "result": {
                    "name": "士林夜市",
                    "rating": 4.2,
                    "formatted_address": "111臺北市士林區大東路",
                    "opening_hours": {"weekday_text": ["星期一: 16:00 – 00:00", ...]},
                    "url": "https://maps.google.com/?cid=...",
                    "geometry": {
                        "location": {"lat": 25.088, "lng": 121.524},
                        "viewport": {
                            "northeast": {"lat": 25.089, "lng": 121.525},
                            "southwest": {"lat": 25.087, "lng": 121.523},
                        },
                    },
                },
            }

    Raises:
        ValueError: `GOOGLE_MAP_API_KEY` 未設定。
        TransientPlacesAPIError: 配額超限或伺服器錯誤，重試耗盡後拋出。
        PermanentPlacesAPIError: 授權或參數錯誤，不重試。
        requests.exceptions.RequestException: 傳輸層失敗，重試耗盡後拋出。
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


# 因維基百科的名稱不一定完全對得上 Google 地圖，允許零星幾個夜市查不到為常態，
# 但大量查不到代表系統性問題（金鑰失效、頁面改版），
# 此 RATE 門檻非硬性規定，可自己調整。
MAX_FAILURE_RATE = 0.5


def e_crawling_nightmarket(csvfile_path: str | Path) -> str:
    """讀入夜市清單 CSV，逐一查詢地理資訊並彙整成一份 JSON。

    每個夜市先查 place ID、再查詳細資料，兩步任一查無結果就記錄下來並跳過。
    因維基百科的名稱不一定完全對得上 Google 地圖，允許零星幾個夜市查不到為常態，
    但如果整體失敗率超過 `MAX_FAILURE_RATE`（50%）就拋出，
    因為這反而可能代表金鑰失效或來源頁面改版等系統性問題。

    Args:
        csvfile_path (str | Path): 夜市清單 CSV 的路徑，需含
            `Night_market_name` 欄位，即 `find_tw_night_markets_list()` 的產出。

    Returns:
        str: 產出的 JSON 檔案路徑，形如
            `"data/raw/Taiwan_night_markets_from_map_api_2026-08-05.json"`。
            檔案內容是 `get_place_details()` 回應的清單。

    Raises:
        ValueError: `GOOGLE_MAP_API_KEY` 未設定，或查詢失敗率超過門檻。
        FileNotFoundError: 夜市清單 CSV 不存在。
        KeyError: CSV 缺少 `Night_market_name` 欄位。
        PermanentPlacesAPIError: 授權或參數錯誤。
        requests.exceptions.RequestException: 傳輸層失敗，重試耗盡後拋出。
        OSError: JSON 檔案寫入失敗。
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
