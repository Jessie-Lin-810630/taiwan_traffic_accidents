"""Transform 階段：清洗夜市地理資訊 JSON，產生夜市事實資料並寫入資料庫。"""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from src.task.l_fact_night_markets import l_fact_night_markets
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def read_googlemap_responsed_json(jsonfile_path: str) -> list[dict]:
    """讀取夜市地理資訊 JSON，驗證必要欄位後取出名稱含「夜市」或「商圈」的項目。

    查詢時是拿維基百科的名稱去 Google 地圖比對，回應中難免混進不是夜市的地點，
    這一步用名稱把它們濾掉。

    - 過濾之前先驗證兩層：每個項目要有 `result` 欄位，以及 `result` 底下要有名稱、地址、
    座標、營業時間這四個必要欄位。
    - 缺一個記 `warning` 並繼續，缺的比例超過 30% 就記 `error` 並 raise，
    因為那代表來源的結構可能已經變了，繼續跑只會把大量空值寫進資料表。

    Args:
        jsonfile_path (str): 夜市地理資訊 JSON 的路徑。

    Returns:
        list[dict]: 每個元素是一個夜市的詳細資料（回應中的 `result` 部分），形如：

            [
                {
                    "name": "士林夜市",
                    "rating": 4.2,
                    "formatted_address": "111台灣臺北市士林區大東路",
                    "opening_hours": {"periods": [{"open": {"day": 0, "time": "1600"}, ...}]},
                    "url": "https://maps.google.com/?cid=...",
                    "geometry": {"location": {"lat": 25.088, "lng": 121.524}, ...},
                },
            ]

    Raises:
        FileNotFoundError: 路徑不存在。
        json.JSONDecodeError: 檔案不是合法的 JSON。
        ValueError: 有項目缺的必要欄位比例超過門檻。

    Notes:
        欄位驗證的分層、門檻與時機參考 ADR-0019。
    """
    # 一個項目要有 result，result 底下要有這四個欄位，缺的比例超過門檻就拋出。
    required_result_column = "result"
    required_columns = ("name", "formatted_address", "geometry", "opening_hours")
    missing_ratio_threshold = 0.3

    jsonfile_path = Path(jsonfile_path)
    with jsonfile_path.open(mode="r", encoding="utf-8") as jf:
        readout = json.load(
            jf
        )  # list with length of ~472, an element = a possible night market

    night_market_info_list = []
    missing_counts = {column: 0 for column in required_columns}
    over_threshold_count = 0

    for r in readout:  # r = a night market; r["result"] = a_night_market_info
        # 1. 開始驗證欄位是否相符，先驗證 'result' 是否存在
        a_night_market_info = r.get(required_result_column)

        # 不存在就無從檢查底下的欄位，把四個必要欄位全部計為缺少，該筆必定超過門檻。
        if a_night_market_info is None:
            over_threshold_count += 1
            for column in required_columns:
                missing_counts[column] += 1
            continue

        # 若存在 'result'，其他四個欄位個別驗證。
        missing_columns = [c for c in required_columns if c not in a_night_market_info]
        for column in missing_columns:
            missing_counts[column] += 1

        if len(missing_columns) / len(required_columns) > missing_ratio_threshold:
            over_threshold_count += 1
            continue

        # 2. 驗證完畢，接著從 'name' 欄位值判斷該筆資料的地理位置是否標為夜市或相似地標。
        nightmarket_name = a_night_market_info.get("name") or ""
        if "夜市" in nightmarket_name or "商圈" in nightmarket_name:
            night_market_info_list.append(a_night_market_info)

    # 只計算「整體」欄位驗證結果
    missing_summary = {k: v for k, v in missing_counts.items() if v}

    if over_threshold_count:
        message = (
            f"{jsonfile_path} 共 {len(readout)} 筆，其中 {over_threshold_count} 筆"
            f"缺少的必要欄位超過 {missing_ratio_threshold:.0%}，"
            f"各欄位缺少的筆數為 {missing_summary}，"
            f"請檢查 Google Maps 的回應結構是否已變動"
        )
        logger.error(message)
        raise ValueError(message)

    if missing_summary:
        logger.warning(
            f"{jsonfile_path} 共 {len(readout)} 筆，各欄位缺少的筆數為 "
            f"{missing_summary}，未超過門檻，這些欄位將使用預設值"
        )

    return night_market_info_list


def _clean_night_market_name(a_night_market_info: dict) -> dict[str]:
    """取出並清理一個夜市的名稱。

    Google 地圖的名稱常帶括號補述（例如「基隆廟口夜市(仁三路)」），若括號內沒有
    「夜市」或「商圈」字樣就整段去掉，只留主要名稱。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。

    Returns:
        dict[str, str]: 形如 `{"nightmarket_name": "基隆廟口夜市"}`。
    """
    source = a_night_market_info
    nightmarket_name = source.get("name")

    # 找左括號
    left_parenthesis1 = nightmarket_name.find("(")
    left_parenthesis2 = nightmarket_name.find("（")
    if left_parenthesis1 != -1 or left_parenthesis2 != -1:
        idx = max(left_parenthesis1, left_parenthesis2)
        text_in_parenthesis = nightmarket_name[idx:]
        if "夜市" not in text_in_parenthesis and "商圈" not in text_in_parenthesis:
            nightmarket_name = nightmarket_name[:idx]
    cleaned_night_market_name = {"nightmarket_name": str(nightmarket_name).strip()}
    logger.info(f"取得{nightmarket_name}")

    return cleaned_night_market_name


def _clean_night_market_address(
    a_night_market_info: dict, cities_per_region: dict
) -> dict[str]:
    """從格式化地址拆出地區、縣市、行政區、郵遞區號與街道地址。

    先把「台」統一成「臺」，再依序取出各欄位：縣市以 `cities_per_region` 的縣市
    名比對，地區由縣市反查，郵遞區號以「不緊鄰號、巷、弄、No.」的 3 到 6 位數字
    比對，行政區取「區」字之前的片段並去掉前面的縣市與路名贅字。街道地址保留
    原樣，方便回頭檢查前述清理是否有漏。任何一項比對不到都會填入說明字串
    （例如「無匹配縣市資訊」），不會是空值。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。
        cities_per_region (dict): 地區對應縣市清單的字典。

    Returns:
        dict[str, str]: 形如：

            {
                "region": "北部",
                "city": "基隆市",
                "district": "仁愛區",
                "zipcode": "200",
                "area_road": "200臺灣基隆市仁愛區玉田里仁三路",
            }
    """
    source = a_night_market_info

    formatted_address = str(
        source.get("formatted_address", "無地址資訊")
    )  # e.g. '200台灣基隆市仁愛區玉田里仁三路'

    # 同義字統一
    formatted_address = formatted_address.replace("台", "臺")

    # 清出city後分類出所屬region，i.e.: 北、中、南
    all_cities_list = []
    # 清出city
    for cites in cities_per_region.values():
        all_cities_list.extend(cites)
    cities_pattern = re.compile(rf"{'|'.join(all_cities_list)}")  # 未來可以改為globals
    find_city = re.search(cities_pattern, formatted_address)
    find_city = find_city.group() if find_city is not None else "無匹配縣市資訊"

    # 分類所屬region
    find_region = "無匹配地區資訊"
    for r, c in cities_per_region.items():
        if find_city in c:
            find_region = r
            break

    # 找出zipcode
    # \b                    # 單詞邊界
    # (?<!No\.|號|巷)      # ?<!：前面不能是 "No."、"號"、"巷"
    # \d{3,6}              # 3-6位純數字
    # (?![\s,號巷弄No\.])  # ?!：後面不能是空白、逗號、號、巷、弄、No.
    # \b                   # 單詞邊界
    zipcode_pattern = re.compile(
        r"(?<!No\. )(?<!No\.)(?<!No )(?<!No)(?<!NO\. )(?<!NO\.)(?<!NO )(?<!NO)\d{3,6}(?!號|巷|弄)(?! 號| 巷| 弄)"
    )
    find_zipcode = re.search(zipcode_pattern, formatted_address)  # 未來可以改為globals
    find_zipcode = (
        str(find_zipcode.group()) if find_zipcode is not None else "無匹配郵遞區號"
    )  # 保持zipcode為字串。

    # 清出第二、三行政區名，關鍵字：xx 區，由於"區"的字詞結構簡單，不需要動用速度較慢的reg
    def clean_district(formatted_address: str) -> str:
        """從地址中取出第二、三級行政區名。

        取「區」字之前的片段，再依序去掉縣、市、路、街、段、巷、樓等字之前的
        贅字。以字串搜尋而非正規表達式處理，因為「區」的字詞結構單純。

        Args:
            formatted_address (str): 已統一為「臺」的格式化地址。

        Returns:
            str: 行政區名，例如 `"仁愛區"`；找不到「區」字時為 `"無匹配第二、三行政區"`。
        """
        # 去後方贅字
        cyu_letter_idx = formatted_address.find("區")
        if cyu_letter_idx == -1:
            return "無匹配第二、三行政區"
        else:
            find_district = formatted_address[: cyu_letter_idx + 1]

        # 去前方贅字
        invalid_letters = "縣市路街段巷樓"
        for letter in invalid_letters:
            invalid_letter_idx = find_district.find(letter)
            if invalid_letter_idx != -1:
                find_district = find_district[invalid_letter_idx + 1 :]
        return find_district

    find_district = clean_district(formatted_address)

    # 整理街道地址，先維持原樣，以利追溯上述清理邏輯是否有漏洞
    find_area_road = formatted_address.strip()

    # 裝成dict
    cleaned_address = {
        "region": find_region,
        "city": find_city,
        "district": find_district,
        "zipcode": find_zipcode,
        "area_road": find_area_road,
    }
    return cleaned_address


def _clean_night_market_geometry_location(
    a_night_market_info: dict,
) -> dict[float | None]:
    """取出一個夜市的中心座標與東北、西南兩個邊界端點座標。

    取不到的欄位一律是 `None`，不會以 0 代替。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。

    Returns:
        dict[str, float | None]: 形如：

            {
                "latitude": 25.128240,
                "longitude": 121.743557,
                "northeast_latitude": 25.129718,
                "northeast_longitude": 121.744682,
                "southwest_latitude": 25.127020,
                "southwest_longitude": 121.741984,
            }
    """
    source = a_night_market_info

    # 清理夜市中心點經度與緯度
    location = source.get(
        "geometry", {}
    )  # {'location': {'lat': 25.1282405, 'lng': 121.7435579}}
    location = location.get("location", {})  # {'lat': 25.1282405, 'lng': 121.7435579}
    latitude = location.get("lat", None)
    longitude = location.get("lng", None)
    latitude = float(latitude) if latitude is not None else None
    longitude = float(longitude) if longitude is not None else None

    # 清理夜市區域邊界經度與緯度 - 東北角
    viewport = source.get("geometry", {})
    # {'viewport': {
    # 'northeast': {'lat': 25.12971878029151, 'lng': 121.7446820302915},
    # 'southwest': {'lat': 25.1270208197085, 'lng': 121.7419840697085}
    # }}
    viewport = viewport.get("viewport", {})
    northeast_end = viewport.get("northeast", {})
    northeast_latitude = northeast_end.get("lat", None)
    northeast_longitude = northeast_end.get("lng", None)
    northeast_latitude = (
        float(northeast_latitude) if northeast_latitude is not None else None
    )
    northeast_longitude = (
        float(northeast_longitude) if northeast_longitude is not None else None
    )

    # 清理夜市區域邊界經度與緯度 - 西南角
    viewport = source.get("geometry", {})
    viewport = viewport.get("viewport", {})
    southwest_end = viewport.get("southwest", {})
    southwest_latitude = southwest_end.get("lat", None)
    southwest_longitude = southwest_end.get("lng", None)
    southwest_latitude = (
        float(southwest_latitude) if southwest_latitude is not None else None
    )
    southwest_longitude = (
        float(southwest_longitude) if southwest_longitude is not None else None
    )

    # 裝入dict
    find_long_lat = {
        "latitude": latitude,
        "longitude": longitude,
        "northeast_latitude": northeast_latitude,
        "northeast_longitude": northeast_longitude,
        "southwest_latitude": southwest_latitude,
        "southwest_longitude": southwest_longitude,
    }
    return find_long_lat


def _clean_business_datetime(a_night_market_info: dict) -> dict[list[str]]:
    """把一週的營業時段整理成「一天一列」的形式。

    來源的每個時段有開始與結束兩個時間點，跨夜時兩者落在不同天。為了讓資料
    能按星期存放，這裡把時段拆成五種情況處理：

    - 全年無休（來源沒有結束時間）：七天都填 00:00:00 到 23:59:59。
    - 真正跨夜（例如週日 16:00 到週一 02:00）：拆成當天到 23:59:59 與隔天
      00:00:00 起兩列。
    - 週六跨到週日：同上，只是結束日的星期序號比開始日小。
    - 結束時間標成隔天 00:00：不算真正跨夜，只填當天到 23:59:59 一列。
    - 一般情況：開始與結束都在同一天，填一列。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。

    Returns:
        list[dict]: 每個元素是一天的營業時段，形如：

            [
                {
                    "business_days_weekday": "星期日",
                    "business_hours_opening": "16:00:00",
                    "business_hours_closing": "23:59:59",
                },
                {
                    "business_days_weekday": "星期一",
                    "business_hours_opening": "00:00:00",
                    "business_hours_closing": "02:00:00",
                },
            ]
    """
    source = a_night_market_info
    opening_info = source.get("opening_hours", {})
    periods = opening_info.get("periods", [])
    weekday_map = {
        0: "星期日",
        1: "星期一",
        2: "星期二",
        3: "星期三",
        4: "星期四",
        5: "星期五",
        6: "星期六",
    }

    # 格式化時間函式
    def format_t(t: str) -> str:
        """把來源的四碼時間字串轉成 `HH:MM:SS`。

        Args:
            t (str): 四碼時間字串，例如 `"1630"`。

        Returns:
            str: 形如 `"16:30:00"` 的時間字串。
        """
        return f"{t[:2]}:{t[2:]}:00"

    find_business_datetime = []
    for p in periods:
        # 逐個處理每一段營業時間
        open_day = p["open"]["day"]  # 0、1、2....
        open_time_raw = p["open"]["time"]  # "1630"
        close_day = p.get("close", {}).get("day", None)  # 0、1、2...
        close_time_raw = p.get("close", {}).get("time", None)  # "0200"

        # 狀況A：全年無休(無close key)
        if close_day is None:
            # print("走A:!")
            for v in weekday_map.values():
                find_business_datetime.append(
                    {
                        "business_days_weekday": v,  # 星期一、星期二、星期三、....
                        "business_hours_opening": "00:00:00",
                        "business_hours_closing": "23:59:59",
                    }
                )

        # 狀況B：星期一到星期六、星期日到星期一的期間真正有跨夜。例如 0: 16:00 - 1: 02:00
        elif close_day > open_day and close_time_raw != "0000":
            # print("走B:!")
            # 處理當天開始營業後~跨夜前一秒 (23:59:59)
            find_business_datetime.append(
                {
                    "business_days_weekday": weekday_map[open_day],  # 0
                    "business_hours_opening": format_t(open_time_raw),  # 16:00:00
                    "business_hours_closing": "23:59:59",
                }
            )
            # 處理跨夜後 (00:00:00) 繼續營業到隔天
            find_business_datetime.append(
                {
                    "business_days_weekday": weekday_map[close_day],  # 1
                    "business_hours_opening": "00:00:00",
                    "business_hours_closing": format_t(close_time_raw),  # 02:00:00
                }
            )

        # 狀況C：真正跨夜，但是是六跨日。例如 6: 17:00 - 0: 01:00
        elif close_day < open_day and close_day == 0 and close_time_raw != "0000":
            # print("走C:!")
            # 處理當天開始營業後~跨夜前一秒 (23:59:59)
            find_business_datetime.append(
                {
                    "business_days_weekday": weekday_map[open_day],  # 6
                    "business_hours_opening": format_t(open_time_raw),  # 17:00:00
                    "business_hours_closing": "23:59:59",
                }
            )

            # 處理跨夜後 (00:00:00) 繼續營業，直到結束營業日的前一天
            find_business_datetime.append(
                {
                    "business_days_weekday": weekday_map[close_day],  # 0
                    "business_hours_opening": "00:00:00",
                    "business_hours_closing": format_t(close_time_raw),  # 01:00:00
                }
            )

        # 狀況D：不算真正跨夜，僅結束時間被標示為隔日的00:00。例如：6: 16:00 - 0: 00:00
        elif close_time_raw == "0000":
            # print("走D:!")
            find_business_datetime.append(
                {
                    "business_days_weekday": weekday_map[open_day],  # 6
                    "business_hours_opening": format_t(open_time_raw),  # 16:00:00
                    "business_hours_closing": "23:59:59",
                }
            )

        # 狀況E：不跨夜、不在00:00結束營業，一般來說都是狀況E
        else:
            # print("走E:!")
            find_business_datetime.append(
                {
                    "business_days_weekday": weekday_map[open_day],
                    "business_hours_opening": format_t(open_time_raw),
                    "business_hours_closing": format_t(close_time_raw),
                }
            )
    return find_business_datetime


def _clean_googlemap_rating(a_night_market_info: dict) -> dict[float]:
    """取出一個夜市的 Google 地圖評分。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。

    Returns:
        dict[str, float | None]: 形如 `{"googlemap_rating": 4.2}`；
            來源沒有評分時值為 `None`。
    """
    source = a_night_market_info
    rating = source.get("rating", None)
    cleaned_rating = {
        "googlemap_rating": float(rating) if rating is not None else rating
    }
    return cleaned_rating


def _clean_googlemap_url(a_night_market_info: dict) -> dict[str]:
    """取出一個夜市在 Google 地圖上的網址。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。

    Returns:
        dict[str, str]: 形如 `{"url_to_googlemap": "https://maps.google.com/?cid=..."}`；
            來源沒有網址時值為 `"未取得"`。
    """
    source = a_night_market_info
    url = source.get("url", "未取得")
    cleaned_url = {"url_to_googlemap": str(url)}
    return cleaned_url


def _t_clean_one_night_market(
    a_night_market_info: dict, cities_per_region: dict
) -> list[dict]:
    """把各支清洗函式的結果組成一個夜市的資料列。

    以營業時段（一天一列）為骨架，其餘欄位（名稱、地址、座標、評分、網址）
    對整個夜市都相同，逐欄補上去，因此一個夜市會展開成多列。

    Args:
        a_night_market_info (dict): 一個夜市的詳細資料。
        cities_per_region (dict): 地區對應縣市清單的字典。

    Returns:
        list[dict]: 一個夜市的多列資料，每個字典是一列，形如：

                [
                {
                    "business_days_weekday": "星期日",
                    "business_hours_opening": "16:00:00",
                    "business_hours_closing": "23:59:59",
                    "nightmarket_name": "士林夜市",
                    "region": "北部",
                    "city": "臺北市",
                    "district": "士林區",
                    "zipcode": "111",
                    "area_road": "111臺灣臺北市士林區大東路",
                    "latitude": 25.088100,
                    "longitude": 121.524300,
                    "northeast_latitude": 25.089000,
                    "northeast_longitude": 121.525200,
                    "southwest_latitude": 25.087200,
                    "southwest_longitude": 121.523400,
                    "url_to_googlemap": "https://maps.google.com/?cid=...",
                    "googlemap_rating": 4.2,
                },
            ]
    """
    nm = a_night_market_info

    cleaned_night_market_name = _clean_night_market_name(nm)  # dict[str]
    clean_address = _clean_night_market_address(nm, cities_per_region)  # dict[str]
    cleaned_night_market_loc = _clean_night_market_geometry_location(nm)  # dict[float]
    cleaned_business_datetime = _clean_business_datetime(nm)  # dict[list[str]]
    cleaned_rating = _clean_googlemap_rating(nm)  # dict[float]
    cleaned_url = _clean_googlemap_url(nm)  # dict[str]

    # 整併成DataFrame，再to_dict(速度會比最後不斷concat多個dataframe快)
    df = pd.DataFrame(cleaned_business_datetime)
    df["nightmarket_name"] = cleaned_night_market_name["nightmarket_name"]
    df["region"] = clean_address["region"]
    df["city"] = clean_address["city"]
    df["district"] = clean_address["district"]
    df["zipcode"] = clean_address["zipcode"]
    df["area_road"] = clean_address["area_road"]
    df["latitude"] = cleaned_night_market_loc["latitude"]
    df["longitude"] = cleaned_night_market_loc["longitude"]
    df["northeast_latitude"] = cleaned_night_market_loc["northeast_latitude"]
    df["northeast_longitude"] = cleaned_night_market_loc["northeast_longitude"]
    df["southwest_latitude"] = cleaned_night_market_loc["southwest_latitude"]
    df["southwest_longitude"] = cleaned_night_market_loc["southwest_longitude"]
    df["url_to_googlemap"] = cleaned_url["url_to_googlemap"]
    df["googlemap_rating"] = cleaned_rating["googlemap_rating"]
    records_a_night_market = df.to_dict(
        "records"
    )  # 一個夜市的7天資料(7個字典組成的list)[{dict1}, {dict2}, ...]
    return records_a_night_market


def t_fact_night_markets(
    night_market_info_list: list[dict],
    cities_per_region: dict[list],
    database: str,
    batch_size: int | None = None,
) -> None:
    """分批清洗夜市地理資訊，每清完一批就寫入 `fact_night_markets`。

    與其他 `t_*` 不同，本函式不回傳 DataFrame，而是逐批呼叫 `l_fact_night_markets()`
    寫入資料庫，避免一次把全部夜市的資料留在記憶體。每一批清洗後會補空值並去除
    完全重複的列。

    Args:
        night_market_info_list (list[dict]): 夜市詳細資料清單，
            即 `read_googlemap_responsed_json()` 的產出。
        cities_per_region (dict): 地區對應縣市清單的字典。
        database (str): 目標資料庫名稱。
        batch_size (int | None): 每批處理幾個夜市；`None` 表示一次處理全部。

    Raises:
        pymysql.MySQLError: 任一批寫入失敗，該批事務復原後往外拋。
        TypeError: `batch_size` 為 `None` 時日誌訊息的計算會失敗。
    """
    # 清理、並將清洗後的dataframe合併

    size = len(night_market_info_list) if batch_size is None else batch_size
    logger.info("Cleaning data...")
    for i in range(0, len(night_market_info_list), size):
        all_records = []
        nms = night_market_info_list[i : i + size]
        for nm in nms:
            records_a_nm = _t_clean_one_night_market(nm, cities_per_region)
            all_records.extend(
                records_a_nm
            )  # list.extend(list[dict]) => list[dict, dict]
        df_night_markets = pd.DataFrame(all_records)

        # 填補空值
        df_night_markets = df_night_markets.replace({np.nan: None})

        # 爬蟲難免有重複取得之資料，做去重
        df_night_markets = df_night_markets.drop_duplicates(keep="first")
        logger.info(
            f"Successfully cleaned the records of the No.{i}~{i + size} potential night market..."
        )
        l_fact_night_markets(df_night_markets, database)
    return None
