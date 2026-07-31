"""Transform 階段：清洗夜市地理資訊 JSON，產生夜市事實 DataFrame。"""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from src.task.l_fact_night_markets import l_fact_night_markets
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def generate_night_market_serial_num_list(jsonfile_path: str | Path) -> list[int]:
    """Open the json file returned by googlemap place api which

    describing the geometry and business time of all the night markets
     in Taiwan.
    """
    jsonfile_path = Path(str(jsonfile_path))
    with jsonfile_path.open(mode="r", encoding="utf-8") as jf:
        readout = json.load(jf)  # list with length of ~472, an element = a night market
    batch_list = [i for i in range(len(readout))]
    return batch_list


def read_googlemap_responsed_json(jsonfile_path: str) -> list[dict]:
    """Open the json file returned by googlemap place api which

    describing the geometry and business time of all the night markets
     in Taiwan.
    """
    jsonfile_path = Path(jsonfile_path)
    with jsonfile_path.open(mode="r", encoding="utf-8") as jf:
        readout = json.load(
            jf
        )  # list with length of ~472, an element = a possible night market
        night_market_info_list = []
        for r in readout:  # r = a night market; r["result"] = a_night_market_info
            if "夜市" in r["result"].get("name") or "商圈" in r["result"].get("name"):
                night_market_info_list.append(r["result"])
    return night_market_info_list


def clean_night_market_name(a_night_market_info: dict) -> dict[str]:
    """Extract and clean the name of a certain night market in Taiwan."""
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


def clean_night_market_address(
    a_night_market_info: dict, cities_per_region: dict
) -> dict[str]:
    """Extract and clarify the address-related attributes of a certain

    night market in Taiwan, including region, zipcode, city and area road.
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


def clean_night_market_geometry_location(
    a_night_market_info: dict,
) -> dict[float | None]:
    """Extract and clarify the latitude and longitude of a certain

    night market in Taiwan.
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


def clean_business_datetime(a_night_market_info: dict) -> dict[list[str]]:
    """Extract and transform the business datetime within a week for a certain

    night market in Taiwan.
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
    def format_t(t):
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


def clean_googlemap_rating(a_night_market_info: dict) -> dict[float]:
    """Extract and clean the rating for a certain night market in Taiwan."""
    source = a_night_market_info
    rating = source.get("rating", None)
    cleaned_rating = {
        "googlemap_rating": float(rating) if rating is not None else rating
    }
    return cleaned_rating


def clean_googlemap_url(a_night_market_info: dict) -> dict[str]:
    """Extract and clean the URL to GoogleMap APP for a certain night market in Taiwan."""
    source = a_night_market_info
    url = source.get("url", "未取得")
    cleaned_url = {"url_to_googlemap": str(url)}
    return cleaned_url


def t_clean_one_night_market(
    a_night_market_info: dict, cities_per_region: dict
) -> list[dict]:
    """Apply discrete functions to get the Series containing

    cleaned/transformed data for one night market in Taiwan.
    The generated Series are combined and returned in a list[dict].
    """
    nm = a_night_market_info

    cleaned_night_market_name = clean_night_market_name(nm)  # dict[str]
    clean_address = clean_night_market_address(nm, cities_per_region)  # dict[str]
    cleaned_night_market_loc = clean_night_market_geometry_location(nm)  # dict[float]
    cleaned_business_datetime = clean_business_datetime(nm)  # dict[list[str]]
    cleaned_rating = clean_googlemap_rating(nm)  # dict[float]
    cleaned_url = clean_googlemap_url(nm)  # dict[str]

    # 整併成DataFrame，再to_dict(速度會比最後不斷concat多個dataframe快)
    df = pd.DataFrame(cleaned_business_datetime)
    # print(df.head(10))
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
    """分批清洗夜市地理資訊，並將結果寫入 `fact_night_markets`。

    Parameters:
        night_market_info_list (list[dict]): Google Maps API 回傳的夜市詳細資訊。
        cities_per_region (dict[list]): 地區（北／中／南部）對應的縣市清單。
        database (str): 目標資料庫名稱。
        batch_size (int | None): 每批處理的夜市筆數；None 表示一次處理全部。
    """
    # 清理、並將清洗後的dataframe合併

    size = len(night_market_info_list) if batch_size is None else batch_size
    logger.info("Cleaning data...")
    for i in range(0, len(night_market_info_list), size):
        all_records = []
        nms = night_market_info_list[i : i + size]
        for nm in nms:
            records_a_nm = t_clean_one_night_market(nm, cities_per_region)
            all_records.extend(
                records_a_nm
            )  # list.extend(list[dict]) => list[dict, dict]
        df_night_markets = pd.DataFrame(all_records)

        # 填補空值
        df_night_markets = df_night_markets.replace({np.nan: None})

        # 爬蟲難免有重複取得之資料，做去重
        df_night_markets = df_night_markets.drop_duplicates(keep="first")
        logger.info(
            f"Successfully cleaned the records of the No.{i}~{i + batch_size} potential night market..."
        )
        l_fact_night_markets(df_night_markets, database)
    return None
