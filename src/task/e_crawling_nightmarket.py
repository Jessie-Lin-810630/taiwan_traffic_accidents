from dotenv import load_dotenv
import os
import json
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from datetime import datetime

# 指定要爬取的網址
night_markets_wiki_url = "https://zh.wikipedia.org/zh-tw/%E8%87%BA%E7%81%A3%E5%A4%9C%E5%B8%82%E5%88%97%E8%A1%A8"

# 準備headers
headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
           " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"}

cities_per_region = {
    "北部": ["臺北市", "新北市", "基隆市", "桃園市", "新竹市", "新竹縣"],
    "中部": ["臺中市", "彰化縣", "南投縣", "雲林縣", "苗栗縣"],
    "南部": ["臺南市", "高雄市", "屏東縣", "嘉義市", "嘉義縣"],
    "東部": ["花蓮縣", "臺東縣", "宜蘭縣"],
    "離島": ["澎湖縣", "金門縣", "連江縣"]
}


def find_tw_night_markets_list(url: str, headers: dict, cities_per_region: dict) -> str:
    """
    Request the url Wikipedia to get the list of night markets in Taiwan. 
    The obtained list will be saved in a new csv file of which the file path is 
    return value.

    :param url: URL of Wikipedia summarizing the night markets in Taiwan.
    :type url: str
    :param headers: Headers required for Request.get() function.
    :type headers: dict
    :param cities_per_region: Dict to map the region that a night market is located
    (north, south, west, east, outlying islands).
    :type cities_per_region: dict

    :returns: String of the path of generated csv file.
    :rtype: str
    """

    # 變數宣告
    response = None
    soup = None

    # 定義存檔路徑，並確保資料夾存在
    curr_working_dir = Path().resolve()  # 取得專案根目錄的絕對路徑
    raw_data_save_dir = curr_working_dir/"test"/"raw_data"
    raw_data_save_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    csvfile_name = raw_data_save_dir/f"Taiwan_night_markets_list_{today}.csv"

    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            print(f"====成功訪問{url}====")
            soup = BeautifulSoup(response.text, "html.parser")

    except requests.exceptions.Timeout as e:
        print(f"Timeout occurred while fetching from {url}, "
              f"error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error occurred while fetching from {url},"
              f"error: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching from {url},"
              f"error: {e}")
    except Exception as e:
        print(f"An error occurred while fetching from {url},"
              f"error: {e}")
    else:
        if soup is not None:
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
            df = pd.DataFrame({"Region": regionlst,
                               "City": citylst,
                               "Night_market_name": nm_namelst,
                               "Night_market_address": nm_addresslst, })
            if df.empty:
                print(f"{csvfile_name}為空的dataframe，請檢查爬蟲程式")

            df.to_csv(csvfile_name, sep=",", encoding="utf-8-sig")
            print(f"====Save the file successfully! {csvfile_name}====")
    finally:
        return str(csvfile_name)


# 讀取 .env
load_dotenv()
API_KEY = os.getenv("GOOGLE_MAP_API_KEY")


def search_place_id(place_name: str) -> None | str:
    """
    Call the GoogleMap Place API to request the place IDs of each
    interested location.

    :param place_name: location name or shop name (e.g. night market name)
    :type place_name: str

    :returns: If requests.Exception or not found ID, it will return None. 
    Otherwise return the place ID.
    :rtype: None or str
    """

    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": place_name,
        "inputtype": "textquery",
        "fields": "place_id",
        "language": "zh-TW",
        "key": API_KEY
    }
    try:
        response = requests.get(base_url, params=params, timeout=120)
    except requests.exceptions.Timeout as e:
        print(f"Timeout occurred while fetching from {place_name}, "
              f"error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error occurred while fetching from {place_name},"
              f"error: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching from {place_name},"
              f"error: {e}")
    except Exception as e:
        print(f"An error occurred while fetching from {place_name},"
              f"error: {e}")
    else:
        data = response.json()
        if data.get("candidates"):
            return data["candidates"][0]["place_id"]
    return None


def get_place_details(place_id: str) -> dict | None:
    """
    Use the place ID and call the GoogleMap Place API to get detailed information
    of a location, including name, rating, formatted_address, opening_hours, URL to GoogleMap
    and geometry.

    :param place_id: place ID registered in GoogleMap API
    :type place_id: str

    :returns: If requests.Exception or not found details, it will return None. 
    Otherwise, return a python dict object decoded from the json-type response.
    :rtype: dict or None
    """

    base_url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,rating,formatted_address,opening_hours,url,geometry",
        "language": "zh-TW",
        "key": API_KEY
    }
    try:
        response = requests.get(base_url, params=params, timeout=120)
    except requests.exceptions.Timeout as e:
        print(f"Timeout occurred while fetching from {place_id}, "
              f"error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error occurred while fetching from {place_id},"
              f"error: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while fetching from {place_id},"
              f"error: {e}")
    except Exception as e:
        print(f"An error occurred while fetching from {place_id},"
              f"error: {e}")
    else:
        return response.json()
    return None


def e_crawling_nightmarket(csvfile_path: str | Path) -> str | None:
    """
    Extracting data:
    Open the csv file that containing the night market name.
    With them, call the GoogleMap API by the function get_place_id() and get_place_details()
    to get the place details ot night markets in Taiwan. The place details is saved in new
    json file.

    :param csvfile_path: csv file path to open.
    :type csvfile_path: str | Path

    :returns: If requests.Exception,no founding ID/details or file I/O exceptioon, 
    it will return None. Otherwise, the path of generated json file is returned.
    :rtype: str | None
    """
    if not API_KEY:
        return "找不到 API 金鑰，請確認 .env 檔"

    # 讀取csv，取得所有夜市名稱
    df_markets = pd.read_csv(Path(csvfile_path), sep=",")
    nm_names = df_markets["Night_market_name"]

    all_details_json = []  # 用來儲存所有夜市的原始 details(decoded-json)
    failure_id_list = []
    failure_detail_list = []
    for name in nm_names:
        print(f"====正在查詢：{name} 的place ID...====")
        place_id = search_place_id(name)
        if not place_id:
            print(f"找不到 {name} 的place ID")
            failure_id_list.append(name)
            continue

        print(f"====已取得{name}的place_id，正在進一步查詢地理位置細節...====")
        details = get_place_details(place_id)

        if not details:
            print(f"找不到{name}的地理位置細節")
            failure_detail_list.append(name)
            continue

        print(f"====已取得{name}的地理位置細節====")
        all_details_json.append(details)

    # 合併儲存所有夜市 details 到同一個json
    # 定義存檔路徑，並確保資料夾存在
    curr_working_dir = Path().resolve()  # 取得專案根目錄的絕對路徑
    raw_data_save_dir = curr_working_dir/"test"/"raw_data"
    raw_data_save_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date()
    jsonfile_name = raw_data_save_dir/f"Taiwan_night_markets_from_map_api_{today}.json"
    try:
        with open(jsonfile_name, "w", encoding="utf-8") as f:
            # 將字典 dict 型別的資料，寫入本機json檔案。
            json.dump(all_details_json, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error on writing into JSON file. {e}")
        return None
    else:
        print(f"全部夜市地理資訊已成功輸出到：{jsonfile_name}，"
              f"總計找到了: {len(all_details_json)}個夜市資訊。"
              f"失敗率: {(len(failure_detail_list) + len(failure_id_list))} / {len(nm_names)}")
        return jsonfile_name


if __name__ == "__main__":
    night_market_list_csv = find_tw_night_markets_list(night_markets_wiki_url, headers, cities_per_region)
    # e_crawling_nightmarket(night_market_list_csv)
