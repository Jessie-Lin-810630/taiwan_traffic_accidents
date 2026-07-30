"""Extract 階段：自 data.gov.tw 下載並解壓縮交通事故開放資料。"""

import re
import zipfile
from datetime import datetime
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# 指定要爬取的網址
historical_years_urls = [
    "https://data.gov.tw/dataset/158865",  # 2021
    "https://data.gov.tw/dataset/161199",  # 2022
    "https://data.gov.tw/dataset/167905",  # 2023
    "https://data.gov.tw/dataset/172969",  # 2024
    "https://data.gov.tw/dataset/177136",
]  # 2025
this_year_A1_url = ["https://data.gov.tw/dataset/12818"]  # 2026A1
this_year_A2_url = ["https://data.gov.tw/dataset/13139"]  # 2026A2

# 準備headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
}


def find_download_links(urls: list[str], headers: dict) -> dict[str, str]:
    """從指定的url清單中尋找、蒐集下載連結。

    Parameters:
        urls (list[str]): 要爬取的網頁URL列表。
        headers (dict): 請求headers。
    Returns:
        dict[str, str]: 包含下載連結和年份標題的字典。
    """
    download_links = {}
    for url in urls:
        response = None
        soup = None
        try:
            response = requests.get(url, headers=headers, verify=False, timeout=120)
            if response.status_code == 200:
                logger.info(f"====成功訪問 {url}====")
                soup = BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.Timeout:
            logger.error(
                f"Timeout occurred while fetching download links from {url}",
            )
            raise
        except requests.exceptions.ConnectionError:
            logger.error(
                f"Connection error while fetching download links from {url}",
            )
            raise
        except requests.exceptions.HTTPError:
            logger.error(f"HTTP error while fetching download links from {url}")
            raise
        except Exception:
            logger.error(
                f"Unexpected error while fetching download links from {url}",
            )
            raise
        else:
            # Only process if soup was successfully created
            if soup is not None:
                # page_topic = soup.find('h2', class_='print-title').text.strip()
                page_topic = None
                if soup.select_one(
                    "#__nuxt > div > div > main > div.page > div.table.table--fixed.od-table.od-table--bordered.print-table > div:nth-child(2) > div:nth-child(2) > ul:nth-child(1) > li > span"
                ):
                    page_topic = soup.select_one(
                        "#__nuxt > div > div > main > div.page > div.table.table--fixed.od-table.od-table--bordered.print-table > div:nth-child(2) > div:nth-child(2) > ul:nth-child(1) > li > span"
                    )
                    page_topic = page_topic.text.strip().replace(".zip", "")
                for a_tag in soup.find_all("a", title=re.compile("下載檔案")):
                    href = a_tag.get("href")
                    available_file_type = (
                        a_tag.get("title").replace("下載檔案", "").strip()
                    )
                    if href:
                        logger.info(f"====成功找到下載連結: {href}====")
                        download_links[href] = (available_file_type, page_topic)
    return download_links


def download_and_extract_zip(
    download_link: str,
    zipfile_save_dir: str | Path,
    zipfile_name: str,
    unzipfile_save_dir: str | Path,
) -> list[str] | None:
    """下載zip檔案並解壓縮到指定資料夾。

    Parameters:
        download_link (str): zip檔案的url。
        zipfile_save_dir (str | Path): zip檔案存放的資料夾。
        zipfile_name (str): zip檔案的名稱，應加上.zip副檔名。
        unzipfile_save_dir (str | Path): 解壓縮後檔案存放的資料夾。
    Returns:
        list[str] | None: 解壓縮後的csv檔案路徑列表。
    """
    zipfile_save_dir = Path(str(zipfile_save_dir))
    unzipfile_save_dir = Path(str(unzipfile_save_dir))
    zipfile_name = (
        Path(str(zipfile_name))
        if zipfile_name.endswith(".zip")
        else Path(str(zipfile_name) + ".zip")
    )

    try:
        response = requests.get(
            download_link, headers=headers, stream=True, verify=False, timeout=300
        )
        # 如果成功回傳200，才下載zip檔案
        if response.status_code == 200:
            # 下載zip檔案到raw_data資料夾
            logger.info(f"====下載zip檔案中: {download_link}====")
            zipfile_path = zipfile_save_dir / zipfile_name
            with open(zipfile_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        else:
            logger.error(
                f"無法下載zip檔案，response.status_code: {response.status_code}"
            )
            return None
    except Exception:
        logger.error("下載或儲存zip檔案過程中發生錯誤")
        raise
    else:
        logger.info(f"====成功下載並儲存zip檔案至: {str(zipfile_path)}====")

    # 解壓縮並存到processed_data資料夾
    try:
        logger.info("====準備解壓縮...====")
        with zipfile.ZipFile(zipfile_path, "r") as z:
            logger.info(f"====成功讀取zip檔案: {str(zipfile_name)}====")
            extracted_files = z.infolist()  # 這裡是解壓縮後的zipinstances列表

            csvfile_pathlist = []  # 用來存放找到的csv檔案路徑，最後會回傳這個列表

            # 遍歷每個解壓縮後的檔案，尋找csv檔案並處理可能的亂碼問題
            for f in extracted_files:
                logger.info("====尋找csv檔案並存檔中...====")
                # 處理在亞洲國家容易發生的zip檔案亂碼問題：
                # 已知檔案內容一定有中文字，但不能確定zip標記是否有加UTF-8、不能確定標記是否丟失。
                # 此時可能會因此退回用cp437解碼而發生中文字解析失敗，所以此時f.filename一開始會是亂碼。
                # 要糾正f.filename，先嘗試用cp437編碼回原始bytes，然後再用utf-8或cp950解碼；
                # 如果cp437編碼這關報錯，有可能是我的系統環境本身自動辨認到要用utf-8解碼，此時就直接使用原始檔名f.filename。
                try:
                    filename = f.filename.encode("cp437").decode("utf-8")
                except (UnicodeEncodeError, UnicodeDecodeError):
                    try:
                        filename = f.filename.encode("cp437").decode("cp950")
                    except Exception:
                        filename = f.filename

                if filename.endswith(".csv") and ("A1" in filename or "A2" in filename):
                    # 存下csv檔案，optional: 直接讀取csv檔案內容到DataFrame
                    with (
                        z.open(f, mode="r") as source,
                        open(unzipfile_save_dir / filename, mode="wb") as target,
                    ):
                        target.write(source.read())
                        csvfile_pathlist.append(str(unzipfile_save_dir / filename))

            else:
                logger.info(
                    f"====解壓縮完成，成功找到{len(csvfile_pathlist)}個csv檔案===="
                )
                return csvfile_pathlist  # 回傳找到的csv檔案路徑列表

    except Exception:
        logger.error("解壓縮zip檔或存成csv檔的過程中發生錯誤")
        raise


def download_csv(
    download_link: str, csvfile_name: str, csvfile_save_dir: str | Path
) -> list[str] | None:
    """下載csv檔案到指定資料夾。

    Parameters:
        download_link (str): csv檔案的url。
        csvfile_name (str): csv檔案的名稱，應加上.csv副檔名。
        csvfile_save_dir (str | Path): csv檔案存放的資料夾。
    Returns:
        list[str] | None: 解壓縮後的csv檔案路徑列表。
    """
    csvfile_save_dir = Path(str(csvfile_save_dir))
    csvfile_name = (
        Path(str(csvfile_name))
        if csvfile_name.endswith(".csv")
        else Path(str(csvfile_name) + ".csv")
    )

    try:
        response = requests.get(
            download_link, headers=headers, stream=True, verify=False, timeout=300
        )
        # 如果成功回傳200，才下載csv檔案
        if response.status_code == 200:
            # 下載csv檔案到raw_data資料夾
            logger.info(f"====下載csv檔案中: {download_link}====")
            csvfile_pathlist = []  # 用來存放找到的csv檔案路徑，最後會回傳這個列表
            csvfile_path = csvfile_save_dir / csvfile_name
            with open(csvfile_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        else:
            logger.error(
                f"無法下載csv檔案，response.status_code: {response.status_code}"
            )
            return None
    except Exception:
        logger.error("下載或儲存csv檔案過程中發生錯誤")
        raise
    else:
        csvfile_pathlist.append(str(csvfile_path))
        logger.info(f"====成功下載並儲存csv檔案至: {str(csvfile_path)}====")
        return csvfile_pathlist


def e_crawling_historical_traffic_accident(
    historical_years_urls: list[str], headers: dict[str, str]
) -> list[str] | None:
    """執行爬取歷年交通事故資料的任務。

    歷年資料會下載zip檔案並解壓縮，解壓縮後的csv檔案會存到processed_data資料夾；
    爬取後會驗證所有csv檔案的編碼是否為UTF-8，如果不是則刪除該檔案。
    最後會回傳所有成功爬取並解壓縮的csv檔案路徑列表；如果過程中任一csv檔案發生錯誤則不計入列表。
    Parameters:
        historical_years_urls (list[str]): 歷年資料的網頁URL列表。
        headers (dict[str, str]): 請求headers。
    Returns:
        list[str] | None: 成功爬取並解壓縮的csv檔案路徑列表。
    """
    # 定義存檔路徑，並確保資料夾存在
    curr_working_dir = Path().resolve()  # 取得專案根目錄的絕對路徑
    raw_data_save_dir = curr_working_dir / "test" / "raw_data"  # 定義存檔資料夾路徑
    processed_data_save_dir = (
        curr_working_dir / "test" / "processed_data"
    )  # 定義存檔資料夾路徑
    raw_data_save_dir.mkdir(parents=True, exist_ok=True)
    processed_data_save_dir.mkdir(parents=True, exist_ok=True)

    # 關閉「不安全請求」的警告，因為等下會使用 verify=False (跳過憑證檢查)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 爬取歷年資料
    historical_download_links = find_download_links(historical_years_urls, headers)
    csvfile_paths_historical = []
    for download_link, (file_type, page_topic) in historical_download_links.items():
        zipfile_name = f"{page_topic}_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
        csvfile_paths_a_hist_year = download_and_extract_zip(
            download_link, raw_data_save_dir, zipfile_name, processed_data_save_dir
        )
        csvfile_paths_historical.extend(csvfile_paths_a_hist_year)
    # validate_csv_encoding(csvfile_paths_historical)
    return csvfile_paths_historical


def e_crawling_latest_traffic_accident(
    A1_url: list[str], A2_url: list[str], headers: dict[str, str]
) -> list[str] | None:
    """執行爬取今年A1、A2交通事故資料的任務。

    今年A1、A2資料會下載csv檔案或zip檔案，如果是zip檔案則會解壓縮，先存在raw_data資料夾、
    解壓縮後的csv檔案會存到processed_data資料夾；
    如果是不需解壓縮即可取得的csv檔案，則直接存到raw_data資料夾。
    爬取後會驗證所有csv檔案的編碼是否為UTF-8，如果不是則刪除該檔案。
    最後會回傳所有成功爬取並解壓縮的csv檔案路徑列表；如果過程中任一csv檔案發生錯誤則不計入列表。
    Parameters:
        A1_url (list[str]): 今年A1資料的網頁URL列表。
        A2_url (list[str]): 今年A2資料的網頁URL列表。
        headers (dict[str, str]): 請求headers。
    Returns:
        list[str] | None: 成功爬取並解壓縮的csv檔案路徑列表。
    """
    # 定義存檔路徑，並確保資料夾存在
    curr_working_dir = Path().resolve()  # 取得專案根目錄的絕對路徑
    raw_data_save_dir = curr_working_dir / "test" / "raw_data"  # 定義存檔資料夾路徑
    processed_data_save_dir = (
        curr_working_dir / "test" / "processed_data"
    )  # 定義存檔資料夾路徑
    raw_data_save_dir.mkdir(parents=True, exist_ok=True)
    processed_data_save_dir.mkdir(parents=True, exist_ok=True)

    # 關閉「不安全請求」的警告，因為等下會使用 verify=False (跳過憑證檢查)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 爬取今年A1A2資料
    this_year_A1A2_url = A1_url + A2_url
    this_year_download_links = find_download_links(this_year_A1A2_url, headers)
    csvfile_paths_this_year = []
    for download_link, (file_type, page_topic) in this_year_download_links.items():
        file_type = file_type.lower()
        if file_type == "csv":
            csvfile_name = f"{page_topic}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            csvfile_paths_this_year_c = download_csv(
                download_link, csvfile_name, raw_data_save_dir
            )
            csvfile_paths_this_year.extend(csvfile_paths_this_year_c)

        elif file_type == "zip":
            zipfile_name = f"{page_topic}_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
            csvfile_paths_this_year_z = download_and_extract_zip(
                download_link, raw_data_save_dir, zipfile_name, processed_data_save_dir
            )
            csvfile_paths_this_year.extend(csvfile_paths_this_year_z)

        else:
            logger.info(f"錯誤: 找到的檔案類型 {file_type} 不受支援，無法下載。")
            continue
    return csvfile_paths_this_year
