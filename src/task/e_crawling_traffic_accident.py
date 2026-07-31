"""Extract 階段：自 data.gov.tw 下載並解壓縮交通事故開放資料。

下載與解析的實作在 `src.util.crawling_utils`（含暫時性故障的重試，見 ADR-0006）；
本模組只負責決定「抓哪些頁面、檔案怎麼命名、存到哪裡」。
"""

import re
from datetime import datetime
from pathlib import Path

import urllib3

from src.util.crawling_utils import (
    download_and_extract_zip,
    download_csv,
    fetch_soup,
)
from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# data.gov.tw 的憑證鏈有問題，本模組是全專案唯一停用 SSL 驗證之處（ADR-0006 子決策 6）。
# `crawling_utils` 的預設是 verify=True，停用與否由呼叫端明確傳入。
VERIFY_SSL = False

# 只停用 InsecureRequestWarning，避免每次呼叫 download_csv() 都噴一次警告到 stderr 導致 log 落落長，
# 但也不用 disable_warnings() 全關（會遮蔽其他種類的安全警告）。
# 改用 logger.warning 在模組載入時印出一次，確保行為可追蹤。
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger.warning(
    "SSL certificate verification is disabled (verify=False). "
    "Only use this for trusted internal endpoints."
)

# 指定要爬取的網址
historical_years_urls = [
    "https://data.gov.tw/dataset/158865",  # 2021
    "https://data.gov.tw/dataset/161199",  # 2022
    "https://data.gov.tw/dataset/167905",  # 2023
    "https://data.gov.tw/dataset/172969",  # 2024
    "https://data.gov.tw/dataset/177136",  # 2025
]
this_year_A1_url = ["https://data.gov.tw/dataset/12818"]  # 2026A1
this_year_A2_url = ["https://data.gov.tw/dataset/13139"]  # 2026A2

# 準備headers
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
}


def find_download_links(urls: list[str], headers: dict) -> dict[str, str]:
    """從 data.gov.tw 的資料集頁面蒐集下載連結。

    解析規則（CSS 選擇器、「下載檔案」的 title）是 data.gov.tw 專屬的，
    因此住在本模組而非 `crawling_utils`；後者只提供通用的 `fetch_soup()`。

    任一頁面抓取失敗即中止並拋出 —— 少一個年度的下載連結會讓後續的
    ETL 靜默地少一批資料（ADR-0006）。

    Parameters:
        urls (list[str]): 要爬取的網頁URL列表。
        headers (dict): 請求headers。
    Returns:
        dict[str, str]: 下載連結 → (檔案類型, 頁面標題)。
    """
    download_links = {}
    for url in urls:
        soup = fetch_soup(url, headers, verify=VERIFY_SSL)

        page_topic = None

        # data.gov.tw 頁面上「檔案下載」區塊的位置；改版時要跟著調整。
        _DOWNLOAD_LINK_SELECTOR = (
            "#__nuxt > div > div > main > div.page > "
            "div.table.table--fixed.od-table.od-table--bordered.print-table > "
            "div:nth-child(2) > div:nth-child(2) > ul:nth-child(1) > li > span"
        )

        if soup.select_one(_DOWNLOAD_LINK_SELECTOR):
            page_topic = (
                soup.select_one(_DOWNLOAD_LINK_SELECTOR)
                .text.strip()
                .replace(".zip", "")
            )
        for a_tag in soup.find_all("a", title=re.compile("下載檔案")):
            href = a_tag.get("href")
            available_file_type = a_tag.get("title").replace("下載檔案", "").strip()
            if href:
                logger.info(f"成功找到下載連結: {href}")
                download_links[href] = (available_file_type, page_topic)

    return download_links


def _is_accident_csv(filename: str) -> bool:
    """壓縮檔內只取交通事故的 csv：A1（死亡）與 A2（受傷）兩類。"""
    return filename.endswith(".csv") and ("A1" in filename or "A2" in filename)


def _prepare_save_dirs() -> tuple[Path, Path]:
    """建立並回傳 (raw_data, processed_data) 兩個存檔資料夾。"""
    curr_working_dir = Path().resolve()  # 取得專案根目錄的絕對路徑
    raw_data_save_dir = curr_working_dir / "test" / "raw_data"
    processed_data_save_dir = curr_working_dir / "test" / "processed_data"
    raw_data_save_dir.mkdir(parents=True, exist_ok=True)
    processed_data_save_dir.mkdir(parents=True, exist_ok=True)
    return raw_data_save_dir, processed_data_save_dir


def _timestamped_name(page_topic: str, suffix: str) -> str:
    """以頁面標題與當下時間組出檔名。"""
    return f"{page_topic}_{datetime.now().strftime('%Y%m%d%H%M%S')}{suffix}"


def e_crawling_historical_traffic_accident(
    historical_years_urls: list[str], headers: dict[str, str]
) -> list[str]:
    """執行爬取歷年交通事故資料的任務。

    歷年資料會下載 zip 檔案並解壓縮，解壓縮後的 csv 檔案會存到 processed_data 資料夾。

    任一年度下載失敗即中止並拋出（ADR-0006）—— 少一個年度會讓後續的
    ETL 靜默地少一批資料。暫時性故障（5xx、429、連線錯誤）已於
    `crawling_utils` 層重試過。

    Parameters:
        historical_years_urls (list[str]): 歷年資料的網頁URL列表。
        headers (dict[str, str]): 請求headers。
    Returns:
        list[str]: 成功爬取並解壓縮的csv檔案路徑列表。

    Raises:
        requests.exceptions.RequestException: 抓取或下載失敗（重試耗盡後）。
    """
    raw_data_save_dir, processed_data_save_dir = _prepare_save_dirs()

    historical_download_links = find_download_links(historical_years_urls, headers)
    csvfile_paths_historical = []
    for download_link, (file_type, page_topic) in historical_download_links.items():
        zipfile_name = _timestamped_name(page_topic, ".zip")
        csvfile_paths_a_hist_year = download_and_extract_zip(
            download_link,
            headers,
            raw_data_save_dir,
            zipfile_name,
            processed_data_save_dir,
            filename_filter=_is_accident_csv,
            verify=VERIFY_SSL,
        )
        csvfile_paths_historical.extend(csvfile_paths_a_hist_year)

    return csvfile_paths_historical


def e_crawling_latest_traffic_accident(
    A1_url: list[str], A2_url: list[str], headers: dict[str, str]
) -> list[str]:
    """執行爬取今年A1、A2交通事故資料的任務。

    今年 A1、A2 資料可能是 csv 或 zip：zip 會先存進 raw_data 再解壓縮到
    processed_data；csv 則直接存到 raw_data。

    任一檔案下載失敗即中止並拋出（ADR-0006）。不支援的檔案類型會跳過並記錄。

    Parameters:
        A1_url (list[str]): 今年A1資料的網頁URL列表。
        A2_url (list[str]): 今年A2資料的網頁URL列表。
        headers (dict[str, str]): 請求headers。
    Returns:
        list[str]: 成功爬取並解壓縮的csv檔案路徑列表。

    Raises:
        requests.exceptions.RequestException: 抓取或下載失敗（重試耗盡後）。
    """
    raw_data_save_dir, processed_data_save_dir = _prepare_save_dirs()

    this_year_A1A2_url = A1_url + A2_url
    this_year_download_links = find_download_links(this_year_A1A2_url, headers)
    csvfile_paths_this_year = []
    for download_link, (file_type, page_topic) in this_year_download_links.items():
        file_type = file_type.lower()
        if file_type == "csv":
            csvfile_name = _timestamped_name(page_topic, ".csv")
            csvfile_paths_this_year.extend(
                download_csv(
                    download_link,
                    headers,
                    csvfile_name,
                    raw_data_save_dir,
                    verify=VERIFY_SSL,
                )
            )

        elif file_type == "zip":
            zipfile_name = _timestamped_name(page_topic, ".zip")
            csvfile_paths_this_year.extend(
                download_and_extract_zip(
                    download_link,
                    headers,
                    raw_data_save_dir,
                    zipfile_name,
                    processed_data_save_dir,
                    filename_filter=_is_accident_csv,
                    verify=VERIFY_SSL,
                )
            )

        else:
            logger.info(f"錯誤: 找到的檔案類型 {file_type} 不受支援，無法下載。")
            continue

    return csvfile_paths_this_year
