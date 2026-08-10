"""Extract 階段：自 data.gov.tw 下載並解壓縮交通事故開放資料。

抓取與重試的通用能力在 `src.util.crawling_utils`，本模組只負責 data.gov.tw
專屬的部分：抓哪些頁面、頁面上的下載連結長在哪、檔案怎麼命名、存到哪裡。

模組層常數含歷年（2021～2025）資料集網址與今年 A1、A2 兩個資料集網址。
data.gov.tw 的憑證鏈有問題，因此 `VERIFY_SSL` 設為 `False`，是全專案唯一停用
SSL 驗證之處；模組載入時會記一筆 warning 讓這件事在日誌裡留下痕跡。

Notes:
    停用 SSL 驗證的判斷參考 ADR-0006。
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
from src.util.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR

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
    """逐一走訪 data.gov.tw 資料集頁面，蒐集頁面上的下載連結。

    每個頁面取兩件事：檔案下載區塊的標題（作為之後的檔名），以及所有 title
    含「下載檔案」的連結與其檔案類型。任一頁面抓取失敗即中止並往外拋，
    因為少一個年度的下載連結會讓後續的 ETL 靜默地少一批資料。

    Args:
        urls (list[str]): 要走訪的資料集頁面網址。
        headers (dict): 請求標頭。

    Returns:
        dict[str, str]: 以下載連結為鍵，值是（檔案類型, 頁面標題）的 tuple，形如：

            {
                "https://.../158865.zip": ("ZIP", "110年度A1及A2類交通事故資料"),
                "https://.../12818.csv": ("CSV", "115年度A1類交通事故資料"),
            }

    Raises:
        requests.exceptions.RequestException: 頁面抓取失敗且重試耗盡。

    Notes:
        「一頁失敗就整批中止」的取捨參考 ADR-0006。
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
    """判斷壓縮檔內的某個檔案是否要取出。

    只取交通事故的 CSV，也就是檔名含 A1（死亡事故）或 A2（受傷事故）者。

    Args:
        filename (str): 壓縮檔內的檔名。

    Returns:
        bool: 要取出為 `True`，略過為 `False`。
    """
    return filename.endswith(".csv") and ("A1" in filename or "A2" in filename)


def _prepare_save_dirs() -> tuple[Path, Path]:
    """建立並回傳原始檔與清洗後檔案的兩個存檔目錄。

    目錄已存在時不會報錯。路徑取自 `src.util.paths`，以專案根為基準，
    不隨行程的工作目錄漂移。

    Returns:
        tuple[Path, Path]: (原始檔目錄, 解壓後檔案目錄)，即
            `data/raw` 與 `data/processed`。

    Notes:
        路徑取得方式參考 ADR-0007。
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return RAW_DATA_DIR, PROCESSED_DATA_DIR


def _timestamped_name(page_topic: str, suffix: str) -> str:
    """以頁面標題與當下時間組出檔名，避免重跑時覆蓋掉前一次下載的檔案。

    Args:
        page_topic (str): 頁面上的檔案下載區塊標題。
        suffix (str): 副檔名，含點號，例如 `".zip"`。

    Returns:
        str: 形如 `"110年度A1及A2類交通事故資料_20260805143012.zip"` 的檔名。
    """
    return f"{page_topic}_{datetime.now().strftime('%Y%m%d%H%M%S')}{suffix}"


def e_crawling_historical_traffic_accident(
    historical_years_urls: list[str], headers: dict[str, str]
) -> list[str]:
    """下載歷年交通事故資料並解壓縮，回傳解出來的 CSV 路徑。

    歷年資料一律是 ZIP：ZIP 存進 `data/raw`，解出來的 A1、A2 CSV 存進
    `data/processed`。任一年度下載失敗即中止並往外拋，因為少一個年度會讓後續的
    ETL 靜默地少一批資料；暫時性故障（429、5xx、連線錯誤）已在下載層重試過。

    Args:
        historical_years_urls (list[str]): 歷年資料的資料集頁面網址。
        headers (dict[str, str]): 請求標頭。

    Returns:
        list[str]: 解壓出來的 CSV 檔案路徑，形如：

            [
                "data/processed/110年度A1類交通事故資料.csv",
                "data/processed/110年度A2類交通事故資料.csv",
            ]

    Raises:
        requests.exceptions.RequestException: 頁面抓取或檔案下載失敗且重試耗盡。
        zipfile.BadZipFile: 下載回來的檔案不是有效的 ZIP。

    Notes:
        「一年失敗就整批中止」的取捨參考 ADR-0006。
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
    """下載今年的 A1、A2 交通事故資料，回傳 CSV 路徑。

    今年度的資料集有時提供 CSV、有時提供 ZIP，因此依頁面標示的檔案類型分流：
    CSV 直接存進 `data/raw`；ZIP 存進 `data/raw` 後解壓到 `data/processed`。
    兩者以外的檔案類型只記一筆日誌後跳過。任一檔案下載失敗即中止並往外拋。

    Args:
        A1_url (list[str]): 今年 A1（死亡事故）資料的資料集頁面網址。
        A2_url (list[str]): 今年 A2（受傷事故）資料的資料集頁面網址。
        headers (dict[str, str]): 請求標頭。

    Returns:
        list[str]: 下載或解壓出來的 CSV 檔案路徑，形如：

            [
                "data/raw/115年度A1類交通事故資料_20260805143012.csv",
                "data/processed/115年度A2類交通事故資料.csv",
            ]

    Raises:
        requests.exceptions.RequestException: 頁面抓取或檔案下載失敗且重試耗盡。
        zipfile.BadZipFile: 下載回來的檔案不是有效的 ZIP。

    Notes:
        「一檔失敗就整批中止」的取捨參考 ADR-0006。
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
