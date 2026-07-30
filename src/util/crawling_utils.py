"""爬蟲共用工具：蒐集下載連結、下載檔案與解壓縮。"""

import re
import zipfile
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# 只停用 InsecureRequestWarning，避免每次呼叫 download_csv() 都噴一次警告到 stderr 導致 log 落落長，
# 但也不用 disable_warnings() 全關（會遮蔽其他種類的安全警告）。
# 改用 logger.warning 在模組載入時印出一次，確保行為可追蹤。
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger.warning(
    "SSL certificate verification is disabled (verify=False). "
    "Only use this for trusted internal endpoints."
)


def find_download_links(urls: list[str], headers: dict) -> dict[str, str]:
    """從指定的 url 清單中尋找、蒐集下載連結。

    Parameters:
        urls (list[str]): 要爬取的網頁URL列表。
        headers (dict): 請求headers。
    Returns:
        dict[str, str]: 包含下載連結和年份標題的字典。
    """
    download_links = {}
    for url in urls:
        soup = None
        try:
            logger.info(f"開始發送請求至: {url}")
            response = requests.get(url, headers=headers, verify=False, timeout=30)

            # 確保 HTTP 狀態碼為 200，否則主動拋出 HTTPError 進入 except 區塊
            response.raise_for_status()
            logger.info(f"成功訪問 {url}，狀態碼: {response.status_code}")
            soup = BeautifulSoup(response.text, "html.parser")

        except requests.exceptions.Timeout:
            logger.error(f"請求超時 (Timeout) -> URL: {url}", exc_info=True)
            raise

        except requests.exceptions.ConnectionError:
            logger.error(f"連線失敗 (ConnectionError) -> URL: {url}", exc_info=True)
            raise

        except requests.exceptions.HTTPError:
            logger.error(f"HTTP 回應異常 (HTTPError) -> URL: {url}", exc_info=True)
            raise

        except Exception:
            logger.error(f"未預期的錯誤 -> URL: {url}", exc_info=True)
            raise

        else:
            if soup is not None:
                page_topic = None
                selector = (
                    "#__nuxt > div > div > main > div.page > "
                    "div.table.table--fixed.od-table.od-table--bordered.print-table > "
                    "div:nth-child(2) > div:nth-child(2) > ul:nth-child(1) > li > span"
                )
                if soup.select_one(selector):
                    page_topic = (
                        soup.select_one(selector).text.strip().replace(".zip", "")
                    )
                for a_tag in soup.find_all("a", title=re.compile("下載檔案")):
                    href = a_tag.get("href")
                    available_file_type = (
                        a_tag.get("title").replace("下載檔案", "").strip()
                    )
                    if href:
                        logger.info(f"成功找到下載連結: {href}")
                        download_links[href] = (available_file_type, page_topic)

    return download_links


def iterate_crawling_similar_urls(urls: list[str], headers: dict) -> dict:
    """從多個 html 結構相似的 urls 清單，變歷每個 url，做重複爬蟲。

    Parameters:
        urls (list[str]): 要爬取的網頁URL列表。
        headers (dict): 請求headers。
    Returns:
        dict: 使用 dict 存放各 url 爬取的結果。
    """
    result = {}
    for url in urls:
        soup = None
        try:
            logger.info(f"開始發送請求至: {url}")
            response = requests.get(url, headers=headers, verify=False, timeout=30)

            # 確保 HTTP 狀態碼為 2xx，否則主動拋出 HTTPError 進入 except 區塊
            response.raise_for_status()
            logger.info(f"成功訪問 {url}，狀態碼: {response.status_code}")
            soup = BeautifulSoup(response.text, "html.parser")

        except requests.exceptions.Timeout:
            logger.error(f"請求超時 (Timeout) -> URL: {url}", exc_info=True)
            raise  # 視情況可以不 raise，僅跳過這個 url 、接續下一個 url

        except requests.exceptions.ConnectionError:
            logger.error(f"連線失敗 (ConnectionError) -> URL: {url}", exc_info=True)
            raise  # 視情況可以不 raise，僅跳過這個 url 、接續下一個 url

        except requests.exceptions.HTTPError:
            logger.error(f"HTTP 回應異常 (HTTPError) -> URL: {url}", exc_info=True)
            raise  # 視情況可以不 raise，僅跳過這個 url 、接續下一個 url

        except Exception:
            logger.error(f"未預期的錯誤 -> URL: {url}", exc_info=True)
            raise  # 視情況可以不 raise，僅跳過這個 url 、接續下一個 url

        else:
            if soup is not None:
                # 以下放入你的爬蟲邏輯，例如：

                # page_topic = None
                # selector = (
                #     "#__nuxt > div > div > main > div.page > "
                #     "div.table.table--fixed.od-table.od-table--bordered.print-table > "
                #     "div:nth-child(2) > div:nth-child(2) > ul:nth-child(1) > li > span"
                # )
                # if soup.select_one(selector):
                #     page_topic = soup.select_one(selector).text.strip().replace(".zip", "")
                # for a_tag in soup.find_all("a", title=re.compile("下載檔案")):
                #     href = a_tag.get("href")
                #     available_file_type = a_tag.get("title").replace("下載檔案", "").strip()
                #     if href:
                #         logger.info(f"成功找到下載連結: {href}")
                #         result[href] = (available_file_type, page_topic)

                result["your_key"] = "value_you_find"  # 根據需求設計
            else:
                logger.warning(f"Not found target information in {url}")
    return result


def download_and_extract_zip(
    download_link: str,
    headers: dict,
    zipfile_save_dir: str | Path,
    zipfile_name: str,
    unzipfile_save_dir: str | Path,
) -> list[str]:
    """Download a ZIP file from the given URL and extract its contents to the specified directory.

    Parameters:
        download_link (str): URL to download the zip file.
        headers (dict): Headers for HTTP request.
        zipfile_save_dir (str | Path): Path of directory to save the downloaded zip file.
        zipfile_name (str): File name for the zip file; should include the ``.zip`` extension as suffix.
        unzipfile_save_dir (str | Path): Path of directory to extract the zip contents into.

    Returns:
        list[str] : Paths of extracted CSV files.

    Raises:
        requests.exceptions.*: Original exceptions are logged then re-raised, so the
            traceback stays intact for the caller (Airflow task log or local stderr).
    """
    zipfile_save_dir = Path(str(zipfile_save_dir))
    unzipfile_save_dir = Path(str(unzipfile_save_dir))
    zipfile_save_dir.mkdir(parents=True, exist_ok=True)
    unzipfile_save_dir.mkdir(parents=True, exist_ok=True)

    zipfile_name = Path(zipfile_name)
    if zipfile_name.suffix != ".zip":
        zipfile_name = zipfile_name.with_suffix(".zip")

    zipfile_path = zipfile_save_dir / zipfile_name
    try:
        logger.info(f"==== Downloading ZIP file from URL: {download_link} ====")
        response = requests.get(
            download_link, headers=headers, stream=True, verify=False, timeout=300
        )
        response.raise_for_status()

        with open(zipfile_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    except requests.exceptions.Timeout:
        logger.error(f"Timeout -> URL: {download_link}", exc_info=True)
        raise

    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error -> URL: {download_link}", exc_info=True)
        raise

    except requests.exceptions.HTTPError:
        logger.error(f"HTTP error -> URL: {download_link}", exc_info=True)
        raise

    except Exception:
        logger.error("Unexpected error during zip download or save.", exc_info=True)
        raise

    else:
        logger.info(f"==== ZIP file saved to: {str(zipfile_path)} ====")

    try:
        logger.info("==== Preparing to extract ZIP file... ====")
        with zipfile.ZipFile(zipfile_path, "r") as z:
            logger.info(f"==== ZIP file opened successfully: {str(zipfile_name)} ====")
            extracted_files = z.infolist()  # 這裡是解壓縮後的zipinstances列表
            csvfile_pathlist = []

            # 遍歷每個解壓縮後的檔案，尋找csv檔案並處理可能的亂碼問題
            for f in extracted_files:
                logger.info("==== Searching for CSV files and saving... ====")
                # Handle potential filename encoding issues common in Asian locales:
                # try cp437 -> utf-8 first, then fall back to cp437 -> cp950.
                # File contents are known to contain Chinese characters, but the ZIP entry
                # flags may or may not indicate UTF-8. If the flag is missing or lost,
                # Python falls back to cp437, producing garbled filenames.
                # To recover the original filename, we re-encode with cp437 and decode
                # with utf-8 or cp950. If the cp437 encode step itself raises an error,
                # the runtime likely already decoded the filename correctly as utf-8,
                # so we use f.filename as-is.
                try:
                    filename = f.filename.encode("cp437").decode("utf-8")
                except (UnicodeEncodeError, UnicodeDecodeError):
                    try:
                        filename = f.filename.encode("cp437").decode("cp950")
                    except Exception:
                        filename = f.filename

                # Save only CSV files whose names contain specific keywords.
                # Adjust ".csv", "A1", "A2" as needed.
                if filename.endswith(".csv") and ("A1" in filename or "A2" in filename):
                    with (
                        z.open(f, mode="r") as source,
                        open(unzipfile_save_dir / filename, mode="wb") as target,
                    ):
                        target.write(source.read())
                        csvfile_pathlist.append(str(unzipfile_save_dir / filename))

                    # [optional] Read CSV directly into a DataFrame without saving to disk.
                    # with z.open(f, mode="r") as source:
                    #     df = pd.read_csv(source)
                    #     csvfile_pathlist.append(df)
    except Exception:
        logger.error(
            "Unexpected error during zip extraction or CSV save.", exc_info=True
        )
        raise

    logger.info(
        f"==== Extraction complete. {len(csvfile_pathlist)} CSV file(s) found. ===="
    )
    return csvfile_pathlist


def download_csv(
    download_link: str, csvfile_name: str, csvfile_save_dir: str | Path
) -> list[str]:
    """Download CSV file from a passed URL to a targeted directory.

    Parameters:
        download_link (str): URL to download csv file.
        csvfile_name (str): File name to save and it should include the file extension ``.csv`` as suffix.
        csvfile_save_dir (str | Path): Path of directory to save the csv file.

    Returns:
        list[str]: Paths of successfully saved CSV files.

    Raises:
        requests.exceptions.*: Original exceptions are logged then re-raised, so the
            traceback stays intact for the caller (Airflow task log or local stderr).
    """
    csvfile_save_dir = Path(str(csvfile_save_dir))
    csvfile_save_dir.mkdir(parents=True, exist_ok=True)

    csvfile_name = Path(csvfile_name)
    if csvfile_name.suffix != ".csv":
        csvfile_name = csvfile_name.with_suffix(".csv")

    csvfile_path = csvfile_save_dir / csvfile_name
    try:
        logger.info(f"==== Downloading CSV file from URL: {download_link} ====")
        response = requests.get(
            download_link,
            headers={},
            stream=True,  # 為了搭配後方 response.iter_content() 使用
            verify=False,
            timeout=300,
        )
        response.raise_for_status()
        with open(csvfile_path, "wb") as f:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):  # 每次只讀 1 MB 到記憶體後寫入f
                if chunk:
                    f.write(chunk)

    except requests.exceptions.Timeout:
        logger.error(f"Timeout -> URL: {download_link}", exc_info=True)
        raise

    except requests.exceptions.ConnectionError:
        logger.error(f"Connection error -> URL: {download_link}", exc_info=True)
        raise

    except requests.exceptions.HTTPError:
        logger.error(f"HTTP error -> URL: {download_link}", exc_info=True)
        raise

    except Exception:
        logger.error("Unexpected error during CSV download.", exc_info=True)
        raise

    logger.info(f"==== CSV saved to: {csvfile_path} ====")
    return [str(csvfile_path)]
