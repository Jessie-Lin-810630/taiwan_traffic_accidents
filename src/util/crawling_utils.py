"""爬蟲共用工具：蒐集下載連結、下載檔案與解壓縮。"""

import zipfile
from collections.abc import Callable
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# 暫時性故障的重試設定（ADR-0006）：這裡若重試耗盡，將再往上由 Airflow task-level 的 retries 接手。
RETRY_ATTEMPTS = 3
RETRY_WAIT = wait_exponential(multiplier=2, max=10)

TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def is_transient(exc: BaseException) -> bool:
    """判斷例外是否為值得重試的暫時性故障（ADR-0006）。

    公開給呼叫端組合用：有額外重試條件的模組（例如 Google API 在 HTTP 200
    的回應 body 裡回報失敗）可以把它併進自己的判斷式，而不必疊第二層裝飾器。
    """
    if isinstance(
        exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    ):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        response = exc.response
        return response is not None and response.status_code in TRANSIENT_STATUS_CODES
    return False


def _log_attempt_failure(message: str, exc: BaseException) -> None:
    """記錄一次請求失敗，級別依「是否還會被重試」而定（ADR-0006）。

    本模組的函式都在 `@retry_on_transient` 之下，暫時性故障的每一次失敗
    都可能自癒，用 ERROR 記錄會讓一次最終失敗在監控上放大成三筆告警。
    永久性故障不會重試，那一筆就是確定的失敗，維持 ERROR。

    最終失敗的完整 traceback 由呼叫端負責 —— DAG 路徑上由 Airflow 自動輸出
    （ADR-0003 子決策 5）。
    """
    if is_transient(exc):
        logger.warning(f"{message}（暫時性故障，將視情況重試）")
    else:
        logger.error(message)


def _log_retry(retry_state) -> None:
    """在每次重試前留下記錄，讓「重試過幾次」在 log 裡可追。"""
    logger.warning(
        f"第 {retry_state.attempt_number} 次嘗試失敗，"
        f"{retry_state.next_action.sleep:.0f} 秒後重試："
        f"{retry_state.outcome.exception()}"
    )


# 供 requests 呼叫端共用的重試裝飾器：只重試暫時性故障，永久性故障立即拋出。
retry_on_transient = retry(
    retry=retry_if_exception(is_transient),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=RETRY_WAIT,
    before_sleep=_log_retry,
    reraise=True,  # 重試耗盡時拋出原始例外，而非 tenacity 的 RetryError
)


@retry_on_transient
def fetch_soup(url: str, headers: dict, *, verify: bool = True) -> BeautifulSoup:
    """抓取單一頁面並解析為 BeautifulSoup；暫時性故障會自動重試。

    重試的單位刻意是**單一請求**而非整個 url 清單 ——
    第三個頁面 503 時不該讓前兩個已成功的頁面重抓。
    要走訪多個結構相似的頁面時，在呼叫端寫迴圈即可，每頁各自重試：

    ```python
    results = {}
    for url in urls:
        soup = fetch_soup(url, headers)
        results[url] = soup.select_one("你的選擇器")   # 各站專屬的解析邏輯
    ```

    解析規則因站而異，屬於呼叫端（`src/task/e_*.py`）的職責；
    本函式只負責「把一頁安全地抓回來」。

    Parameters:
        url (str): 要抓取的網頁 URL。
        headers (dict): 請求 headers。
        verify (bool): 是否驗證 SSL 憑證，預設 `True`。
            僅在來源站台的憑證鏈確實有問題時才由呼叫端傳入 `False`，
            並由該呼叫端自行記錄警告（ADR-0006 子決策 6）。

    Returns:
        BeautifulSoup: 解析後的頁面。

    Raises:
        requests.exceptions.HTTPError: 回應非 2xx；5xx 與 429 會先重試。
        requests.exceptions.Timeout | ConnectionError: 重試耗盡後原樣拋出。
    """
    try:
        logger.info(f"開始發送請求至: {url}")
        response = requests.get(url, headers=headers, verify=verify, timeout=30)

        # 確保 HTTP 狀態碼為 200，否則主動拋出 HTTPError 進入 except 區塊
        response.raise_for_status()
        logger.info(f"成功訪問 {url}，狀態碼: {response.status_code}")
        return BeautifulSoup(response.text, "html.parser")

    except requests.exceptions.Timeout as exc:
        _log_attempt_failure(f"請求超時 (Timeout) -> URL: {url}", exc)
        raise

    except requests.exceptions.ConnectionError as exc:
        _log_attempt_failure(f"連線失敗 (ConnectionError) -> URL: {url}", exc)
        raise

    except requests.exceptions.HTTPError as exc:  # 429 與 5xx 會被重試，其餘不會
        _log_attempt_failure(f"HTTP 回應異常 (HTTPError) -> URL: {url}", exc)
        raise

    except Exception:
        logger.error(f"未預期的錯誤 -> URL: {url}")
        raise


@retry_on_transient
def download_and_extract_zip(
    download_link: str,
    headers: dict,
    zipfile_save_dir: str | Path,
    zipfile_name: str,
    unzipfile_save_dir: str | Path,
    *,
    filename_filter: Callable[[str], bool] = lambda name: name.endswith(".csv"),
    verify: bool = True,
) -> list[str]:
    """Download a ZIP file from the given URL and extract its contents to the specified directory.

    Parameters:
        download_link (str): URL to download the zip file.
        headers (dict): Headers for HTTP request.
        zipfile_save_dir (str | Path): Path of directory to save the downloaded zip file.
        zipfile_name (str): File name for the zip file; should include the ``.zip`` extension as suffix.
        unzipfile_save_dir (str | Path): Path of directory to extract the zip contents into.
        filename_filter (Callable[[str], bool]): 決定壓縮檔內哪些檔案要取出；
            預設取出所有 ``.csv``。檔名的篩選規則因資料來源而異，屬呼叫端的職責。
        verify (bool): 是否驗證 SSL 憑證，預設 ``True``。

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
            download_link, headers=headers, stream=True, verify=verify, timeout=300
        )  # stream=True 為了搭配後方 response.iter_content() 使用
        response.raise_for_status()

        with open(zipfile_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    except requests.exceptions.Timeout as exc:
        _log_attempt_failure(f"Timeout -> URL: {download_link}", exc)
        raise

    except requests.exceptions.ConnectionError as exc:
        _log_attempt_failure(f"Connection error -> URL: {download_link}", exc)
        raise

    except requests.exceptions.HTTPError as exc:  # 429/5xx 會被重試，其餘不會
        _log_attempt_failure(f"HTTP error -> URL: {download_link}", exc)
        raise

    except Exception:
        # 非 requests 例外不會被重試，這一筆就是確定的失敗。
        logger.error("Unexpected error during zip download or save.")
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
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        filename = f.filename

                # 取出哪些檔案由呼叫端決定（各資料來源的命名規則不同）。
                if filename_filter(filename):
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
        logger.error("Unexpected error during zip extraction or CSV save.")
        raise

    logger.info(
        f"==== Extraction complete. {len(csvfile_pathlist)} CSV file(s) found. ===="
    )
    return csvfile_pathlist


@retry_on_transient
def download_csv(
    download_link: str,
    headers: dict,
    csvfile_name: str,
    csvfile_save_dir: str | Path,
    *,
    verify: bool = True,
) -> list[str]:
    """Download CSV file from a passed URL to a targeted directory.

    Parameters:
        download_link (str): URL to download csv file.
        headers (dict): Headers for HTTP request.
        csvfile_name (str): File name to save and it should include the file extension ``.csv`` as suffix.
        csvfile_save_dir (str | Path): Path of directory to save the csv file.
        verify (bool): 是否驗證 SSL 憑證，預設 ``True``。

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
            headers=headers,
            stream=True,  # 為了搭配後方 response.iter_content() 使用
            verify=verify,
            timeout=300,
        )
        response.raise_for_status()
        with open(csvfile_path, "wb") as f:
            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):  # 每次只讀 1 MB 到記憶體後寫入f
                if chunk:
                    f.write(chunk)

    except requests.exceptions.Timeout as exc:
        _log_attempt_failure(f"Timeout -> URL: {download_link}", exc)
        raise

    except requests.exceptions.ConnectionError as exc:
        _log_attempt_failure(f"Connection error -> URL: {download_link}", exc)
        raise

    except requests.exceptions.HTTPError as exc:  # 429/5xx 會被重試，其餘不會
        _log_attempt_failure(f"HTTP error -> URL: {download_link}", exc)
        raise

    except Exception:
        # 非 requests 例外不會被重試，這一筆就是確定的失敗。
        logger.error("Unexpected error during CSV download.")
        raise

    logger.info(f"==== CSV saved to: {csvfile_path} ====")
    return [str(csvfile_path)]
