"""抓取外部資料的共用能力：重試策略、頁面抓取與檔案下載。

本模組只放通用於「各式外部資料來源」的程式邏輯，各 ETL 任務專屬的解析規則、選擇器與
檔名規則屬於呼叫端（`src/task/e_*.py`）的職責。

三支對外函式（`fetch_soup`、`download_and_extract_zip`、`download_csv`）都套上
使用 tenacity 做出的 `retry_on_transient` 裝飾器，
`retry_on_transient` 處理暫時性故障的重試行為，
包含: 連線失敗、逾時、HTTP 429 與 5xx，最多重試 3 次並採指數退避。
重試耗盡後拋出的是原始例外而非 tenacity 的 `RetryError`，
再往上由 Airflow task 層級的 retries 接手。

永久性故障（例如 404）立即拋出不重試。

Notes:
    重試策略與日誌級別的取捨參考 ADR-0006。
"""

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

# 暫時性故障的重試設定：這裡若重試耗盡，將由 Airflow task-level 的 retries 接手。
RETRY_ATTEMPTS = 3
RETRY_WAIT = wait_exponential(multiplier=2, max=10)
TRANSIENT_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def is_transient(exc: BaseException) -> bool:
    """判斷例外是否為值得重試的暫時性故障。

    認定為暫時性的有三類：逾時 (`requests.exceptions.Timeout`)、
    連線失敗 (`requests.exceptions.ConnectionError`)，
    以及狀態碼落在 429、500、502、503、504 的 HTTPError。
    其餘（例如 404、403）視為永久性故障，不值得重試。

    本函式公開給呼叫端組合用：有額外重試條件的模組（例如某些 API 會在 HTTP 200
    的回應內容裡回報失敗）可以把它併進自己的判斷式，而不必疊第二層重試裝飾器。

    Args:
        exc (BaseException): 要判斷的例外。

    Returns:
        bool: 屬於暫時性故障為 `True`，否則為 `False`。

    Notes:
        參考 ADR-0006。
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
    """記錄一次請求失敗，日誌級別會依「是否屬於有重試機會的暫時性故障」而定。

    暫時性故障記 warning，因為它每一次失敗都可能在下一次重試後成功，用 error
    會讓一次最終失敗在監控上放大成三筆告警，除錯時鑒別度變差；永久性故障不會重試，
    ，直接記 error。

    此函式設計給 try-except 的 except 區塊做彈性設計日誌記錄，
    但是完整的 traceback **必須**由呼叫端負責輸出。

    Args:
        message (str): 要寫進日誌的訊息，通常含 URL。
        exc (BaseException): 這次失敗的例外，用來判斷級別。

    Notes:
        參考 ADR-0006。
    """
    if is_transient(exc):
        logger.warning(f"{message}（暫時性故障，將視情況重試）")
    else:
        logger.error(message)


def _log_retry(retry_state) -> None:
    """確定重試後，在每次重試前留下一筆 warning，記載「重試過幾次」在日誌裡可追。

    此函式設計要傳給 tenacity 的 `before_sleep` 參數。

    Args:
        retry_state: tenacity 傳入的重試狀態，含嘗試次數、下次等待秒數與本次例外。
    """
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
    """抓取單一頁面並解析成 BeautifulSoup，暫時性故障會自動重試。

    重試機制仰賴 `@retry_on_transient` 設定，重試次數的單位是單一 URL 請求而非整份 URL 清單，
    若要走訪多個結構相似的頁面時，在呼叫端寫迴圈即可，每頁各自重試：

    ```python
    results = {}
    for url in urls:
        soup = fetch_soup(url, headers)
        results[url] = soup.select_one("你的selector")   # 各站專屬的解析邏輯
    ```

    Args:
        url (str): 要抓取的網頁 URL。
        headers (dict): 請求標頭。
        verify (bool): 是否驗證 SSL 憑證，預設 `True`；只有來源站台憑證鏈確實
            有問題時才傳入 `False`，並由該呼叫端自行記錄警告。

    Returns:
        BeautifulSoup: 以 `html.parser` 解析後的頁面。

    Raises:
        requests.exceptions.HTTPError: 429、500、502、503、504 會先重試，重試耗盡後拋出。
        requests.exceptions.Timeout: 請求逾時且重試耗盡。
        requests.exceptions.ConnectionError: 連線失敗且重試耗盡。
        Exception: 其他非預期錯誤，記下 error 後原樣拋出、不重試。

    Notes:
        `verify` 的使用時機參考 ADR-0006。
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
    """下載 ZIP 檔並把符合條件的內容解壓到指定目錄，重試機制仰賴 `@retry_on_transient` 設定。

    下載以串流方式每次寫入 1 MB，因此大檔不會整份讀進記憶體。解壓時會修復
    壓縮檔內常見的中文檔名亂碼（依序嘗試 cp437 轉 utf-8、cp437 轉 cp950，
    都失敗則沿用原檔名），再依 `filename_filter` 決定要取出哪些檔案。

    Args:
        download_link (str): ZIP 檔的下載網址。
        headers (dict): 請求標頭。
        zipfile_save_dir (str | Path): 存放下載回來的 ZIP 檔的目錄，不存在會自動建立。
        zipfile_name (str): ZIP 檔的檔名，未帶 `.zip` 副檔名時會自動補上。
        unzipfile_save_dir (str | Path): 解壓目的地目錄，不存在會自動建立。
        filename_filter (Callable[[str], bool]): 決定壓縮檔內哪些檔案要取出，
            預設取出所有 `.csv`。命名規則因資料來源而異，屬呼叫端的職責。
        verify (bool): 是否驗證 SSL 憑證，預設 `True`。

    Returns:
        list[str]: 實際取出的檔案完整路徑，形如：

            [
                "data/raw/113年A1交通事故資料.csv",
                "data/raw/113年A2交通事故資料.csv",
            ]

    Raises:
        requests.exceptions.HTTPError: 429、500、502、503、504 會重試，重試耗盡後拋出。
        requests.exceptions.Timeout: 下載逾時且重試耗盡。
        requests.exceptions.ConnectionError: 連線失敗且重試耗盡。
        zipfile.BadZipFile: 下載回來的檔案不是有效的 ZIP。
        Exception: 下載或解壓過程中的其他錯誤，記下 error 後原樣拋出、不重試。
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

    except requests.exceptions.HTTPError as exc:
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
    """下載單一 CSV 檔並存到指定目錄，重試機制仰賴 `@retry_on_transient` 設定。

    下載以串流方式每次寫入 1 MB，因此大檔不會整份讀進記憶體。回傳型別是
    list 而非單一路徑，為的是與 `download_and_extract_zip()` 一致，讓下游的
    `t_*` 函式都能接同一種輸入型別。

    Args:
        download_link (str): CSV 檔的下載網址。
        headers (dict): 請求標頭。
        csvfile_name (str): 存檔用的檔名，未帶 `.csv` 副檔名時會自動補上。
        csvfile_save_dir (str | Path): 存檔目錄，不存在會自動建立。
        verify (bool): 是否驗證 SSL 憑證，預設 `True`。

    Returns:
        list[str]: 只含一個元素的路徑清單，形如：

            ["data/raw/night_markets.csv"]

    Raises:
        requests.exceptions.HTTPError: 429、500、502、503、504 會先重試，重試耗盡後拋出。
        requests.exceptions.Timeout: 下載逾時且重試耗盡。
        requests.exceptions.ConnectionError: 連線失敗且重試耗盡。
        Exception: 其他非預期錯誤，記下 error 後原樣拋出、不重試。
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

    except requests.exceptions.HTTPError as exc:
        _log_attempt_failure(f"HTTP error -> URL: {download_link}", exc)
        raise

    except Exception:
        # 非 requests 例外不會被重試，這一筆就是確定的失敗。
        logger.error("Unexpected error during CSV download.")
        raise

    logger.info(f"==== CSV saved to: {csvfile_path} ====")
    return [str(csvfile_path)]
