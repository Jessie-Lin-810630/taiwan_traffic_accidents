"""GCS 共用工具：Client 單例與 Parquet 物件的讀取、寫入與列舉。

暫時性故障的重試由 `google-cloud-storage` 自己完成，本模組**不再疊第二層**
（ADR-0006 提醒過疊層會讓重試上限變成 3×3）。以下是這件事的精確範圍 ——
寫在這裡是因為「以為有重試、其實沒有」與其反面同樣危險。

**由什麼工具重試**：`google.cloud.storage.retry.DEFAULT_RETRY`，
底層是 `google.api_core.retry.Retry`，判斷式為該模組的 `_should_retry()`。

**在什麼情境下重試**（`_RETRYABLE_TYPES` 與 `_RETRYABLE_STATUS_CODES`）：

- HTTP 狀態碼 **408、429、500、502、503、504** ——
  注意是這六個，不是「所有 5xx」；**501 不重試**
- 傳輸層：`ConnectionError`、`requests.ConnectionError`、
  `ChunkedEncodingError`、`requests.Timeout`
- 協定層：`http.client` 的 `BadStatusLine` / `IncompleteRead` / `ResponseNotReady`、
  `urllib3` 的 `PoolError` / `ProtocolError` / `SSLError` / `TimeoutError`

**退避參數**：首次等 1 秒、每次 ×2、單次上限 60 秒、總時限 120 秒。

**本模組三支函式所用的 API 皆為無條件 `DEFAULT_RETRY`**
（`Blob.download_as_bytes`、`Blob.upload_from_file`、`Client.list_blobs`，
已以 `inspect.signature` 核對 3.13.0 的實際預設值）。

**陷阱 —— 日後擴充本模組時務必留意**：函式庫另有 `ConditionalRetryPolicy`。
對「重試可能造成資料重複或其他副作用」的操作（例如 `Blob.delete()`、
metadata 異動），預設是 `DEFAULT_RETRY_IF_GENERATION_SPECIFIED` ——
**只有在請求帶了 `if_generation_match` / `generation` 時才會重試，否則一次都不重試**。
若要在此新增這類函式，必須自行決定是否傳入 precondition，
不能沿用「反正函式庫會重試」的假設。

能傳到呼叫端的 `GoogleAPIError`，都是上述重試耗盡後仍然失敗的，
因此一律原樣拋出、不再轉型別（ADR-0001）。

參考：
- https://cloud.google.com/storage/docs/retry-strategy#client-libraries
- https://cloud.google.com/python/docs/reference/storage/latest/retry_timeout
- https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.retry
"""

import io

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)

# 模組層 Client 單例（與 mysql_utils._ENGINES、redis_utils._REDIS_POOL 同形）：
# Client 內含一個 HTTP session 與認證憑證，憑證取得需往 metadata server 走一趟。
# 每次呼叫都新建會讓那趟往返在每個 batch 重複發生。
_CLIENT: storage.Client | None = None


def _get_client() -> storage.Client:
    """取得行程內共用的 GCS Client。

    以 Application Default Credentials 建立：VM 上取用附加的 service account
    （經 GCE metadata server），地端則取用 `gcloud auth application-default login`
    留下的憑證。因此本模組不讀任何 GCP 相關的環境變數。

    Returns:
        storage.Client: 行程內共用的 Client 實例。
    """
    global _CLIENT
    if _CLIENT is None:
        logger.info("==== Creating GCS Client ====")
        _CLIENT = storage.Client()
    return _CLIENT


def read_parquet(bucket: str, object_name: str) -> pd.DataFrame:
    """從 GCS 讀取單一 Parquet 物件並解析為 DataFrame。

    下載與解析綁在一起是刻意的：本專案存進 GCS 的一律是 Parquet，
    把 `BytesIO` 樣板留在呼叫端只會讓同一段程式碼複製多份
    （`redis_utils` 擁有 pickle 是同一個形狀）。

    Parameters:
        bucket (str): bucket 名稱。由呼叫端傳入，本模組不知道任何特定 bucket。
        object_name (str): bucket 內的完整物件路徑。

    Returns:
        pandas.DataFrame: 解析後的資料。

    Raises:
        GoogleAPIError: 下載失敗。`Blob.download_as_bytes()` 預設帶
            `DEFAULT_RETRY`，408/429/500/502/503/504 與傳輸層故障
            已在其內部退避重試過（詳見模組 docstring）；
            能傳到這裡的都是重試耗盡後仍然失敗的，原樣拋出。
    """
    try:
        blob = _get_client().bucket(bucket).blob(object_name)
        data = blob.download_as_bytes()
    except GoogleAPIError:
        logger.error(f"Failed to download from GCS: gs://{bucket}/{object_name}")
        raise

    return pd.read_parquet(io.BytesIO(data))


def write_parquet(
    bucket: str, object_name: str, df: pd.DataFrame, *, index: bool = False
) -> None:
    """將 DataFrame 序列化為 Parquet 並上傳至 GCS（同名物件會被覆蓋）。

    透過 `io.BytesIO` 在記憶體中完成序列化，不落地暫存檔，
    也避開 pandas 內部直接讀寫 GCS 時的套件版本相依。

    Parameters:
        bucket (str): bucket 名稱。
        object_name (str): bucket 內的完整物件路徑。
        df (pandas.DataFrame): 待寫入的資料。
        index (bool): 是否將 DataFrame 的 index 一併寫入，預設 `False`。
            需要保留 index 的呼叫端請顯式傳入 `True` —— 兩種需求都存在，
            預設值不該讓差異隱形。

    Raises:
        GoogleAPIError: 上傳失敗。`Blob.upload_from_file()` 在 3.13.0 的預設是
            **無條件** `DEFAULT_RETRY`（非 `DEFAULT_RETRY_IF_GENERATION_SPECIFIED`），
            因此暫時性故障已在其內部退避重試過（詳見模組 docstring）；
            能傳到這裡的都是重試耗盡後仍然失敗的，原樣拋出。

            本函式不傳 `if_generation_match`，因此同名物件一律被覆蓋 ——
            天氣 pipeline 需要的正是「重跑就覆蓋」。
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=index, engine="pyarrow")
    buffer.seek(0)

    try:
        blob = _get_client().bucket(bucket).blob(object_name)
        blob.upload_from_file(buffer, content_type="application/octet-stream")
    except GoogleAPIError:
        logger.error(f"Failed to upload to GCS: gs://{bucket}/{object_name}")
        raise


def list_parquet(bucket: str, prefix: str) -> list[str]:
    """列出指定前綴下的所有 Parquet 物件路徑。

    回傳空 list 代表該前綴下確實沒有 Parquet 物件 —— 那是一個真實的答案，
    不是故障。「沒有檔案算不算故障」是呼叫端的政策（ADR-0003）。

    Parameters:
        bucket (str): bucket 名稱。
        prefix (str): 物件路徑前綴。

    Returns:
        list[str]: 以 `.parquet` 結尾的物件完整路徑，不含其餘物件。

    Raises:
        GoogleAPIError: 列舉失敗。`Client.list_blobs()` 預設帶 `DEFAULT_RETRY`，
            暫時性故障已在其內部退避重試過（詳見模組 docstring）；
            能傳到這裡的都是重試耗盡後仍然失敗的，原樣拋出。
    """
    try:
        blobs = _get_client().list_blobs(bucket, prefix=prefix)
        return [blob.name for blob in blobs if blob.name.endswith(".parquet")]
    except GoogleAPIError:
        logger.error(f"Failed to list GCS objects: gs://{bucket}/{prefix}")
        raise
