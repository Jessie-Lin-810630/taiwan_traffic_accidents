"""GCS 的唯一存取面：Client 單例與 Parquet 物件的讀取、寫入與列舉。

三支公開函式只處理 Parquet，因為專案存進 GCS 的一律是 Parquet
（dag d07/d08 天氣 ETL 的中繼檔）。
bucket 名稱一律由呼叫端傳入，本模組的函式不預設任何 bucket。GCS 認證走
Application Default Credentials，因此不讀任何 GCP 相關的環境變數。

暫時性故障的重試交予 `google-cloud-storage` 內建的 `DEFAULT_RETRY` 負責，
本模組不另外使用 tenacitiy 等重試工具多疊一層，否則會讓重試上限變成兩者相乘。

重試行為範圍是：

- 會重試的情況：HTTP 408、429、500、502、503、504（注意不是所有 5xx，
  501 不重試）、傳輸層的連線與逾時錯誤、`http.client` 與 `urllib3` 的協定層錯誤。
- 退避參數：首次等 1 秒、每次乘以 2、單次上限 60 秒、總時限 120 秒。
- 本模組用到的三個 API（`Blob.download_as_bytes`、`Blob.upload_from_file`、
  `Client.list_blobs`）預設都是無條件重試。

日後擴充要留意：函式庫對「重試可能造成副作用」的操作（例如 `Blob.delete()`、
metadata 異動）預設改用 `DEFAULT_RETRY_IF_GENERATION_SPECIFIED` —— 只有請求帶了
`if_generation_match` 或 `generation` 時才重試，否則一次都不重試。新增這類函式
必須自行決定是否傳入 precondition，不能沿用「函式庫會重試」的假設。

傳到呼叫端的 `GoogleAPIError` 都是重試耗盡後仍然失敗的，一律原樣拋出、不轉型別。

Notes:
    重試不疊層參考 ADR-0006，例外不轉型別參考 ADR-0001。

References:
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

# 模組層 Client 單例：
_CLIENT: storage.Client | None = None


def _get_client() -> storage.Client:
    """取得行程內共用的 GCS Client。

    以 Application Default Credentials 建立，憑證取自：

    - 如在 VM 上：取用附加的 service account （經 GCE metadata server）。
    - 地端：取用 `gcloud auth application-default login`
    本函式預設不額外讀取任何 GCP 相關的環境變數。

    Returns:
        storage.Client: 行程內共用的 Client 實例。
    """
    global _CLIENT
    if _CLIENT is None:
        logger.info("==== Creating GCS Client ====")
        _CLIENT = storage.Client()
    return _CLIENT


def read_parquet(bucket: str, object_name: str) -> pd.DataFrame:
    """從 GCS 下載單一 Parquet 物件並解析成 DataFrame。

    下載與解析綁在一起，呼叫端不必自行處理 `BytesIO`。

    Args:
        bucket (str): bucket 名稱。
        object_name (str): bucket 內的 Parquet 物件之完整路徑。

    Returns:
        pandas.DataFrame: 解析後的資料，欄位與當初寫入時相同。
            以本專案天氣中繼檔為例，回傳的 DataFrame 形如：

                latitude  longitude  time                 temperature_2m  precipitation
                25.05     121.55     2024-01-01 00:00:00  16.4            0.0
                25.05     121.55     2024-01-01 01:00:00  16.1            0.2

    Raises:
        GoogleAPIError: 下載失敗且內建重試已耗盡（物件不存在也屬此類），原樣拋出。
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
    """把 DataFrame 序列化成 Parquet 並上傳到 GCS，同名物件會被覆蓋。

    序列化在記憶體中以 `io.BytesIO` 完成，不落地暫存檔。函式不帶
    `if_generation_match`，因此同名物件一律直接覆蓋，此行為屬本專案刻意。

    Args:
        bucket (str): bucket 名稱。
        object_name (str): bucket 內的 Parquet 物件之完整路徑。
        df (pandas.DataFrame): 待寫入的資料。
        index (bool): 是否把 DataFrame 的 index 一併寫入，預設 `False`；
            需要保留 index 的呼叫端請顯式傳入 `True`。

    Raises:
        GoogleAPIError: 上傳失敗且內建重試已耗盡，原樣拋出。
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
    """列出指定前綴底下所有 Parquet 物件的完整路徑。

    回傳空 list 代表該前綴下確實沒有 Parquet 物件，那是一個真實的答案而非故障；
    「沒有檔案算不算故障」由呼叫端自行決定。

    Args:
        bucket (str): bucket 名稱。
        prefix (str): 物件路徑前綴。

    Returns:
        list[str]: 以 `.parquet` 結尾的物件完整路徑，其餘物件不列入，形如：

            [
                "weather_cache_final/2024/data/2024-01/25.05_121.55.parquet",
                "weather_cache_final/2024/data/2024-01/24.15_120.65.parquet",
            ]

    Raises:
        GoogleAPIError: 列舉失敗且內建重試已耗盡，原樣拋出。

    Notes:
        空結果不視為故障，參考 ADR-0003。
    """
    try:
        blobs = _get_client().list_blobs(bucket, prefix=prefix)
        return [blob.name for blob in blobs if blob.name.endswith(".parquet")]
    except GoogleAPIError:
        logger.error(f"Failed to list GCS objects: gs://{bucket}/{prefix}")
        raise
