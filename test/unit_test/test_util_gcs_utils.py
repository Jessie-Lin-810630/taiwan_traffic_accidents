"""驗證 GCS 工具的序列化契約、Client 單例與失敗語意（ADR-0011 候選 5）。

一律以假的 Client 取代真實 GCS：這些測試釘的是 gcs_utils 自己的行為，
不是 google-cloud-storage 的行為，打真 bucket 只會讓測試變慢又不可重現。
"""

import io

import pandas as pd
import pytest
from google.api_core.exceptions import ServiceUnavailable

from src.util import gcs_utils


class FakeBlob:
    """記錄上傳內容、回放下載內容的假 blob。"""

    def __init__(self, name: str, store: dict):
        """以物件名與共用的後端 dict 建立假 blob。"""
        self.name = name
        self._store = store

    def upload_from_file(self, buffer, content_type=None):
        """把 buffer 的內容原樣收進後端 dict。"""
        self._store[self.name] = buffer.read()

    def download_as_bytes(self) -> bytes:
        """回放先前寫入的 bytes。"""
        return self._store[self.name]


class FakeBucket:
    """只負責產生 FakeBlob 的假 bucket。"""

    def __init__(self, store: dict):
        """持有共用的後端 dict。"""
        self._store = store

    def blob(self, name: str) -> FakeBlob:
        """取得指向 name 的假 blob。"""
        return FakeBlob(name, self._store)


class FakeClient:
    """以 dict 當作 bucket 內容的假 Client。"""

    instances = 0

    def __init__(self, store: dict | None = None):
        """建立假 Client，並累計建立次數以驗證單例行為。"""
        FakeClient.instances += 1
        self._store = store if store is not None else {}

    def bucket(self, name: str) -> FakeBucket:
        """取得假 bucket；測試中不區分 bucket 名稱。"""
        return FakeBucket(self._store)

    def list_blobs(self, bucket: str, prefix: str = ""):
        """回傳後端 dict 中符合前綴的假 blob，順序固定以利斷言。"""
        return [
            FakeBlob(name, self._store)
            for name in sorted(self._store)
            if name.startswith(prefix)
        ]


@pytest.fixture
def fake_gcs(monkeypatch):
    """把 gcs_utils 的 Client 換成假的，並回傳其後端 dict。"""
    store: dict[str, bytes] = {}
    client = FakeClient(store)
    monkeypatch.setattr(gcs_utils, "_CLIENT", client)
    return store


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {"lat_round": [24.15, 25.03], "lon_round": [120.68, 121.56], "batch_id": [0, 0]}
    )


def test_write_後_read_可取回相同內容(fake_gcs):
    """寫入與讀取是同一個序列化契約的兩端，往返後資料必須逐格相同。"""
    df = _df()

    gcs_utils.write_parquet("bkt", "a/b.parquet", df)
    got = gcs_utils.read_parquet("bkt", "a/b.parquet")

    pd.testing.assert_frame_equal(got, df)


def test_index_預設不寫入(fake_gcs):
    """預設 index=False：讀回來的 index 是重新編號的 RangeIndex，非原 index。"""
    df = _df()
    df.index = [10, 11]

    gcs_utils.write_parquet("bkt", "a/b.parquet", df)
    got = gcs_utils.read_parquet("bkt", "a/b.parquet")

    assert list(got.index) == [0, 1]


def test_index_顯式傳入時保留原索引(fake_gcs):
    """prep_batch_plan 的暫存檔依賴 index=True，這個區別不得消失。"""
    df = _df()
    df.index = [10, 11]

    gcs_utils.write_parquet("bkt", "a/b.parquet", df, index=True)
    got = gcs_utils.read_parquet("bkt", "a/b.parquet")

    assert list(got.index) == [10, 11]


def test_list_parquet_只回傳_parquet_物件(fake_gcs):
    """列舉要濾掉非 Parquet 物件，否則下游 read_parquet 會拿到解析不了的內容。"""
    fake_gcs["d/1.parquet"] = b""
    fake_gcs["d/2.parquet"] = b""
    fake_gcs["d/_SUCCESS"] = b""
    fake_gcs["d/notes.txt"] = b""

    got = gcs_utils.list_parquet("bkt", "d/")

    assert got == ["d/1.parquet", "d/2.parquet"]


def test_list_parquet_受前綴限制(fake_gcs):
    """前綴之外的物件不得混入。"""
    fake_gcs["2025/data/a.parquet"] = b""
    fake_gcs["2026/data/b.parquet"] = b""

    assert gcs_utils.list_parquet("bkt", "2026/data") == ["2026/data/b.parquet"]


def test_list_parquet_空結果不是故障(fake_gcs):
    """前綴下真的沒有物件時回傳空 list —— 那是真實答案，由呼叫端決定是否算故障（ADR-0003）。"""
    assert gcs_utils.list_parquet("bkt", "nothing/here") == []


def test_client_在行程內只建立一次(monkeypatch):
    """Client 攜帶憑證與 HTTP session，每次新建會讓取憑證的往返重複發生（同 ADR-0004）。"""
    monkeypatch.setattr(gcs_utils, "_CLIENT", None)
    monkeypatch.setattr(gcs_utils.storage, "Client", FakeClient)
    FakeClient.instances = 0

    first = gcs_utils._get_client()
    second = gcs_utils._get_client()

    assert first is second
    assert FakeClient.instances == 1


def test_下載失敗原樣拋出不吞成空(monkeypatch):
    """故障不得被吞成「正常但空」，否則上游監控失效（ADR-0003）。"""

    class ExplodingClient(FakeClient):
        def bucket(self, name):
            raise ServiceUnavailable("GCS 503")

    monkeypatch.setattr(gcs_utils, "_CLIENT", ExplodingClient())

    with pytest.raises(ServiceUnavailable):
        gcs_utils.read_parquet("bkt", "a/b.parquet")


def test_上傳失敗原樣拋出(monkeypatch):
    """上傳失敗同樣原樣拋出，讓 Airflow 自行輸出完整 traceback（ADR-0001）。"""

    class ExplodingClient(FakeClient):
        def bucket(self, name):
            raise ServiceUnavailable("GCS 503")

    monkeypatch.setattr(gcs_utils, "_CLIENT", ExplodingClient())

    with pytest.raises(ServiceUnavailable):
        gcs_utils.write_parquet("bkt", "a/b.parquet", _df())


def test_寫入的內容確實是_parquet(fake_gcs):
    """契約是 Parquet，不是任意二進位 —— 換掉 engine 或格式會在這裡被抓到。"""
    gcs_utils.write_parquet("bkt", "a/b.parquet", _df())

    raw = fake_gcs["a/b.parquet"]

    assert raw[:4] == b"PAR1"  # Parquet 檔案魔術數字
    pd.testing.assert_frame_equal(pd.read_parquet(io.BytesIO(raw)), _df())
