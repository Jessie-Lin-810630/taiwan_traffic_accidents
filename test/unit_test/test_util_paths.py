"""驗證 ADR-0007：路徑以專案根為基準，不隨行程的工作目錄漂移。"""

import ast
import pathlib

from src.util import paths


def test_專案根含有_pyproject_toml():
    """PROJECT_ROOT 的 parents[2] 層數若算錯，這裡會第一個發現。"""
    assert (paths.PROJECT_ROOT / "pyproject.toml").is_file()
    assert (paths.PROJECT_ROOT / "src").is_dir()


def test_切換工作目錄不影響任何路徑(tmp_path, monkeypatch):
    """本輪的核心：從任何 CWD 執行，落點都相同。"""
    import importlib

    before = (
        paths.PROJECT_ROOT,
        paths.RAW_DATA_DIR,
        paths.PROCESSED_DATA_DIR,
        paths.MART_SQL_DIR,
    )

    monkeypatch.chdir(tmp_path)
    reloaded = importlib.reload(paths)
    after = (
        reloaded.PROJECT_ROOT,
        reloaded.RAW_DATA_DIR,
        reloaded.PROCESSED_DATA_DIR,
        reloaded.MART_SQL_DIR,
    )

    assert before == after


def test_資料落點位於專案根之下():
    """落點與程式碼分離，但仍在專案根之下（子決策 2：不引入環境變數）。"""
    assert paths.RAW_DATA_DIR == paths.PROJECT_ROOT / "data" / "raw"
    assert paths.PROCESSED_DATA_DIR == paths.PROJECT_ROOT / "data" / "processed"


def test_mart_sql_目錄指向實際存在的_sql_檔():
    """程式碼資產以 __file__ 推導，必須真的指得到那些 SQL。"""
    assert paths.MART_SQL_DIR.is_dir()

    sql_files = list(paths.MART_SQL_DIR.glob("*.sql"))
    assert len(sql_files) >= 6, f"只找到 {len(sql_files)} 個 .sql"


def test_匯入時不建立任何目錄():
    """Streamlit 容器只 COPY ./src，載入本模組不該憑空建出 data/（子決策 7）。"""
    source = pathlib.Path(paths.__file__).read_text(encoding="utf-8")

    calls = [
        node.func.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]
    assert "mkdir" not in calls


def test_全_repo_不再以_cwd_為路徑基準():
    """`Path().resolve()` 取得的是 CWD 而非專案根 —— 本輪要消滅的寫法。"""
    offenders = []
    for directory in ("src", "dags"):
        for path in (paths.PROJECT_ROOT / directory).rglob("*.py"):
            if "__pycache__" in str(path):
                continue
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                # 比對 Path().resolve() —— 無引數的 Path() 後接 .resolve()
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "resolve"
                    and isinstance(node.func.value, ast.Call)
                    and isinstance(node.func.value.func, ast.Name)
                    and node.func.value.func.id == "Path"
                    and not node.func.value.args
                ):
                    offenders.append(f"{path}:{node.lineno}")

    assert offenders == []
