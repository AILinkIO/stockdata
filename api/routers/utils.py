"""工具：代码标准化。"""

from fastapi import APIRouter

from core.helpers import normalize_index_code_logic, normalize_stock_code_logic

router = APIRouter(prefix="/api/v1/utils", tags=["utils"])


@router.get("/normalize-code")
def normalize_code(code: str):
    return {"input": code, "normalized": normalize_stock_code_logic(code)}


@router.get("/normalize-index-code")
def normalize_index_code(code: str):
    return {"input": code, "normalized": normalize_index_code_logic(code)}
