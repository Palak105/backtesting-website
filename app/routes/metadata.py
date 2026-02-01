from fastapi import APIRouter
from app.utils.db_schema import get_indicator_columns

router = APIRouter(prefix="/metadata")

@router.get("/indicators")
def indicators():
    return {
        "indicators": get_indicator_columns()
    }
