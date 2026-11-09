"""Revenue-related routes."""
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, field_validator

from api.lib.config import settings
from api.lib.db import execute_query
from api.lib.middleware import limiter
from api.lib.cache import cached

router = APIRouter(prefix="/api/v1", tags=["revenue"])


class RevenueSummaryResponse(BaseModel):
    """Daily revenue summary response model."""
    revenue_usd_30d: float = Field(
        description="Total revenue in USD for the last 30 days",
        ge=0
    )
    yield_usd_7d: float = Field(
        description="Total yield in USD for the last 7 days",
        ge=0
    )
    bifi_buyback_usd_7d: float = Field(
        description="Total BIFI buyback in USD for the last 7 days",
        ge=0
    )
    

@router.get(
    "/revenue/summary",
    response_model=RevenueSummaryResponse,
    summary="Get daily revenue summary",
    description="Returns daily aggregation of revenue, yield, and BIFI buyback metrics for the last 30 days",
)
@limiter.limit(f"{settings.RATE_LIMIT_PER_MINUTE}/minute")
@cached()
async def get_revenue_summary(request: Request):
    rows = execute_query("SELECT * FROM api.revenue_summary")
    if not rows or not rows[0]:
        return RevenueSummaryResponse(
            revenue_usd_30d=0.0,
            yield_usd_7d=0.0,
            bifi_buyback_usd_7d=0.0,
        )
    
    # Pydantic will automatically convert Decimal to float via the field validator
    return RevenueSummaryResponse(**rows[0])

