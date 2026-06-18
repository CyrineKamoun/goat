from typing import Any

from fastapi import APIRouter, Depends

from core.deps.auth import user_token
from core.schemas.plan import PlansList, get_plans_data

router = APIRouter()


@router.get(
    "/plans",
    summary="Get Plans List",
    response_model=PlansList,
)
async def get_plans(
    *,
    user_token: dict = Depends(user_token),
) -> Any:
    """
    Get Plans List
    """

    plans_data = get_plans_data()
    plans_list = PlansList(plans=plans_data)
    return plans_list
