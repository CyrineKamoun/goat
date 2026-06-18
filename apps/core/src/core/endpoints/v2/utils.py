from typing import Any

from fastapi import APIRouter, Depends
from pydantic.networks import EmailStr

from core.deps.auth import is_superuser
from core.schemas.common import Msg
from core.utils.email import send_test_email

router = APIRouter()


@router.post(
    "/test-email",
    summary="Test email",
    response_model=Msg,
    dependencies=[Depends(is_superuser)],
)
def test_email(
    *,
    email_to: EmailStr,
) -> Any:
    """
    Test emails.
    """
    send_test_email(email_to=email_to)
    return {"msg": "Test email sent"}
