from uuid import uuid4

import pytest
from core.schemas.user import UserRead


@pytest.mark.unit
def test_user_read_accepts_null_avatar() -> None:
    user = UserRead(
        id=uuid4(),
        email="user@example.com",
        firstname="Test",
        lastname="User",
        avatar=None,
    )
    assert user.avatar is None


@pytest.mark.unit
def test_user_read_keeps_avatar_value() -> None:
    user = UserRead(
        id=uuid4(),
        email="user@example.com",
        firstname="Test",
        lastname="User",
        avatar="https://assets.example.com/avatar.png",
    )
    assert user.avatar == "https://assets.example.com/avatar.png"
