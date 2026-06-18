from uuid import UUID

from sqlmodel import SQLModel

from core.db.models.user import UserBase
from core.utils.partial import optional

first_user_id = "f25ea71c-77c5-4842-b23d-ef7c2c29c1ca"


class UserRead(UserBase):
    id: UUID
    firstname: str
    lastname: str
    organization_id: UUID | None = None
    enabled: bool | None = None
    topt: bool | None = None
    roles: list[str] | None = []


class UserCreate(UserBase):
    id: UUID


@optional
class UserUpdate(UserBase):
    pass


@optional
class UserProfileUpdate(SQLModel):
    firstname: str
    lastname: str
    avatar: str
    email: str


request_examples = {
    "user": {
        "update": {
            "firstname": "Majk",
            "lastname": "Shkurti",
            "avatar": "https://plan4better.de",
            "email": "plan4better20242024@gmail.com",
        }
    }
}
