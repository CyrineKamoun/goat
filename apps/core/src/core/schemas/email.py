from pydantic import BaseModel


class EmailTemplateContent(BaseModel):
    artwork_url: str
    title: str
    message: str
    action_label: str | None = None
    action_url: str | None = None
