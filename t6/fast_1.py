from pydantic import BaseModel


class Fast_post(BaseModel):
    index: str
    id: str
    body: dict


