from tortoise.models import Model
from tortoise import fields
from pydantic import BaseModel


class UserModel(Model):
    id = fields.IntField(pk=True)
    username = fields.CharField(max_length=50, unique=True)
    password = fields.CharField(max_length=50)

    class Meta:
        table = "users"


class UserCreate(BaseModel):
    username: str
    password: str
