from tortoise.models import Model
from tortoise import fields
from pydantic import BaseModel


class UserModel(Model):
    id = fields.IntField(pk=True)
    username = fields.CharField(max_length=50, unique=True)
    password = fields.CharField(max_length=50)
    role = fields.ForeignKeyField("models.RoleModel", related_name="user")
    region = fields.ManyToManyField("models.RegionModel", related_name="user", through="user_region")

    class Meta:
        table = "users"


class RoleModel(Model):
    id = fields.IntField(pk=True)
    role = fields.CharField(max_length=50, unique=True)

    class Meta:
        table = "roles"


class RegionModel(Model):
    id = fields.IntField(pk=True)
    region = fields.CharField(max_length=50, unique=True)

    class Meta:
        table = "regions"


class UserCreate(BaseModel):
    username: str
    password: str
    role: int
    region: list = [0]


class RoleCreate(BaseModel):
    role: str


class RegionCreate(BaseModel):
    region: str


class LoginRequest(BaseModel):
    username: str
    password: str
