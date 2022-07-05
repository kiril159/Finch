from fastapi import FastAPI, HTTPException, Depends
from tortoise.contrib.fastapi import register_tortoise
#from fastapi.security import HTTPBearer
from tortoise import Model
from tortoise import fields
from pydantic import BaseModel
import jwt
from fastapi import Depends, HTTPException
from pydantic import ValidationError
from datetime import datetime, timedelta
from typing import Union, Any
from fastapi.security import HTTPBearer

SECURITY_ALGORITHM = 'HS256'
SECRET_KEY = '123456'

reusable_oauth2 = HTTPBearer(
    scheme_name='Authorization'
)


def validate_token(http_authorization_credentials=Depends(reusable_oauth2)) -> str:
    """
    Decode JWT token to get username => return username
    """
    try:
        payload = jwt.decode(http_authorization_credentials.credentials, SECRET_KEY, algorithms=[SECURITY_ALGORITHM])
        return payload.get('username')
    except(jwt.PyJWTError, ValidationError):
        raise HTTPException(
            status_code=403,
            detail=f"Could not validate token",
        )


def generate_token(username: Union[str, Any]) -> str:
    expire = datetime.utcnow() + timedelta(
        seconds=60 * 60 * 24 * 3  # Expired after 3 days
    )
    to_encode = {
        "exp": expire, "username": username
    }
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=SECURITY_ALGORITHM)
    return encoded_jwt


class UserModel(Model):
    id = fields.IntField(pk=True)
    username = fields.CharField(max_length=50, unique=True)
    password = fields.CharField(max_length=50)
    role = fields.ForeignKeyField("RoleModel", related_name="user")
    region = fields.ManyToManyField("RegionModel", related_name="user", through="user_region")

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
app = FastAPI()

register_tortoise(
    app,
    db_url="postgres://postgres:100401@localhost:5432/user_roles",
    #modules={"models": ["models"]},
    generate_schemas=True,
    add_exception_handlers=True,
)



@app.post('/user')
async def create_user(user_model: UserCreate):
    user = await UserModel.create(username=user_model.username,
                                         password=user_model.password,
                                         role=await RoleModel.get(id=user_model.role),
                                         )
    for region in user_model.region:
        await user.region.add(await RegionModel.get(id=region))
    return user_model


@app.post('/role')
async def create_role(role_model: RoleCreate):
    roles = await RoleModel.create(role=role_model.role)
    return roles


@app.post('/region')
async def create_region(region_model: RegionCreate):
    region = await RegionModel.create(region=region_model.region)
    return region


@app.get('/', dependencies=[Depends(validate_token)])
async def authorization_user(username, password):
    try:
        await UserModel.get(username=username, password=password)
        return "Authorization was successful"
    except:
        return "Incorrect user or password"


@app.post('/login')
async def login(request_data: LoginRequest):
    try:
        await UserModel.get(username=request_data.username, password=request_data.password)
        token = generate_token(request_data.username)
        return {
            'token': token
        }
    except:
        raise HTTPException(status_code=404, detail="User not found")