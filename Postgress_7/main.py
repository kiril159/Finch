from fastapi import FastAPI, HTTPException, Depends
from tortoise.contrib.fastapi import register_tortoise
import models
from for_jwt import generate_token,  validate_token
from fastapi.security import HTTPBearer

app = FastAPI()

register_tortoise(
    app,
    db_url="postgres://postgres:100401@localhost:5432/user_roles",
    modules={"models": ["models"]},
    generate_schemas=True,
    add_exception_handlers=True,
)



@app.post('/user')
async def create_user(user_model: models.UserCreate):
    user = await models.UserModel.create(username=user_model.username,
                                         password=user_model.password,
                                         role=await models.RoleModel.get(id=user_model.role),
                                         )
    for region in user_model.region:
        await user.region.add(await models.RegionModel.get(id=region))
    return user_model


@app.post('/role')
async def create_role(role_model: models.RoleCreate):
    roles = await models.RoleModel.create(role=role_model.role)
    return roles


@app.post('/region')
async def create_region(region_model: models.RegionCreate):
    region = await models.RegionModel.create(region=region_model.region)
    return region


@app.get('/', dependencies=[Depends(validate_token)])
async def authorization_user(username, password):
    try:
        await models.UserModel.get(username=username, password=password)
        return "Authorization was successful"
    except:
        return "Incorrect user or password"


@app.post('/login')
async def login(request_data: models.LoginRequest):
    try:
        await models.UserModel.get(username=request_data.username, password=request_data.password)
        token = generate_token(request_data.username)
        return {
            'token': token
        }
    except:
        raise HTTPException(status_code=404, detail="User not found")


