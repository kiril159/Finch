from fastapi import FastAPI, HTTPException, Depends
from tortoise.contrib.fastapi import register_tortoise
import models
from for_jwt import generate_token, validate_token

app = FastAPI()

register_tortoise(
    app,
    db_url="postgres://postgres:100401@db:5432/user_roles",
    modules={"models": ["models"]},
    generate_schemas=True,
    add_exception_handlers=True,
)


@app.post('/user')
def create_user(user_model: models.UserCreate):
    user = models.UserModel.create(username=user_model.username,
                                         password=user_model.password,
                                         role=models.RoleModel.get(id=user_model.role),
                                         )
    for region in user_model.region:
        user.region.add(models.RegionModel.get(id=region))
    return user_model


@app.post('/role')
def create_role(role_model: models.RoleCreate):
    roles = models.RoleModel.create(role=role_model.role)
    return roles


@app.post('/region')
def create_region(region_model: models.RegionCreate):
    region = models.RegionModel.create(region=region_model.region)
    return region


@app.get('/', dependencies=[Depends(validate_token)])
def authorization_user(username, password):
    try:
        models.UserModel.get(username=username, password=password)
        return "Authorization was successful"
    except:
        return "Incorrect user or password"


@app.post('/login')
def login(request_data: models.LoginRequest):
    try:
        models.UserModel.get(username=request_data.username, password=request_data.password)
        token = generate_token(request_data.username)
        return {
            'token': token
        }
    except:
        raise HTTPException(status_code=404, detail="User not found")
