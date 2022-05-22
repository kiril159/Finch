from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise
import models

app = FastAPI()

register_tortoise(
    app,
    db_url="postgres://test7:test7@localhost:5432/num1",
    modules={"models": ["models"]},
    generate_schemas=True,
    add_exception_handlers=True,
)


@app.post('/')
async def create_user(user_model: models.UserCreate):
    user = await models.UserModel.create(username=user_model.username, password=user_model.password)
    return user


@app.get('/')
async def authorization_user(username, password):
    try:
        await models.UserModel.get(username=username, password=password)
        return "Authorization was successful"
    except:
        return "Incorrect username or password"



