from user import User
from user import Input, Input_file
from fastapi import FastAPI, HTTPException
import gitlab
import os

app = FastAPI(
    title='Gitlab',
    docs_url='/datastore-api/docs',
    openapi_url='/datastore-api',
)


@app.post('/datastore-api/add/database')
async def add_database(inp: Input):
    try:
        gl = gitlab.Gitlab(private_token=os.getenv('TOKEN'))
        gl.auth()
        u = User(gl, inp.email, inp.name, inp.project, inp.branch)
        u.created_branch(u.branch)
    except gitlab.exceptions.GitlabGetError:
        raise HTTPException(404, 'Проект не найден')
    except gitlab.exceptions.GitlabCreateError:
        raise HTTPException(404, f'База данных {inp.branch} уже есть')
    return f'База данных {u.branch} создана'


@app.post('/datastore-api/add/table')
async def add_table(inp: Input_file):
    try:
        gl = gitlab.Gitlab(private_token=os.getenv('TOKEN'))
        gl.auth()
        u = User(gl, inp.email, inp.name, inp.project, inp.branch)
        u.uploaded_file(inp.file_path, u.branch, inp.file_with_content, u.email, u.name, inp.commit)
    except gitlab.exceptions.GitlabGetError:
        raise HTTPException(404, 'Проект не найден')
    except gitlab.exceptions.GitlabCreateError:
        raise HTTPException(404, f' Файл {inp.file_path} уже есть')
    except FileNotFoundError:
        raise HTTPException(404, f'Таблицы {inp.file_with_content} нет')
    return f'Таблица {inp.file_path} создана'


@app.post('/datastore-api/edit/table')
async def edit_table(inp: Input_file):
    try:
        gl = gitlab.Gitlab(private_token=os.getenv('TOKEN'))
        gl.auth()
        u = User(gl, inp.email, inp.name, inp.project, inp.branch)
        content = open(inp.file_with_content).read()
        u.update_file(inp.file_path, u.branch, content, inp.commit)
    except gitlab.exceptions.GitlabGetError:
        raise HTTPException(404, 'Проект не найден')
    except FileNotFoundError:
        raise HTTPException(404, f'Таблицы {inp.file_with_content} нет')

    return f'Таблица {inp.file_path} обновлена'

# file_path = 'main.py'
# content = open(file_path).read()
# user.update_file(file_path, user.branch, content, 'Update structure')
#
# user.created_commit(user.branch, 'initial commit', file_path, file_path)
