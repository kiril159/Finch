import gitlab
from fastapi import HTTPException
import os
import json
import string


def str_format_verity(name):
    alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
    # alphabet = string.ascii_letters
    if name[0] == '-' or name[:5] == '.atom' or name[:4] == '.git':
        raise f"Неверный формат {name}. can contain only letters, digits, '_', '-' and '.'. Cannot start with '-', end in '.git' or end in '.atom'"
    for letter in name:
        if letter in alphabet:
            raise f"Неверный формат {name}. Все буквы должны быть написаны на латинице"


def create_project(token, project_name):
    str_format_verity(project_name)
    try:
        gl = gitlab.Gitlab(private_token=token)
        gl.auth()
        gl.projects.create({'name': project_name})
        return f"Project {project_name} has been created"
    except gitlab.exceptions.GitlabCreateError:
        raise HTTPException(404,
                            "Проект с таким именем уже существует или имя проекта введено в неверном формате(can contain only letters, digits, '_', '-' and '.'. Cannot start with '-', end in '.git' or end in '.atom')")
    except gitlab.exceptions.GitlabAuthenticationError:
        raise HTTPException(401, "Неверно введён токен")


class GitDatastoreApi:
    '''
    Project: Проект
    Database: Ветка репозитория
    Table: Файл на гитлабе
    '''

    def __init__(self, token, gl_account_name, project_name, name, email):
        str_format_verity(project_name)
        str_format_verity(gl_account_name)
        try:
            self._gl = gitlab.Gitlab(private_token=token)
            self._gl.auth()
            self.gl_account_name = gl_account_name
            self.name = name
            self.email = email
            self._project = self._gl.projects.get(f'{self.gl_account_name}/{project_name}')
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, "Проект не был найден")
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")

    def database_verify(self, database_name: str):
        str_format_verity(database_name)
        lst = [db_name.name for db_name in self._project.branches.list()]
        if database_name in lst:
            return database_name
        raise HTTPException(404, "Базы данных с таким именем не существует")

    def data_verify(self, table_name: str, data: dict, database_name: str):
        str_format_verity(database_name)
        str_format_verity(table_name)
        database_name = self.database_verify(database_name)
        try:
            config = json.loads(
                self._project.files.get(file_path=f"{table_name}.conf", ref=database_name).decode().decode(
                    encoding='utf-8'))
            if type(data) == type(config) and len(data) == len(config) and list(data.keys()) == list(config.keys()):
                print('success')
                return data
            raise HTTPException(404, f"Неверный формат записи. Запись должна быть формата: {config}")

        except:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

    def create_database(self, database_name: str):
        str_format_verity(database_name)
        try:
            self._project.branches.create({'branch': database_name, 'ref': 'main'})
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "База данных с таким именем уже существует")

    def create_table(self, table_name: str, config: dict, database_name: str):
        str_format_verity(table_name)
        str_format_verity(database_name)
        try:
            database_name = self.database_verify(database_name)
            data_config = json.dumps(config)
            self._project.files.create(
                {
                    'file_path': f'{table_name}.jsons',
                    'branch': database_name,
                    'content': " ",
                    'author_email': self.email,
                    'author_name': self.name,
                    'commit_message': "initial_commit"
                })
            self._project.files.create(
                {
                    'file_path': f'{table_name}.conf',
                    'branch': database_name,
                    'content': data_config,
                    'author_email': self.email,
                    'author_name': self.name,
                    'commit_message': "initial_commit"
                })
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Таблица с таким именем уже существует")

    def update_table(self, table_name: str, row_to_update: int, new_row: dict, database_name: str):
        if row_to_update <= 0 or type(row_to_update) == float:
            raise HTTPException(404, "Параметр row_to_update должен быть целым неотрицательным числом")
        database_name = self.database_verify(database_name)
        try:
            new_row = self.data_verify(table_name, new_row, database_name)
            gen = (row for row in self._project.files.get(file_path=f'{table_name}.jsons', ref=database_name).decode().
                decode(encoding='utf-8').split('\n'))
            lst = []
            i = 0
            for row in gen:
                i += 1
                row = (new_row if i == row_to_update else row)
                lst.append(str(row))
            if row_to_update > len(lst):
                for i in range(len(lst), row_to_update-1):
                    lst.append("{}")
                lst.append(str(new_row))
            data = {
                'branch': database_name,
                'commit_message': f'Update row{row_to_update} in {table_name}.jsons',
                'actions': [
                    {
                        'action': 'update',
                        'file_path': f'{table_name}.jsons',
                        'content': "\n".join(lst)
                    }
                ]
            }
            self._project.commits.create(data)
        except gitlab.exceptions.GitlabHttpError:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

    def read_table(self, table_name, database_name: str, stop=None, start=None):
        str_format_verity(table_name)
        str_format_verity(database_name)
        database_name = self.database_verify(database_name)
        try:
            if not start or start < 0:
                start = 0
            if not stop or stop < 0:
                stop = len(self._project.files.get(file_path=f'{table_name}.jsons', ref=database_name).decode().decode(
                    encoding='utf-8').split('\n')) + 1
            if stop <= start:
                raise HTTPException(404, "Параметр start должен быть меньше параметра stop")

            i = 0
            lst = []
            for row in self._project.files.get(file_path=f'{table_name}.jsons', ref=database_name).decode().decode(
                    encoding='utf-8').split('\n'):
                i += 1
                if start <= i < stop:
                    lst.append(row)
            return lst
        except:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

    def table_delete(self, table_name: str, database_name: str):
        str_format_verity(table_name)
        str_format_verity(database_name)
        database_name = self.database_verify(database_name)
        try:
            self._project.files.delete(file_path=f'{table_name}.jsons', commit_message=f'Delete {table_name}.jsons',
                                       branch=database_name)
            self._project.files.delete(file_path=f'{table_name}.conf', commit_message=f'Delete {table_name}.jsons',
                                       branch=database_name)
        except gitlab.exceptions.GitlabDeleteError:
            raise HTTPException(400, "Таблица с таким именем не была найдена")

    def database_delete(self, database_name):
        database_name = self.database_verify(database_name)
        self._project.branches.delete(database_name)

    def project_delete(self, project_id):
        try:
            self._gl.projects.delete(project_id)
        except:
            raise HTTPException(404, "Проект с таким id не был найден")

# gda = GitDatastoreApi("glpat-hNeknDFPsv_DRqb51HdB", 'markgaranin24', "1", "markgaranin", 'markgaranin@list.ru')

# create_project("glpat-hNeknDFPsv_DRqb51HdB", "1")
# gda.create_database('db')
# gda.create_table('table', {"name": "", "age": 0}, 'db')
# gda.update_table('table', 2, {"name": "", "age": 0}, 'db')
# print(gda.read_table('table', 'db', 20, 10))
# gda.table_delete('pam', 'pampam')
# gda.database_delete('pampam')
# gda.project_delete('36569969')
