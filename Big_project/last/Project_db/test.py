import gitlab
from fastapi import HTTPException
import os
import json


class GitDatastoreApiGroups:
    def __init__(self, token, group_id):
        try:
            self.token = token
            self._gl = gitlab.Gitlab(private_token=token)
            self._gl.auth()
            self.group_id = group_id
        except:
            pass

    def create_project(self, project_name: str):
        try:
            self._gl.groups.create({'name': project_name, 'path': project_name, 'parent_id': self.group_id})
            return f"Project {project_name} has been created"
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Проект с таким именем уже существует.")
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")

    def database_verify(self, database_name: str, project_name: str):
        try:
            group_id = self._gl.groups.list(search=project_name)[0].id
            lst = [prj_name.name for prj_name in self._gl.groups.get(group_id).projects.list()]
            if database_name in lst:
                return database_name
            raise HTTPException(404, "Базы данных с таким именем не существует")
        except IndexError:
            raise HTTPException(404, "Проект с таким именем не был найден")

    def create_database(self, database_name, project_name): # prokject=database?
        alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        for letter in database_name:
            if letter in alphabet:
                raise f"Неверный формат {database_name}. Все буквы должны быть написаны на латинице"
        try:
            group_id = self._gl.groups.list(search=project_name)[0].id
            self._gl.projects.create({'name': database_name, 'namespace_id': group_id})
            return f"Database {database_name} has been created"
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "База данных с таким именем уже существует.")
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")

    def project_delete(self, project_name: str):
        try:
            group_id = self._gl.groups.list(search=project_name)[0].id
            self._gl.groups.delete(group_id)
        except:
            raise HTTPException(404, "Проект с таким id не был найден")

#g = GitDatastoreApiGroups("glpat-hNeknDFPsv_DRqb51HdB", 53763296)
#g.create_database('test_db3', 'test_prjct')
#g.database_verify("test_db", 'test_prjct3')


def str_format_verity(name):
    if name[0] in '.-' or name[:5] == '.atom' or name[:4] == '.git':
        raise f"Неверный формат {name}. can contain only letters, digits, '_', '-' and '.'. Cannot start with '-', end in '.git' or end in '.atom'"


class GitDatastoreApiProjects:
    '''
    Project: Группа проектов
    Database: Проект
    Ветка репозитория: пока неизвестно
    Table: Файл на гитлабе
    '''

    def __init__(self, token, database_name, name, email, group_name, project_name):
        str_format_verity(database_name)
        try:
            self._gl = gitlab.Gitlab(private_token=token)
            self._gl.auth()
            self.db_name = database_name
            self.group = group_name
            self.project = project_name
            self.name = name
            self.email = email
            self._database = self._gl.projects.get(f'{self.group}/{self.project}/{self.db_name}')
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, "База данных не былa найденa")
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")

    def branch_verify(self, branch_name: str):
        str_format_verity(branch_name)
        lst = [db_name.name for db_name in self._database.branches.list()]
        if branch_name in lst:
            return branch_name
        raise HTTPException(404, "Базы данных с таким именем не существует")

    def data_verify(self, table_name: str, data: dict, branch_name: str):
        str_format_verity(branch_name)
        branch_name = self.branch_verify(branch_name)
        try:
            config = json.loads(self._database.files.get(file_path=f"{table_name}.conf", ref=branch_name).decode().decode(encoding='utf-8'))
            if type(data) == type(config) and len(data) == len(config) and list(data.keys()) == list(config.keys()):
                print('success')
                return data
            raise HTTPException(404, f"Неверный формат записи. Запись должна быть формата: {config}")

        except:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

    '''def create_branch(self, branch_name: str):
        str_format_verity(branch_name)
        try:
            self._database.branches.create({'branch': branch_name, 'ref': 'main'})
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Ветка с таким именем уже существует")'''

    def create_table(self, table_name: str, config: dict, branch_name: str):
        try:
            branch_name = self.branch_verify(branch_name)
            data_config = json.dumps(config)
            self._database.files.create(
                {
                    'file_path': f'{table_name}.jsons',
                    'branch': branch_name,
                    'content': " ",
                    'author_email': self.email,
                    'author_name': self.name,
                    'commit_message': "initial_commit"
                })
            self._database.files.create(
                {
                    'file_path': f'{table_name}.conf',
                    'branch': branch_name,
                    'content': data_config,
                    'author_email': self.email,
                    'author_name': self.name,
                    'commit_message': "initial_commit"
                })
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Таблица с таким именем уже существует")

    def update_table(self, table_name: str, row_to_update: int, new_row: dict, branch_name: str):
        if row_to_update <= 0 or type(row_to_update) == float:
            raise HTTPException(404, "Параметр row_to_update должен быть целым неотрицательным числом")
        branch_name = self.branch_verify(branch_name)
        try:
            new_row = self.data_verify(table_name, new_row, branch_name)
            gen = (row for row in self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().
                decode(encoding='utf-8').split('\n'))
            lst = []
            i = 0
            for row in gen:
                i += 1
                row = (new_row if i == row_to_update else row)
                lst.append(str(row))
            if row_to_update > len(lst):
                for i in range(len(lst), row_to_update - 1):
                    lst.append("{}")
                lst.append(str(new_row))
            data = {
                'branch': branch_name,
                'commit_message': f'Update row{row_to_update} in {table_name}.jsons',
                'actions': [
                    {
                        'action': 'update',
                        'file_path': f'{table_name}.jsons',
                        'content': "\n".join(lst)
                    }
                ]
            }
            self._database.commits.create(data)
        except gitlab.exceptions.GitlabHttpError:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

    def read_table(self, table_name, branch_name: str, stop=None, start=None):
        branch_name = self.branch_verify(branch_name)
        try:
            if not start or start < 0:
                start = 0
            if not stop or stop < 0:
                stop = len(self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(encoding='utf-8').split('\n')) + 1
            if stop <= start:
                raise HTTPException(404, "Параметр start должен быть меньше параметра stop")

            i = 0
            lst = []
            for row in self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(encoding='utf-8').split('\n'):
                i += 1
                if start <= i < stop:
                    lst.append(row)
            return lst
        except:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

    def table_delete(self, table_name: str, branch_name: str):
        branch_name = self.branch_verify(branch_name)
        try:
            self._database.files.delete(file_path=f'{table_name}.jsons', commit_message=f'Delete {table_name}.jsons', branch=branch_name)
            self._database.files.delete(file_path=f'{table_name}.conf', commit_message=f'Delete {table_name}.jsons', branch=branch_name)
        except gitlab.exceptions.GitlabDeleteError:
            raise HTTPException(400, "Таблица с таким именем не была найдена")

    def branch_delete(self, branch_name):
        branch_name = self.branch_verify(branch_name)
        self._database.branches.delete(branch_name)

    def database_delete(self, project_id):
        try:
            self._gl.projects.delete(project_id)
        except:
            raise HTTPException(404, "База данных с таким id не была найдена")

    def insert_on_table(self, table_name: str, new_row: dict, branch_name: str):
        branch_name = self.branch_verify(branch_name)
        try:
            new_row = self.data_verify(table_name, new_row, branch_name)
            gen = (row for row in self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().
                decode(encoding='utf-8').split('\n'))
            lst = []
            for row in gen:
                lst.append(str(row))
            lst.append(str(new_row))
            data = {
                'branch': branch_name,
                'commit_message': f'Insert row in {table_name}.jsons',
                'actions': [
                    {
                        'action': 'update',
                        'file_path': f'{table_name}.jsons',
                        'content': "\n".join(lst)
                    }
                ]
            }
            self._database.commits.create(data)
        except gitlab.exceptions.GitlabHttpError:
            raise HTTPException(404, f"Таблицы {table_name} не существует")


#gda = GitDatastoreApi("glpat-hNeknDFPsv_DRqb51HdB", "1", "markgaranin", 'markgaranin@list.ru', 'test_group_for_projects', 'test_subgroup')

#create_database("glpat-hNeknDFPsv_DRqb51HdB", "1")
#gda.create_branch('аывфа')
#gda.create_table('-аыва', {"name": "", "age": 0}, 'аывфа')
#gda.update_table('table', 2, {"name": "", "age": 0}, 'db')
#print(gda.read_table('table', 'db', 20, 10))
#gda.table_delete('-аыва', 'аывфа')
#gda.branch_delete('аывфа')
#gda.database_delete('36572486')



# gl = gitlab.Gitlab(private_token="glpat-hNeknDFPsv_DRqb51HdB")
# gl.auth()
# gl.projects.delete('36572486')


#create_project("glpat-hNeknDFPsv_DRqb51HdB", 'test_subgroup', '537GDFSG63296')
#create_database("glpat-hNeknDFPsv_DRqb51HdB", "2", 'test_subgroup')

