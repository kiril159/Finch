import gitlab
from fastapi import HTTPException
import json
from operator import itemgetter
from elasticsearch import Elasticsearch


def str_format_verity(name):
    if name[0] in '.-' or name[:5] == '.atom' or name[:4] == '.git':
        raise HTTPException(404,
                            f"Неверный формат {name}. Сan contain only letters, digits, '_', '-' and '.'. Cannot start with '-', end in '.git' or end in '.atom'")


def do_sort(lst, sort_by: str, column_name: str):
    sort_by = sort_by.lower()
    try:
        if sort_by != "":
            sorted_lst = sorted(lst, key=itemgetter(column_name))
            if sort_by == 'asc':
                return sorted_lst
            elif sort_by == 'desc':
                return sorted_lst[::-1]
            else:
                raise HTTPException(404,
                                    r"Для сортировки по возрастанию введите 'asc', по убыванию - 'desc'. Без сортировки - ""  ")
        else:
            return lst
    except KeyError:
        raise HTTPException(404, f"Столбец {column_name} не был найден.")


def add_to_es(gl_prjct_get, group_name, project_name, database_name, branch_name, table_name):
    try:
        HOSTS = ['https://probation.dev.finch.fm/es']
        es = Elasticsearch(HOSTS)
        index_name = f"{group_name}-{project_name}-{database_name}-{branch_name}-{table_name}".lower()
        config = gl_prjct_get.files.get(file_path=f"{table_name}.conf", ref=branch_name).decode().decode(
            encoding='utf-8')

        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
        i = 0
        for row in gl_prjct_get.files.get(file_path=f'{table_name}.jsons', ref=f"{branch_name}").decode().decode(
                encoding='utf-8').split('\n'):
            i += 1
            if row == config:
                es.index(index=index_name, id=i, body={})
            else:
                es.index(index=index_name, id=i, body=row)
    except gitlab.exceptions.GitlabGetError:
        raise HTTPException(404, f"Таблица {table_name} не была найдена. Возможно неверно введено имя таблицы.")


class GitDatastoreApiGroups:  # можно добавить параметр path, чтобы выбирать ссылку на группу/проект/базу данных.
    def __init__(self, token, group_name):
        try:
            self.token = token
            self._gl = gitlab.Gitlab(private_token=token)
            self._gl.auth()
            self.group_name = group_name
            self.group_id = self._gl.groups.list(search=group_name)[0].id
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")
        except IndexError:
            raise HTTPException(404, "Группа с таким именем не была найдена. Возможно неверно введено имя группы.")

    def create_project(self, project_name: str):
        str_format_verity(project_name)
        try:
            self._gl.groups.create({'name': project_name, 'path': project_name, 'parent_id': self.group_id})
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Проект с таким именем уже существует.")

    def create_database(self, database_name, project_name):
        str_format_verity(database_name)
        try:
            group_id = self._gl.groups.list(search=project_name)[0].id
            self._gl.projects.create({'name': database_name, 'namespace_id': group_id})
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404,
                                "База данных с таким именем уже существует. Возможно неверно введено имя базы данных")
        except IndexError:
            raise HTTPException(404, "Проект с таким именем не был найден. Возможно неверно введено имя проекта")

    def project_delete(self, project_name: str):
        try:
            group_id = self._gl.groups.list(search=project_name)[0].id
            self._gl.groups.delete(group_id)
        except:
            raise HTTPException(404, "Проект не был найден. Возможно неверно введено имя проекта.")

    def database_delete(self, project_name, database_name):
        # alphabet = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
        # f = [letter for letter in filter(lambda letter: letter not in alphabet, database_name)]
        # database_name = ''.join(f)

        try:
            project = self._gl.projects.get(f'{self.group_name}/{project_name}/{database_name}')
            project.delete()
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404,
                                "База данных с таким id не была найдена. Возможно неверно введено имя проекта/базы данных.")


class GitDatastoreApiProjects:
    '''
    Project: Группа проектов
    Database: Проект
    Ветка репозитория: пока неизвестно
    Table: Файл на гитлабе
    '''

    def __init__(self, token, database_name, group_name, project_name):
        try:
            self._gl = gitlab.Gitlab(private_token=token)
            self._gl.auth()
            self.db_name = database_name
            self.group = group_name
            self.project = project_name
            self._database = self._gl.projects.get(f'{self.group}/{self.project}/{self.db_name}')
            self._gda_types = ("str", "int", "float", "list", "dict", "bool")
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404,
                                "База данных не былa найденa. Возможно неверно введено имя группы/проекта/базы данных.")
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")

    def type_verify(self, value):
        if type(value) == str:
            value = "str"
        elif type(value) == int:
            value = "int"
        elif type(value) == float:
            value = "float"
        elif type(value) == list:
            value = "list"
        elif type(value) == dict:
            value = "dict"
        elif type(value) == bool:
            value = "bool"
        return value

    def gda_types(self, value):
        if value in self._gda_types:
            return True
        return False

    def branch_verify(self, branch_name: str):
        lst = [db_name.name for db_name in self._database.branches.list()]
        if branch_name in lst:
            return branch_name
        raise HTTPException(404, "Ветки с таким именем не существует. Возможно неверно введено имя ветки.")

    def data_verify(self, table_name: str, data: dict, branch_name: str):
        try:
            config = json.loads(
                self._database.files.get(file_path=f"{table_name}.conf", ref=branch_name).decode().decode(
                    encoding='utf-8'))
            if type(data) != type(config) or len(data) != len(config) or list(data.keys()) != list(config.keys()):
                raise HTTPException(404, f"Неверный формат записи. Запись должна быть формата: {config}")

            conf_val = [val for val in config.values()]
            data_val = [val for val in data.values()]

            for i in range(len(conf_val)):
                val_type = self.type_verify(data_val[i])
                if conf_val[i] == val_type:
                    continue

                raise HTTPException(404,
                                    f"В поле {list(config.keys())[i]} записанны данные неверного типа. Требуется тип {conf_val[i]}")

            return data

        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Таблицы {table_name} не существует. Возможно неверно введено имя таблицы.")

    def create_branch(self, branch_name: str):
        if branch_name[0] in '.-' or branch_name[:5] == '.atom' or branch_name[:4] == '.git':
            raise HTTPException(404,
                                "Неверный формат {name}. can contain only letters, digits, '_', '-' and '.'. Cannot start with '-', end in '.git' or end in '.atom'")
        try:
            self._database.branches.create({'branch': branch_name, 'ref': 'main'})
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Ветка с таким именем уже существует")

    def create_table(self, table_name: str, config: dict, branch_name: str, author_name: str, author_email: str):
        try:
            branch_name = self.branch_verify(branch_name)

            for i in range(len(config)):
                if self.gda_types(list(config.values())[i]):
                    pass
                else:
                    raise HTTPException(404,
                                        f"В поле {list(config.keys())[i]} в конфиге записан несуществующий тип данны. Доступны типы: {self._gda_types}")

            data_config = json.dumps(config)
            self._database.files.create(
                {
                    'file_path': f'{table_name}.jsons',
                    'branch': branch_name,
                    'content': data_config,
                    'author_email': author_email,
                    'author_name': author_name,
                    'commit_message': "initial_commit"
                })
            self._database.files.create(
                {
                    'file_path': f'{table_name}.conf',
                    'branch': branch_name,
                    'content': data_config,
                    'author_email': author_email,
                    'author_name': author_name,
                    'commit_message': "initial_commit"
                })
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Таблица с таким именем уже существует")

    def update_table(self, table_name: str, row_to_update: int, new_row: dict, branch_name: str):
        branch_name = self.branch_verify(branch_name)
        if row_to_update <= 0 or type(row_to_update) == float:
            raise HTTPException(404, "Параметр row_to_update должен быть целым неотрицательным числом.")

        try:
            new_row = self.data_verify(table_name, new_row, branch_name)
            config = json.loads(
                self._database.files.get(file_path=f"{table_name}.conf", ref=branch_name).decode().decode(
                    encoding='utf-8'))
            lst = [json.loads(row) for row in
                   self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(
                       encoding='utf-8').split('\n')]
            if row_to_update > len(lst):
                lst = list(map(json.dumps, lst))
                for i in range(len(lst), row_to_update - 1):
                    lst.append(json.dumps(config))
                lst.append(json.dumps(new_row))
            else:
                i = 0
                for row in lst:
                    i += 1
                    row = (new_row if i == row_to_update else row)
                    lst[i - 1] = json.dumps(row)
            data = {
                'branch': branch_name,
                'commit_message': f'Update row {row_to_update} in {table_name}.jsons',
                'actions': [
                    {
                        'action': 'update',
                        'file_path': f'{table_name}.jsons',
                        'content': "\n".join(lst),
                    }
                ]
            }
            self._database.commits.create(data)
        except gitlab.exceptions.GitlabHttpError:
            raise HTTPException(404, f"Таблицы {table_name} не существует. Возможно неверно введено имя таблицы.")

    def read_table(self, table_name, branch_name: str, stop=None, start=None, sort_by=None, column_name=None):
        branch_name = self.branch_verify(branch_name)
        try:
            if not start or start < 0:
                start = 0
            if not stop or stop < 0:
                stop = len(self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(
                    encoding='utf-8').split('\n')) + 1
            if stop <= start:
                raise HTTPException(404, "Параметр start должен быть меньше параметра stop.")

            i = 0
            lst = []
            for row in self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(
                    encoding='utf-8').split('\n'):
                i += 1
                if start <= i < stop:
                    lst.append(json.loads(row))
            lst = do_sort(lst, sort_by, column_name)
            return lst
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Таблицы {table_name} не существует. Возможно неверно введено имя таблицы.")

    def table_delete(self, table_name: str, branch_name: str):
        branch_name = self.branch_verify(branch_name)
        try:
            self._database.files.delete(file_path=f'{table_name}.jsons', commit_message=f'Delete {table_name}.jsons',
                                        branch=branch_name)
            self._database.files.delete(file_path=f'{table_name}.conf', commit_message=f'Delete {table_name}.jsons',
                                        branch=branch_name)
        except gitlab.exceptions.GitlabDeleteError:
            raise HTTPException(400, "Таблица с таким именем не была найдена. Возможно неверно введено имя таблицы.")

    def branch_delete(self, branch_name):
        branch_name = self.branch_verify(branch_name)
        self._database.branches.delete(branch_name)

    def git_elastic(self, project_name, database_name, branch_name, table_name):
        branch_name = self.branch_verify(branch_name)
        try:
            add_to_es(self._database, self.group, project_name, database_name, branch_name, table_name)
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Таблица {table_name} не была найдена. Возможно неверно введено имя таблицы.")

    def del_str(self, group_name, project_name, database_name, branch_name, table_name, num_str):
        branch_name = self.branch_verify(branch_name)
        if num_str <= 0 or type(num_str) == float:
            raise HTTPException(404, "Параметр row_to_update должен быть целым неотрицательным числом.")

        try:
            config = json.loads(
                self._database.files.get(file_path=f"{table_name}.conf", ref=branch_name).decode().decode(
                    encoding='utf-8'))
            lst = [json.loads(row) for row in
                   self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(
                       encoding='utf-8').split('\n')]
            if num_str > len(lst):
                raise HTTPException(404, f"Такой строки не сущетсвует")
            else:
                i = 0
                for row in lst:
                    i += 1
                    row = (config if i == num_str else row)
                    lst[i - 1] = json.dumps(row)
            data = {
                'branch': branch_name,
                'commit_message': f'delete str',
                'actions': [
                    {
                        'action': 'update',
                        'file_path': f'{table_name}.jsons',
                        'content': "\n".join(lst),
                    }
                ]
            }
            self._database.commits.create(data)
        except gitlab.exceptions.GitlabHttpError:
            raise HTTPException(404, f"Таблицы {table_name} не существует. Возможно неверно введено имя таблицы.")

        HOSTS = ['https://probation.dev.finch.fm/es']
        es = Elasticsearch(HOSTS)
        try:
            index_name = f"{group_name}-{project_name}-{database_name}-{branch_name}-{table_name}".lower()
            es.index(index=index_name, id=num_str, body={})
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Проверьте корректность введенных данных.")
