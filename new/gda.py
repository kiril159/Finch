#import os

import gitlab
from fastapi import HTTPException
import json
from operator import itemgetter
from elasticsearch import Elasticsearch
#import re

#ES_HOST = os.getenv("ES_HOST")


def del_in_elast(group_name,project_name, database_name, branch_name, table_name, num_str):
    HOSTS = ['https://probation.dev.finch.fm/es']
    es = Elasticsearch(HOSTS)
    index_name = f"{group_name}-{project_name}-{database_name}-{branch_name}-{table_name}".lower()
    es.index(index=index_name, id=num_str, body={})


def del_in_gitlab(config, lst, num_str):
    if num_str > len(lst):
        raise HTTPException(404, f"Такой строки не сущетсвует")
    else:
        i = 0
        for row in lst:
            i += 1
            row = (config if i == num_str else row)
            lst[i - 1] = json.dumps(row)
    return lst


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


def update_es(gl_prjct_get, group_name, project_name, database_name, branch_name, table_name, l):
    try:
        HOSTS = ['https://probation.dev.finch.fm/es']
        es = Elasticsearch(HOSTS)
        index_name = f"{group_name}-{project_name}-{database_name}-{branch_name}-{table_name}".lower()
        config = gl_prjct_get.files.get(file_path=f"{table_name}.conf", ref=branch_name).decode().decode(
            encoding='utf-8')
        if len(l) == 2:
            id = int(l[1][1])
            row = gl_prjct_get.files.get(file_path=f'{table_name}.jsons', ref=f"{branch_name}").decode().decode(
                encoding='utf-8').split('\n')[0]
            if row == config:
                es.index(index=index_name, id=id, body={})
            else:
                es.index(index=index_name, id=id, body=row)
        elif len(l) == 3:
            i = int(l[1][1])
            for j in range(int(l[2])):
                id = i + j
                row = gl_prjct_get.files.get(file_path=f'{table_name}.jsons', ref=f"{branch_name}").decode().decode(
                    encoding='utf-8').split('\n')[id - 1]
                if row == config:
                    es.index(index=index_name, id=id, body={})
                else:
                    es.index(index=index_name, id=id, body=row)
        elif len(l) == 4:
            i = int(l[2][1])
            for j in range(int(l[3])):
                id = i + j
                row = gl_prjct_get.files.get(file_path=f'{table_name}.jsons', ref=f"{branch_name}").decode().decode(
                        encoding='utf-8').split('\n')[id-1]
                if row == config:
                    es.index(index=index_name, id=id, body={})
                else:
                    es.index(index=index_name, id=id, body=row)
    except gitlab.exceptions.GitlabGetError:
        raise HTTPException(404, f"Таблица {table_name} не была найдена. Возможно неверно введено имя таблицы.")


def changed_files(database, kind_of_change, indices, group, project, db, branch):
    for table in kind_of_change:
        if table[-5:] == '.conf':
            continue
        table = table.split(".")[0].lower()
        add_to_es(database, group, project, db, branch, table)
        indices.append(f"{group}-{project}-{db}-{branch}-{table}")


def update_files(database, kind_of_change, indices, group, project, db, branch, l):
    for table in kind_of_change:
        if table[-5:] == '.conf':
            continue
        table = table.split(".")[0].lower()
        update_es(database, group, project, db, branch, table, l)
        indices.append(f"{group}-{project}-{db}-{branch}-{table}")


'''def git_elastic_hook(body):
    path = body['project']["path_with_namespace"].split("/")
    group = path[0]
    project = path[1]
    db = path[2]
    branch = body['ref'].split('/')[-1]

    gl = gitlab.Gitlab(private_token="glpat-hNeknDFPsv_DRqb51HdB")
    gl.auth()

    path_with_namespace = body['project']["path_with_namespace"]
    database = gl.projects.get(path_with_namespace)
    commits = database.commits.list(all=True,
                                    query_parameters={'ref_name': branch})
    diff = commits[0].diff()
    indices = []

    database = gl.projects.get(f'{group}/{project}/{db}')

    if body["commits"][0]['modified'] == [] and body["commits"][0]["removed"] == [] and body["commits"][0][
        "added"] == []:
        return "Nothing to commit"

    if body["commits"][0]['modified'] != []:
        mas = diff[0]['diff'].split('')
        for i in mas:
            i = str(i).lstrip('@@ ').rstrip(' @@')
            m = re.split(',| ', i)
            update_files(database, body["commits"][0]['modified'], indices, group, project, db, branch, m)
            if len(m) == 2:
                return diff[0][
                           'diff'], f"Из версии А извлечено 1 строка, начиная со строки {m[0][1]}. Из версии B извлечено 1 строка, начиная со строки {m[1][1]}"
            elif len(m) == 3:
                return diff[0][
                           'diff'], f"Из версии А извлечено 1 строка, начиная со строки {m[0][1]}. Из версии B извлечено {m[2]} строк, начиная со строки {m[1][1]}"
            elif len(m) == 4:
                return diff[0][
                           'diff'], f"Из версии А извлечено {m[1]} строк, начиная со строки {m[0][1]}. Из версии B извлечено {m[3]} строк, начиная со строки {m[2][1]}"

    if body["commits"][0]["added"] != []:
        changed_files(database, body["commits"][0]["added"], indices, group, project, db, branch)

    if body["commits"][0]["removed"] != []:
        HOSTS = ['https://probation.dev.finch.fm/es']
        es = Elasticsearch(HOSTS)
        for table in body["commits"][0]["removed"]:
            if table[-5:] == '.conf':
                continue
            table = table.split(".")[0].lower()
            index_name = f"{group}-{project}-{db}-{branch}-{table}".lower()
            es.indices.delete(index=index_name)
            indices.append(f"index {index_name} was deleted")

    return diff[0]['diff']


# def get_id(gl, name):
#     for i in range(len(gl.groups.list())):
#         if gl.groups.list()[i].name == name:
#             name_id = gl.groups.list()[i].id
#             return name_id
#     raise NameError'''


class GitDatastoreApiGroups:  # можно добавить параметр path, чтобы выбирать ссылку на группу/проект/базу данных.
    def __init__(self, token, group_name):
        try:
            self.token = token
            self._gl = gitlab.Gitlab(private_token=token)
            self._gl.auth()
            self.group_name = group_name
            self.group_id = self.group_id = self._gl.groups.list(search=group_name)[0].id
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")
        except IndexError:
            raise HTTPException(404, "Группа с таким именем не была найдена. Возможно неверно введено имя группы.")

    def create_project(self, project_name: str):
        str_format_verity(project_name)
        try:
            self._gl.groups.create({'name': project_name, 'path': project_name, 'parent_id': self.group_id})
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Невозможно создать проект. Измените название проекта.")

    def create_database(self, database_name, project_name):
        str_format_verity(database_name)
        try:
            for i in range(len(self._gl.groups.list(search=project_name))):
                if self._gl.groups.list(search=project_name)[i].name == project_name:
                    group_id = self._gl.groups.list(search=project_name)[i].id
                    self._gl.projects.create({'name': database_name, 'namespace_id': group_id})
                    db_id = self._gl.projects.get(f'{self.group_name}/{project_name}/{database_name}').id
                    project = self._gl.projects.get(db_id)
                    project.hooks.create({'url': 'https://pulseteam.dev.finch.fm/datastore-api/hook', 'push_events': True})
                    break
        except gitlab.exceptions.GitlabCreateError:
            raise HTTPException(404, "Невозможно создать базу данных. Измените название базы данных.")
        except IndexError:
            raise HTTPException(404, "Проект с таким именем не был найден. Возможно неверно введено имя проекта")

    def project_delete(self, project_name: str):
        for i in range(len(self._gl.groups.list(search=project_name))):
            if self._gl.groups.list(search=project_name)[i].name == project_name:
                group_id = self._gl.groups.list(search=project_name)[i].id
                self._gl.groups.delete(group_id)
                return None
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
                                f"База данных {database_name} не была найдена. Возможно неверно введено имя проекта/базы данных.")


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
        return str((type(value).__name__))

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
            del_in_gitlab(config, lst, num_str)
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

        try:
            del_in_elast(group_name, project_name, database_name, branch_name, table_name, num_str)
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Проверьте корректность введенных данных.")
