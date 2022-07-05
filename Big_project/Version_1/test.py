import gitlab
from fastapi import HTTPException
import json
from operator import itemgetter
from elasticsearch import Elasticsearch

def str_format_verity(name):
    if name[0] in '.-' or name[:5] == '.atom' or name[:4] == '.git':
        raise HTTPException(404,
                            f"Неверный формат {name}. Сan contain only letters, digits, '_', '-' and '.'. Cannot start with '-', end in '.git' or end in '.atom'")


def sorts(lst, sort_by, column_to_sort):
    if sort_by is not None:
        sorted_lst = sorted(lst, key=itemgetter(column_to_sort))
        if sort_by == 'grow':
            return sorted_lst
        elif sort_by == 'not_grow':
            return sorted_lst[::-1]
        else:
            raise HTTPException(404, "Неизвестный тип сортировки")
    else:
        return lst



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
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404,
                                "База данных не былa найденa. Возможно неверно введено имя группы/проекта/базы данных.")
        except gitlab.exceptions.GitlabAuthenticationError:
            raise HTTPException(401, "Неверно введён токен")

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
            if type(data) == type(config) and len(data) == len(config) and list(data.keys()) == list(config.keys()):
                return data
            raise HTTPException(404, f"Неверный формат записи. Запись должна быть формата: {config}")

        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Таблицы {table_name} не существует")

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
            data_config = json.dumps(config)
            self._database.files.create(
                {
                    'file_path': f'{table_name}.jsons',
                    'branch': branch_name,
                    'content': json.dumps(""),
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
            lst = [json.loads(row) for row in
                   self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(
                       encoding='utf-8').split('\n')]
            if row_to_update > len(lst):
                lst = list(map(json.dumps, lst))
                for i in range(len(lst), row_to_update - 1):
                    lst.append(json.dumps(''))
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
            raise HTTPException(404, f"Таблицы {table_name} не существует. Возможно неверно введено имя таблицы")

    def read_table(self, table_name, branch_name: str, stop=None, start=None, sort_by=None, column_to_sort=None):
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
            sorted_lst = sorts(lst, sort_by, column_to_sort)
            return sorted_lst
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

    def git_elast(self, project_name, database_name, branch_name, table_name):
        HOSTS = ['https://probation.dev.finch.fm/es']
        es = Elasticsearch(HOSTS)
        try:
            index_name = f'{project_name}-{database_name}-{branch_name}-{table_name}'
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name)
            i = 0
            for row in self._database.files.get(file_path=f'{table_name}.jsons', ref=branch_name).decode().decode(
                    encoding='utf-8').split('\n'):
                i += 1
                es.index(index=index_name, id=i, body=json.loads(row))
        except gitlab.exceptions.GitlabGetError:
            raise HTTPException(404, f"Проверьте корректность введенных данных.")

#gda_gr = GitDatastoreApiGroups("glpat-hNeknDFPsv_DRqb51HdB", 'test_group_for_projects')
