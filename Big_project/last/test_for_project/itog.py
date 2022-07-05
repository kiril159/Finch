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
            for i in range(len(lst), row_to_update - 1):
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


def insert_table(self, table_name: str, new_row: dict, database_name: str):
    database_name = self.database_verify(database_name)
    try:
        new_row = self.data_verify(table_name, new_row, database_name)
        gen = (row for row in self._project.files.get(file_path=f'{table_name}.jsons', ref=database_name).decode().
            decode(encoding='utf-8').split('\n'))
        lst = []
        for row in gen:
            lst.append(str(row))
        lst.append(str(new_row))
        data = {
            'branch': database_name,
            'commit_message': f'Insert row in {table_name}.jsons',
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