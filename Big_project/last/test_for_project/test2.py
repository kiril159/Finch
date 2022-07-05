import io


def update_table(self, table_name: str, row_to_update: int, new_row: dict, database_name: str):
    if row_to_update <= 0 or type(row_to_update) == float:
        raise HTTPException(404, "Параметр row_to_update должен быть целым неотрицательным числом")
    database_name = self.database_verify(database_name)
    try:
        new_row = self.data_verify(table_name, new_row, database_name)
        gen = (row for row in
               self._project.files.get(file_path=f'{table_name}.jsons', ref=database_name).decode().decode(
                   encoding='utf-8').split('\n'))
        i = 0
        for row in gen:
            i += 1
            row = str((new_row if i == row_to_update else row)) + '\n'
        '''if row_to_update > len(list((row for row in self._project.files.get(file_path=f'{table_name}.jsons',
                                                                            ref=database_name).decode().decode(
                encoding='utf-8').split('\n')))):
            raise HTTPException(404, "Карочи надо сделать так чтобы добавлялись пустые строчки")'''

        data = {
            'branch': database_name,
            'commit_message': f'Update row{row_to_update} in {table_name}.jsons',
            'actions': [
                {
                    'action': 'update',
                    'file_path': f'{table_name}.jsons',
                    'content': gen,
                }
            ]
        }
        self._project.commits.create(data)
    except gitlab.exceptions.GitlabHttpError:
        raise HTTPException(404, f"Таблицы {table_name} не существует")