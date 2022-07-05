from operator import itemgetter


def sort_table(self, table_name: str, column_to_sort, type_sort='grow' ):
    try:
        gen = (row for row in self._database.files.get(file_path=f'{table_name}.jsons', ref=project_name).decode().
            decode(encoding='utf-8').split('\n'))
        lst = []
        for row in gen:
            lst.append(str(row))
        if column_to_sort in self._database.files.get(file_path=f'{table_name}.conf', ref=project_name).decode().
            decode(encoding='utf-8').split('\n')):
            raise HTTPException(404, "Данный столбец отсутствует в таблице")
        if type_sort == 'grow':
            sorted_lst = sorted(lst, key=itemgetter(column_to_sort))
        elif type_sort == 'not_grow':
            sorted_lst = sorted(lst, key=itemgetter(column_to_sort))[::-1]
        else:
            raise HTTPException(404, "Неизвестный тип сортировки")
        return sorted_lst
    except gitlab.exceptions.GitlabHttpError:
        raise HTTPException(404, f"Таблицы {table_name} не существует")