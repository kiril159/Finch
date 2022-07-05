class DeleteString(BaseModel):
    project_name: str
    database_name: str
    branch_name: str
    table_name: str
    num_str: int


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




@app.post("/datastore-api/delete_str")
async def delete_str(delete_string: models.DeleteString, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.del_str(gda_prjct.group_name, delete_string.project_name, delete_string.database_name, delete_string.branch_name,
                delete_string.table_name, delete_string.num_str)
    return "String was delete in Database and Elastic"


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