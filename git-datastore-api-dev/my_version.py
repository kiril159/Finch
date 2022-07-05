import gitlab
from fastapi import HTTPException
import os
gl = gitlab.Gitlab(private_token="glpat-hNeknDFPsv_DRqb51HdB")
gl.auth()
#print(gl.projects.get('markgaranin24/finch').files.get(file_path='Task_1_my_file/log.txt', ref='master'))


def create_project(project_name: str):
    try:
        gl.projects.create({'name': project_name})
    except gitlab.exceptions.GitlabCreateError:
        raise HTTPException(404, "Проект с таким именем уже существует")

#create_project('Test_db_project')

def create_branch(branch_name: str):
    try:
        project = gl.projects.get('markgaranin24/Test_db_project')
        project.branches.create({'branch': branch_name, 'ref': 'main'})
    except gitlab.exceptions.GitlabCreateError:
        raise HTTPException(404, "Ветка с таким именем уже существует")

#create_branch('Test_branch')

def create_file(file_name: str):
    try:
        project = gl.projects.get('markgaranin24/Test_db_project')
        project.files.create({
        'file_path': file_name,
        'branch': 'Test_branch',
        'content': open(file_name).read(),
        'author_email': 'markgaranin@list.ru',
        'author_name': "markgaranin",
        'commit_message': "initial_commit"
        })
    except gitlab.exceptions.GitlabCreateError:
        raise HTTPException(404, "Файл с таким именем уже существует")

#create_file('test.txt')
def update_file(file_name: str, row_to_update: int, new_row: str):
    with open('for_update.jsons', mode='w') as file:
        project = gl.projects.get('markgaranin24/Test_db_project')
        gen = (row for row in project.files.get(file_path=file_name, ref='Test_branch').decode().decode(encoding='utf-8').split('\n'))
        i = 0

        for row in gen:
            i += 1
            row = (new_row if i == row_to_update else row) + '\n'
            file.write(row)
        if row_to_update > len(list((row for row in project.files.get(file_path=file_name, ref='Test_branch').decode().decode(encoding='utf-8').split('\n')))):
            raise HTTPException(404, "Карочи надо сделать так чтобы добавлялись пустые строчки")
    data = {
        'branch': 'Test_branch',
        'commit_message': 'blah blah blah',
        'actions': [
            {
                'action': 'update',
                'file_path': 'test.txt',
                'content': open('for_update.jsons', mode='r', encoding='utf-8').read(),
            }
        ]
    }
    project.commits.create(data)
    os.remove('for_update.jsons')

#update_file('test.txt', 2, "sadghdfshdvx vsgsdf vcsdf ")


def file_read(file_name, stop=None, start=None):
    project = gl.projects.get('markgaranin24/Test_db_project')
    if not start or start < 0:
        start = 0
    if not stop or stop < 0:
        stop = len(project.files.get(file_path=file_name, ref='Test_branch').decode().decode(encoding='utf-8').split('\n')) + 1

    i = 0
    for row in project.files.get(file_path=file_name, ref='Test_branch').decode().decode(encoding='utf-8').split('\n'):
        i += 1
        if start <= i < stop:
            print(row)

#file_read('test.txt')

def file_delete():
    pass

