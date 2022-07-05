from fastapi import FastAPI, Request
from gda import GitDatastoreApiProjects
from gda import GitDatastoreApiGroups
#from gda import git_elastic_hook
import models


app = FastAPI(title='GitDatastoreApi',
              docs_url='/datastore-api/docs',
              openapi_url='/datastore-api',
              )


def gda_prjct_obj(token, database_name, project_name, group_name):
    gda = GitDatastoreApiProjects(token, database_name, group_name, project_name)  # "glpat-hNeknDFPsv_DRqb51HdB"
    return gda


def gda_grp_obj(token, group_name):
    gda = GitDatastoreApiGroups(token, group_name)  # "glpat-hNeknDFPsv_DRqb51HdB", 'test_group_for_projects'
    return gda


@app.post("/datastore-api/create/project")
async def create_project(project: models.Project, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.create_project(project.project_name)
    return f"Project {project.project_name} has been created"


@app.post("/datastore-api/create/database")
async def create_database(database: models.Database, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.create_database(database.database_name, database.project_name)
    return f"Database {database.database_name} has been created"


@app.post("/datastore-api/delete/project")
async def delete_project(project: models.Project, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.project_delete(project.project_name)
    return f"Project {project.project_name} has been deleted"


@app.post("/datastore-api/delete/database")
async def delete_database(database: models.Database, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.database_delete(database.project_name, database.database_name)
    return f"Database {database.database_name} has been deleted"


@app.post("/datastore-api/create/branch")
async def create_branch(branch: models.Branch, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.create_branch(branch.branch_name)
    return f"Branch {branch.branch_name} has been created"


@app.post("/datastore-api/create/table")
async def create_table(create_tbl: models.CreateTable, table: models.WorkWithTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.create_table(table.table_name, create_tbl.config, table.branch_name, create_tbl.author_name,
                     create_tbl.author_email)
    return f"Table {table.table_name} has been created"


@app.post("/datastore-api/update")
async def update_table(update: models.UpdateTable, table: models.WorkWithTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.update_table(table.table_name, update.row_to_update, update.new_row, table.branch_name)
    return f"Row has been updated"


@app.post("/datastore-api/read/")
async def read_table(read: models.ReadTable, table: models.WorkWithTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    return gda.read_table(table.table_name, table.branch_name, stop=read.stop, start=read.start, sort_by=read.sort_by,
                          column_name=read.column_name)


@app.post("/datastore-api/delete/branch")
async def delete_branch(branch: models.Branch, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.branch_delete(branch.branch_name)
    return f"Branch {branch.branch_name} has been deleted"


@app.post("/datastore-api/delete/table")
async def delete_table(table: models.WorkWithTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.table_delete(table.table_name, table.branch_name)
    return f"Table {table.table_name} has been deleted"


@app.post("/datastore-api/upload_on_elastic")
async def upload_on_elastic(elast_git: models.WorkWithTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.git_elastic(gda_prjct.project_name, gda_prjct.database_name, elast_git.branch_name, elast_git.table_name)
    return f'{gda_prjct.group_name}-{gda_prjct.project_name}-{gda_prjct.database_name}-{elast_git.branch_name}-{elast_git.table_name}'.lower() + ' was insert in elastic'


@app.post("/datastore-api/hook")
async def git_elastic(req: Request):
    body = await req.json()
    req.headers.get("X-Gitlab-Event")
    return git_elastic_hook(body)


@app.post("/datastore-api/delete_str")
async def delete_str(delete_string: models.DeleteString, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.del_str(gda_prjct.group_name, delete_string.project_name, delete_string.database_name, delete_string.branch_name,
                delete_string.table_name, delete_string.num_str)
    return "String was delete in Database and Elastic"