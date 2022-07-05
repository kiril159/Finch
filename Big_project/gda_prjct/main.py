from fastapi import FastAPI
from gda import GitDatastoreApiProjects
from gda import GitDatastoreApiGroups
import models

app = FastAPI(title='GitDatastoreApi')


def gda_prjct_obj(token, database_name, project_name, group_name):
    gda = GitDatastoreApiProjects(token, database_name, group_name, project_name)  # "glpat-hNeknDFPsv_DRqb51HdB"
    return gda


def gda_grp_obj(token, group_name):
    gda = GitDatastoreApiGroups(token, group_name)  # "glpat-hNeknDFPsv_DRqb51HdB", 'test_group_for_projects'
    return gda


@app.post("/datastore-api/create/project")
def create_project(project: models.Project, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.create_project(project.project_name)
    return f"Project {project.project_name} has been created"


@app.post("/datastore-api/create/database")
def create_database(database: models.Database, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.create_database(database.database_name, database.project_name)
    return f"Database {database.database_name} has been created"


@app.post("/datastore-api/delete/project")
def delete_project(project: models.Project, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.project_delete(project.project_name)
    return f"Project {project.project_name} has been deleted"


@app.post("/datastore-api/delete/database")
def delete_database(database: models.Database, gda_gr: models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.database_delete(database.project_name, database.database_name)
    return f"Database {database.database_name} has been deleted"


@app.post("/datastore-api/create/branch")
def create_branch(branch: models.Branch, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.create_branch(branch.branch_name)
    return f"Branch {branch.branch_name} has been created"


@app.post("/datastore-api/create/table")
def create_table(table: models.CreateTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.create_table(table.table_name, table.config, table.branch_name, table.author_name, table.author_email)
    return f"Table {table.table_name} has been created"


@app.post("/datastore-api/update")
def update_table(table: models.UpdateTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.update_table(table.table_name, table.row_to_update, table.new_row, table.branch_name)
    return f"Row has been updated"


@app.post("/datastore-api/read/")
def read_table(table: models.ReadTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    return gda.read_table(table.table_name, table.branch_name, stop=table.stop, start=table.start,
                          sort_by=table.sort_by, column_name=table.column_name)


@app.post("/datastore-api/delete/branch")
def delete_branch(branch: models.Branch, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.branch_delete(branch.branch_name)
    return f"Branch {branch.branch_name} has been deleted"


@app.post("/datastore-api/delete/table")
def delete_table(table: models.DelTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.table_delete(table.table_name, table.branch_name)
    return f"Table {table.table_name} has been deleted"


@app.post("/datastore-api/upload_on_elastic")
async def upload_on_elastic(elast_git: models.WorkWithTable, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.git_elastic(gda_prjct.project_name, gda_prjct.database_name, elast_git.branch_name, elast_git.table_name)
    return f'{gda_prjct.group_name}-{gda_prjct.project_name}-{gda_prjct.database_name}-{elast_git.branch_name}-{elast_git.table_name}'.lower() + ' was insert in elastic'



@app.post("/datastore-api/delete_str")
async def delete_str(delete_string: models.DeleteString, gda_prjct: models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.token, gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.del_str(gda_prjct.group_name, delete_string.project_name, delete_string.database_name, delete_string.branch_name,
                delete_string.table_name, delete_string.num_str)
    return "String was delete in Database and Elastic"
