from fastapi import FastAPI
from test import GitDatastoreApiProjects
from test import GitDatastoreApiGroups
import test_models


app = FastAPI(title='GitDatastoreApi')


def gda_prjct_obj(database_name, project_name, group_name):
    gda = GitDatastoreApiProjects("glpat-vwyCwi5hzpvsy42bAPcd", database_name, group_name, project_name)
    return gda


def gda_grp_obj(token, group_name):
    gda = GitDatastoreApiGroups(token, group_name)  #'glpat-vwyCwi5hzpvsy42bAPcd', 'test_group'
    return gda


@app.post("/datastore-api/create/project")
def create_project(project: test_models.Project, gda_gr: test_models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.create_project(project.project_name)
    return f"Project {project.project_name} has been created"


@app.post("/datastore-api/create/database")
def create_database(database: test_models.Database, gda_gr: test_models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.create_database(database.database_name, database.project_name)
    return f"Database {database.database_name} has been created"


@app.post("/datastore-api/delete/project")
def delete_project(project: test_models.Project, gda_gr: test_models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.project_delete(project.project_name)
    return f"Project {project.project_name} has been deleted"


@app.post("/datastore-api/delete/database")
def delete_database(database: test_models.Database, gda_gr: test_models.GdaGrpObj):
    gda = gda_grp_obj(gda_gr.token, gda_gr.group_name)
    gda.database_delete(database.project_name, database.database_name)
    return f"Database {database.database_name} has been deleted"


@app.post("/datastore-api/create/branch")
def create_branch(branch: test_models.Branch, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.create_branch(branch.branch_name)
    return f"Branch {branch.branch_name} has been created"


@app.post("/datastore-api/create/table")
def create_table(table: test_models.CreateTable, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.create_table(table.table_name, table.config, table.branch_name, table.author_name, table.author_email)
    return f"Table {table.table_name} has been created"


@app.post("/datastore-api/update")
def update_table(table: test_models.UpdateTable, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.update_table(table.table_name, table.row_to_update, table.new_row, table.branch_name)
    return f"Row has been updated"


@app.post("/datastore-api/read/")
def read_table(table: test_models.ReadTable, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    return gda.read_table(table.table_name, table.branch_name, stop=table.stop, start=table.start,
                          sort_by=table.sort_by,column_to_sort=table.column_to_sort)


@app.post("/datastore-api/delete/branch")
def delete_branch(branch: test_models.Branch, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.branch_delete(branch.branch_name)
    return f"Branch {branch.branch_name} has been deleted"


@app.post("/datastore-api/delete/table")
def delete_table(table: test_models.DelTable, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.table_delete(table.table_name, table.branch_name)
    return f"Table {table.table_name} has been deleted"


@app.post("/datastore-api/upload_on_elastic")
def upload_on_elastic(elast_git: test_models.GitElast, gda_prjct: test_models.GdaPrjctObj):
    gda = gda_prjct_obj(gda_prjct.database_name, gda_prjct.project_name, gda_prjct.group_name)
    gda.git_elast(gda_prjct.database_name, gda_prjct.project_name, elast_git.branch_name, elast_git.table_name)
    return "Database was insert in Elastic"
