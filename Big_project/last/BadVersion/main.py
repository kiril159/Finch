from fastapi import FastAPI
from gda import GitDatastoreApi
from gda import create_project as crt_prjct
import models


app = FastAPI(title='GitDatastoreApi')


def gda_obj(project_name):
    gda = GitDatastoreApi("glpat-vwyCwi5hzpvsy42bAPcd", 'kiril159', project_name, "kiril159", 'kirill200118@gmail.com')
    return gda


@app.post("/datastore-api/create/project")
def create_project(project: models.CreateProject):
    crt_prjct(project.token, project.project_name)
    return f"Project {project.project_name} has been created"


@app.post("/datastore-api/create/database")
def create_database(database: models.CreateDatabase, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    gda.create_database(database.database_name)
    return f"Database {database.database_name} has been created"

@app.post("/datastore-api/create/table")
def create_table(table: models.CreateTable, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    gda.create_table(table.table_name, table.config, table.database_name)
    return f"Table {table.table_name} has been created"

@app.post("/datastore-api/update")
def update_table(table: models.UpdateTable, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    gda.update_table(table.table_name, table.row_to_update, table.new_row, table.database_name)
    #return f"Table {table.table_name} has been updated"
    return gda.update_table(table.table_name, table.row_to_update, table.new_row, table.database_name)


@app.post("/datastore-api/read/")
def read_table(table: models.ReadTable, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    return gda.read_table(table.table_name, table.database_name, stop=table.stop, start=table.start)


@app.post("/datastore-api/delete/project")
def delete_project(project: models.DelProject, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    gda.project_delete(project.project_id)
    return f"Project {project_name.project_name} has been deleted"


@app.post("/datastore-api/delete/database")
def delete_database(database: models.DelDatabase, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    gda.database_delete(database.database_name)
    return f"Database {database.database_name} has been deleted"


@app.post("/datastore-api/delete/table")
def delete_table(table:models.DelTable, project_name: models.Project):
    gda = gda_obj(project_name.project_name)
    gda.table_delete(table.table_name, table.database_name)
    return f"Table {table.table_name} has been deleted"

