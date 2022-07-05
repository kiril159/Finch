from fastapi import FastAPI
from test import GitDatastoreApiProjects
from test import GitDatastoreApiGroups
import test_models


app = FastAPI(title='GitDatastoreApi')


def gda_prjct_obj(database_name, project_name, group_name):
    gda = GitDatastoreApiProjects("glpat-vwyCwi5hzpvsy42bAPcd", database_name, "kiril159", 'kirill200118@gmail.com',
                                  group_name, project_name)
    return gda


def gda_grp_obj():
    gda = GitDatastoreApiGroups("glpat-hNeknDFPsv_DRqb51HdB", '53763296')
    return gda


@app.post("/datastore-api/create/project")
def create_project(project: test_models.Project):
    gda = gda_grp_obj()
    gda.create_project(project.project_name)
    return f"Project {project.project_name} has been created"


@app.post("/datastore-api/create/database")
def create_database(database: test_models.CreateDatabase):
    gda = gda_grp_obj()
    gda.create_database(database.database_name, database.project_name)
    return f"Database {database.database_name} has been created"


'''@app.post("/datastore-api/create/branch")
def create_branch(branch: test_models.CreateBranch, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.create_branch(branch.branch_name)
    return f"Branch {branch.branch_name} has been created"'''


@app.post("/datastore-api/create/table")
def create_table(table: test_models.CreateTable, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.create_table(table.table_name, table.config, table.branch_name)
    return f"Table {table.table_name} has been created"


@app.post("/datastore-api/insert")
def insert_table(table: test_models.UpdateTable, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.insert_on_table(table.table_name, table.new_row, table.branch_name)
    return f"Row has been inserted"


@app.post("/datastore-api/update")
def update_table(table: test_models.UpdateTable, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.update_table(table.table_name, table.row_to_update, table.new_row, table.branch_name)
    return f"Row has been updated"


@app.post("/datastore-api/read/")
def read_table(table: test_models.ReadTable, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    return gda.read_table(table.table_name, table.branch_name, stop=table.stop, start=table.start)


@app.post("/datastore-api/delete/database")
def delete_database(database: test_models.DelDatabase, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.database_delete(database.project_id)
    return f"Database {database_name.database_name} has been deleted"


@app.post("/datastore-api/delete/branch")
def delete_branch(branch: test_models.DelBranch, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.branch_delete(branch.branch_name)
    return f"Branch {branch.branch_name} has been deleted"


@app.post("/datastore-api/delete/table")
def delete_table(table: test_models.DelTable, database_name: test_models.Database):
    gda = gda_prjct_obj(database_name.database_name, database_name.project_name, database_name.group_name)
    gda.table_delete(table.table_name, table.branch_name)
    return f"Table {table.table_name} has been deleted"


@app.post("/datastore-api/delete/project")
def delete_project(project: test_models.Project):
    gda = gda_grp_obj()
    gda.project_delete(project.project_name)
    return f"Project {project.project_name} has been deleted"
