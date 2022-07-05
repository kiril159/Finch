from pydantic import BaseModel


class Project(BaseModel):
    project_name: str


class Database(BaseModel):
    database_name: str
    project_name: str
    group_name: str


class CreateDatabase(BaseModel):
    database_name: str
    project_name: str
    group_name: str


class DelDatabase(BaseModel):
    database_id: str


'''class CreateBranch(BaseModel):
    branch_name: str


class DelBranch(BaseModel):
    branch_name: str'''


class CreateTable(BaseModel):
    table_name: str
    config: dict
    project_name: str
    group_name: str


class UpdateTable(BaseModel):
    table_name: str
    row_to_update: int
    new_row: dict
    group_name: str


class ReadTable(BaseModel):
    table_name: str
    branch_name: str
    stop: int = None
    start: int = None


class DelTable(BaseModel):
    table_name: str
    branch_name: str
