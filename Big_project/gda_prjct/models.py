from pydantic import BaseModel, Field


class GdaGrpObj(BaseModel):
    token: str
    group_name: str


class Database(BaseModel):
    database_name: str
    project_name: str


class Project(BaseModel):
    project_name: str


class Branch(BaseModel):
    branch_name: str


class CreateTable(BaseModel):
    table_name: str
    config: dict
    branch_name: str
    author_name: str
    author_email: str


class UpdateTable(BaseModel):
    table_name: str
    row_to_update: int
    new_row: dict
    branch_name: str


class ReadTable(BaseModel):
    table_name: str
    branch_name: str
    stop: int = None
    start: int = None
    column_name: str
    sort_by: str = Field(default="", title="asc or desc")


class DelTable(BaseModel):
    table_name: str
    branch_name: str


class GitElast(BaseModel):
    branch_name: str
    table_name: str


class DeleteString(BaseModel):
    project_name: str
    database_name: str
    branch_name: str
    table_name: str
    num_str: int


class WorkWithTable(BaseModel):
    table_name: str
    branch_name: str


class GdaPrjctObj(BaseModel):
    token: str
    database_name: str
    project_name: str
    group_name: str
