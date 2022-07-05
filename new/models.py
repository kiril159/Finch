from pydantic import BaseModel, Field


class GdaGrpObj(BaseModel):
    token: str
    group_name: str


class GdaPrjctObj(BaseModel):
    token: str
    database_name: str
    project_name: str
    group_name: str


class Database(BaseModel):
    database_name: str
    project_name: str


class Project(BaseModel):
    project_name: str


class Branch(BaseModel):
    branch_name: str


class CreateTable(BaseModel):
    config: dict
    author_name: str
    author_email: str


class UpdateTable(BaseModel):
    row_to_update: int
    new_row: dict


class ReadTable(BaseModel):
    stop: int = None
    start: int = None
    column_name: str = ""
    sort_by: str = Field(default="", title="asc or desc")


class WorkWithTable(BaseModel):
    table_name: str
    branch_name: str


class DeleteString(BaseModel):
    project_name: str
    database_name: str
    branch_name: str
    table_name: str
    num_str: int
