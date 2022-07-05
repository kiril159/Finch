from pydantic import BaseModel


class Project(BaseModel):
    project_name: str


class CreateProject(BaseModel):
    token: str
    project_name: str


class CreateDatabase(BaseModel):
    database_name: str


class CreateTable(BaseModel):
    table_name: str
    config: dict
    database_name: str


class UpdateTable(BaseModel):
    table_name: str
    row_to_update: int
    new_row: dict
    database_name: str


class ReadTable(BaseModel):
    table_name: str
    database_name: str
    stop: int = None
    start: int = None


class DelProject(BaseModel):
    project_id: str


class DelDatabase(BaseModel):
    database_name: str


class DelTable(BaseModel):
    table_name: str
    database_name: str
