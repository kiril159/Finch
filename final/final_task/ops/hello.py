from dagster import op
from final.final_task.ClassPS import PythonSheets


@op
def updating():
    t1 = PythonSheets()
    t2 = t1.copy_info()
    t1.paste_info(t2)

