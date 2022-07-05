from dagster import RunRequest, sensor
import gspread
from final.final_task.jobs.say_hello import updating_job


@sensor(job=updating_job)
def my_sensor():
    gc = gspread.service_account(filename='/Users/kirill/Desktop/Работа/final/final_task/credentials.json')
    sh = gc.open('Главный файл1')
    name_sheet = sh.worksheets()[-1].title
    if name_sheet:
        yield RunRequest(run_key=sh.worksheets()[-1].title, run_config={})
