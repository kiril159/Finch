from dagster import repository

from final_task.jobs.say_hello import say_hello_job
from final_task.schedules.my_hourly_schedule import my_hourly_schedule
from final_task.sensors.my_sensor import my_sensor


@repository
def final_task():
    """
    The repository definition for this final_task Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors
