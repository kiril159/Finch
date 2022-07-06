from dagster import repository

from .jobs.say_hello import updating_job1
from .sensors.my_sensor import my_sensor


@repository
def final_finch():
    jobs = [updating_job1]
    sensors = [my_sensor]

    return jobs + sensors
