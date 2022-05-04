import datetime
from dagster import get_dagster_logger, job, op


@op
def check():
    today = datetime.datetime.today()
    return today.strftime("%H:%M")


def even(time_now):
    get_dagster_logger().info(f'Сейчас четная минута. Время: {time_now}')


def not_even(time_now):
    get_dagster_logger().info(f'Сейчас нечетная минута. Время: {time_now}')


@op
def trying_t(time_now):
    if int(time_now[-2:]) % 2 == 0:
        even(time_now)
    else:
        not_even(time_now)


@job
def time_job():
    ch = check()
    trying_t(ch)
