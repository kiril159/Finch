import datetime


def decorator_f(funct):
    def wrapper(*args, **kwargs):
        today = datetime.datetime.today()
        funct(*args, **kwargs)
        p = funct(*args, **kwargs)
        with open('log.txt', 'a', encoding='utf-8') as log:
            if p is None:
                log.writelines(
                    f'Название функции:{funct.__name__}, время начала исполнения:{today.strftime("%Y-%m-%d-%H.%M.%S")}'
                    + '\n')
            else:
                log.writelines(
                    f'Название функции:{funct.__name__}, время начала исполнения:{today.strftime("%Y-%m-%d-%H.%M.%S")},'
                    f' функция возвращает:{p} ' + '\n')

    return wrapper


