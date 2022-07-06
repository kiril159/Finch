from dagster import op, job
import gspread

class PythonSheets:
    def __init__(self):
        self.gc = gspread.service_account(filename='credentials.json')
        self.sh_orig = self.gc.open('Главный файл1')
        self.sh_copy = self.gc.open('Таблица для копирования')
        self.name_sheet = self.sh_orig.worksheets()[-2].title
        self.sh_copy.add_worksheet(title=self.name_sheet, rows=100, cols=100)
        self.worksheet_orig = self.sh_orig.worksheet(self.name_sheet)
        self.worksheet_copy = self.sh_copy.worksheet(self.name_sheet)

    def copy_info(self):
        list_of_lists = self.worksheet_orig.batch_get(['A1:A42', 'M1:AB42'])
        return list_of_lists

    def paste_info(self, list_of_lists):
        self.worksheet_copy.batch_update([{
            'range': 'A1:A42',
            'values': list_of_lists[0],
        }, {
            'range': 'B1:Q42',
            'values': list_of_lists[1],
        }])

'''t1 = PythonSheets()
t2 = t1.copy_info()
t1.paste_info(t2)'''


@op
def updating():
    t1 = PythonSheets()
    t2 = t1.copy_info()
    t1.paste_info(t2)

@job
def updating_job1():
    updating()
