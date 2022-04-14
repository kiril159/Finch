import csv


class My_file:
    def __init__(self, way, action):
        try:
            self.way = way
            self.action = action
            f = open(self.way, 'r')
            f.close()
            if not (self.action == 'read' or self.action == 'write'):
                raise ValueError("Incorrect action")
        except:
            raise FileNotFoundError("File not found at this path")

    def copy(self, way_to_copy):
        if not (self.action == 'write'):
            raise ValueError("Incorrect action")
        with open(self.way, 'r', encoding='utf-8') as origin_f:
            origin_text = origin_f.read()
            with open(way_to_copy, 'w', encoding='utf-8') as copy_f:
                for line in origin_text:
                    copy_f.writelines(line)

    def write_line(self, str_to_file):
        if self.action == 'write':
            with open(self.way, 'a') as f:
                f.writelines(str_to_file + "\n")
        else:
            raise ValueError("This method only for 'write'")

    def print(self, sep=','):
        if ".csv" in self.way:
            with open(self.way, encoding='utf-8') as f_1:
                csv_reader = csv.DictReader(f_1, delimiter=sep)
            for row in csv_reader:
                print(row)
        else:
            raise FileExistsError("Only for '.csv'")

    def replace(self, sep_old, sep_new):
        if (self.action == "write") and (".csv" in self.way):
            reader_f = list(csv.reader(open(self.way, 'r', encoding='utf-8'), delimiter=sep_old))
            with open(self.way, 'w', encoding='utf-8') as f:
                writer_f = csv.writer(f, delimiter=sep_new, lineterminator="\r")
                for row in reader_f:
                    writer_f.writerow(row)
        else:
            raise ValueError("Incorrect action or file extension")
