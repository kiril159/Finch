def multiproc(way, sep, columns):
    import csv
    import multiprocessing
    import json

    way = way
    sep = sep
    columns = columns
    d = {}
    global process
    global output

    def split_columns_in_csv(way_f, sep_f, columns_f):
        data = []
        with open(way_f, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=sep_f)
            for row in reader:
                data.append(int(row[columns_f]))
            l1, l2 = divmod(len(data), 10)
            r = [l1 + 1] * l2 + [l1] * (10 - l2)
            res = []
            last = 0
            for m in r:
                res.append(data[last:last + m])
                last += m
        return res

    def summ(list_f):
        s = 0
        for el in list_f:
            s += el
        return s

    def process(read_f, write_f, write_p_f):
        name_proc = multiprocessing.current_process().name
        x = read_f.get()
        res = summ(x)
        # print(name_proc, res) #для проверки соответсвиям данных в файле .csv
        write_f.put(res)
        write_p_f.put(name_proc)

    def output(dict_f):
        with open('output.csv', 'w') as f:
            json.dump(dict_f, f)

    k = split_columns_in_csv(way, sep, columns)

    if __name__ == '__main__':
        processes = []
        write = multiprocessing.Queue()
        write_p = multiprocessing.Queue()
        read = multiprocessing.Queue()
        [read.put(el) for el in k]
        for i in range(1, 11):
            p = multiprocessing.Process(target=process, args=(read, write, write_p,))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
        for a in range(10):
            d[write_p.get(a)] = write.get(a)
        p_output = multiprocessing.Process(target=output, args=(d,))
        p_output.start()
        p_output.join()

multiproc('input.csv', ',', 'num2')

