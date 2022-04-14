from Task_1.my_file import My_file

p = My_file('../territory_2022.csv', 'write')
p.copy('file_copy.csv')
p.write_line('hello')
p.replace('#', ';')
p.print(';')
p.print_first_line()
