from Task_1.my_file import My_file

p = My_file('../territory_2022.csv', 'write')
p.replace('#', ',')

p_3 = My_file('../test_f.txt', 'read')
p_3.replace('#', ',')