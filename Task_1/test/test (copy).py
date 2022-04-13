from Task_1.my_file import My_file

p = My_file('../territory_2022.csv', 'write')
p.copy('../test_copy.csv')
p.copy('test_2.csv')

p_3 = My_file('../test_f.txt', 'read')
p_3.copy('../test_copy.txt')
p_3.copy('test_2.txt')