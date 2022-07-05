def filter_and_sort(lst,filter_by,sort_by, column_to_filter,column_to_sort)
    if filter_by is not None:
        # funct
        if sort_by is not None:
            sorted_lst = sorted(lst, key=itemgetter(column_to_sort))
            if sort_by == 'grow':
                return sorted_lst
            elif sort_by == 'not_grow':
                return sorted_lst[::-1]
            else:
                raise HTTPException(404, "Неизвестный тип сортировки")
            return sorted_lst


def filter(lst,filter_by,column_to_filter):
    pass
def sort(lst,sort_by,column_to_sort):
    sorted_lst = sorted(lst, key=itemgetter(column_to_sort))
    if sort_by == 'grow':
        return sorted_lst
    elif sort_by == 'not_grow':
        return sorted_lst[::-1]
    else:
        raise HTTPException(404, "Неизвестный тип сортировки")