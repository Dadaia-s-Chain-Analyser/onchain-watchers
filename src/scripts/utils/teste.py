from functools import reduce


dict1 = [
    ('marco','gisele'),
    ('gisele','denise')
]

dict2 = [
    ('marco','gisele'),
    ('marco','pedro'),
    ('gisele','denise'),
    ('pedro','denise'),
]

dict3 = [
    ('marco','gisele'),
    ('gisele','pedro'),
    ('pedro','denise'),
    ('marco','leo'),
    ('leo','willian'),
    ('willian','denise'),
]

dict4 = [
    ('marco','gisele'),
    ('gisele','matheus'),
    ('matheus','denise')]


dict5 = [
    ('marco','gisele'),
    ('gisele','matheus'),
    ('matheus','denise'),
    ('andre','arnaldo'),
    ('marco','antonio'),
    ('antonio','savio'),
    ('gisele','matheus'),
    ('matheus', 'denise')]


def find_connection(list_pairs, wished_swap):
    if wished_swap in list_pairs:
        print("achou na primeira interação")
    else:
        print(list_pairs)
        print(f"Search: {wished_swap}")
        first_filter = [i for i in list_pairs if wished_swap[0] in i or wished_swap[1] in i]
        print(first_filter)
        derretido = reduce(lambda a, b: a + b, [list(i) for i in first_filter])
        print(derretido)
        derretido_filtrado = [i for i in derretido if i not in wished_swap]
        print(derretido_filtrado)
        conn = [i for i in derretido_filtrado if derretido_filtrado.count(i) == 2]
        conn2 = set(conn)
        print("hello")
        print(conn2)
        if len(conn2) > 0:
            for i in conn:
                wished_swap2 = wished_swap[:]
                wished_swap2[1:1] = [i]
                print(wished_swap2)
        else:
            pass

if __name__ == '__main__':
    print("="*50)

    lista = [1,2,3,5]
    lista2 = lista[:]
    lista[3:3] = [4]
    print(lista)
    find_connection(dict1, ['marco', 'denise'])
    print("="*50)
    find_connection(dict2, ['marco', 'denise'])
    print("="*50)
    find_connection(dict3, ['marco', 'denise'])
    print("="*50)
    find_connection(dict4, ['marco', 'denise'])

