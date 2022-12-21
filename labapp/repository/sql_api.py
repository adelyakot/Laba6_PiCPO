from typing import List

from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""
# Вывод списка обработанных файлов с сортировкой по дате
def select_all_from_source_files(connector: StoreConnector):
    connector.start_transaction()  # начинаем выполнение запросов
    query = f'SELECT * FROM source_files ORDER BY processed '
    result = connector.execute(query).fetchall()
    connector.end_transaction()  # завершаем выполнение запросов
    return result

# Вставка в таблицу обработанных файлов
def insert_into_source_files(connector: StoreConnector, filename: str):
    now = datetime.now() # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")   # преобразуем в формат SQL
    connector.start_transaction()
    query = f'INSERT INTO source_files (filename, processed) VALUES (\'{filename}\', \'{date_time}\')'
    result = connector.execute(query)
    connector.end_transaction()
    return result


# Вставка строк из DataFrame в БД
def insert_rows_into_processed_data(connector: StoreConnector, dataframe: DataFrame, filename: str):
    """ Вставка строк из DataFrame в БД с привязкой данных к последнему обработанному файлу (по дате) """
    
    rows = dataframe.to_dict('records') 
    
    # получаем список обработанных файлов
    files_list = select_all_from_source_files(connector)
    last_file_id = files_list[0][0] 
    
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple, например:
    # row = (1, 'seeds_dataset.csv', '2022-11-15 22:03:16') ,
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
      # получаем индекс последней записи из таблицы с файлами
    #connector.start_transaction()
    if len(files_list) > 0:
        for row in rows:
            connector.start_transaction()
            query = f'INSERT INTO processed_data1 (genres,title_movie,production_countries,Release_year,Runtime,tagline,source_file) VALUES (\'{row["genres"]}\', \'{row["title_movie"]}\',\'{row["production_countries"]}\', \'{row["Release_year"]}\',\'{row["Runtime"]}\',\'{row["tagline"]}\', {last_file_id})'
            connector.execute(query)
        print('Data was inserted successfully')
        
    else:
        print('File records not found. Data inserting was canceled.')
    #connector.end_transaction()


    #( \'{row["genres"]}\', \'{row["title_movie"]}\'\'{row["production_countries"]}\', \'{row["Release_year"]}\'\'{row["Runtime"]}\'\'{row["tagline"]}\', {last_file_id})'        
    