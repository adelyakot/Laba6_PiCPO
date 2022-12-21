from abc import ABC, abstractmethod     # подключаем инструменты для создания абстрактных классов
import pandas   # библиотека для работы с датасетами
import os
import numpy as np

"""
    В данном модуле реализуются классы обработчиков для 
    применения алгоритма обработки к различным типам файлов (csv или txt).
    
    ВАЖНО! Если реализация различных обработчиков занимает большое 
    количество строк, то необходимо оформлять каждый класс в отдельном файле
"""

# Родительский класс для обработчиков файлов
class DataProcessor(ABC):
    def __init__(self, datasource):
        # общие атрибуты для классов обработчиков данных
        self._datasource = datasource   # путь к источнику данных
        self._dataset = None            # входной набор данных
        self.result = None              # выходной набор данных (результат обработки)

    # Метод, инициализирующий источник данных
    # Все методы, помеченные декоратором @abstractmethod, ОБЯЗАТЕЛЬНЫ для переобределения
    @abstractmethod
    def read(self) -> bool:
        pass

    # Точка запуска методов обработки данных
    @abstractmethod
    def run(self):
        pass

    """
        Пример одного из общих методов обработки данных.
        В данном случае метод просто сортирует входной датасет по значению заданной колонки (аргумент col)
        
        ВАЖНО! Следует логически разделять методы обработки, например, отдельный метод для сортировки, 
        отдельный метод для удаления "пустот" в датасете и т.д. Это позволит гибко применять необходимые
        методы при переопределении метода run для того или иного типа обработчика.
        НАПРИМЕР, если ваш источник данных это не файл, а база данных, тогда метод сортировки будет не нужен,
        т.к. сортировку можно сделать при выполнении SQL-запроса типа SELECT ... ORDER BY...
   
       """
       
    #def sort_data_by_col(self, df, colname, asc) -> pandas.DataFrame:
     # list = df.sort_values(by=[colname], ascending=asc)
      #list_params = []
      #for n in range (len.list):
       #   list.loc[dataset_copy[list[i][5]] == 2009]
        #  list_params.append(dataset_copy)
      #return df.sort_values(by=[colname], ascending=asc)
                 
    def sort_data_by_col(self, df: pandas.DataFrame, colname: str, asc: bool) -> pandas.DataFrame:
        return df.sort_values(by=[colname], ascending=asc)
      


    def get_mean_value_by_filter(self, df: pandas.DataFrame, filter_expr: str) -> pandas.DataFrame:
        result = df.query(filter_expr)
        return result.mean(axis=0, skipna=True).to_frame().T

    # Абстрактный метод для вывоа результата на экран
    @abstractmethod
    def print_result(self):
        pass


# Реализация класса-обработчика csv-файлов
class CsvDataProcessor(DataProcessor):
    # Переобпределяем конструктор родительского класса
    def __init__(self, datasource):
        DataProcessor.__init__(self, datasource)    # инициализируем конструктор родительского класса для получения общих атрибутов
        self.separator = ','        # дополнительный атрибут - сепаратор по умолчанию
    """
        Переопределяем метод инициализации источника данных.
        Т.к. данный класс предназначен для чтения CSV-файлов, то используем метод read_csv
        из библиотеки pandas
    """

    
    def read(self):
        try:
            self._dataset = pandas.read_csv(self._datasource, sep=self.separator, header='infer', names=None, encoding="utf-8")
            
            # Читаем имена колонок из файла данных
            col_names = self._dataset.columns
            # Если количество считанных колонок < 2 возвращаем false
            if len(col_names) < 2:
                return False
            return True
        except Exception as e:
            print(str(e))
            return False

    #    Сортируем данные по значениям колонки "LKG" и сохраняем результат в атрибуте result
    # Метод для удаления выбросов
    def emossions (self):
        col_namess = self._dataset.columns
        for i in range(len(col_namess)):
            self._dataset [col_namess[i]].replace('', np.nan, inplace=True)
            self._dataset.dropna(subset=[col_namess[i]], inplace=True)


    # Метод для создания колонки для разбития на категории
    def cathegories(self, new_colname: str,categ_colname: str):
        self._dataset [new_colname] = pandas.cut(self._dataset[categ_colname],[1995,2000,2005,2016], labels = [1,2,3])
        self.result = self.sort_data_by_col(self._dataset, categ_colname, True)



    # Метод для создания выборки с определенным значением атрибута
    def set_param (self, colname: str, value):
        return self._dataset[self._dataset[colname] == value]

    """
    def check_values_arr(self, arr_values: list, colname_check):
        #self._dataset[self._dataset[colname]
        dataset_filtered = self._dataset#.copy()
        tmp = []
        for i in range (1, len(arr_values)):   
            #for j in range (2,dataset_filtered.shape[0]):
            for j in (1,len(dataset_filtered)):
                if  arr_values[i] in dataset_filtered[colname_check].loc[dataset_filtered.index[j]]:
                #if  arr_values[i] in dataset_filtered.loc[catheg_colname,j]: #data.iloc[[0,1,3],[0]]
                #if  arr_values[i] in dataset_filtered.loc[dataset_filtered[j][catheg_colname]]:
                    tmp.append(dataset_filtered.iloc[j]) # dataset_copy.loc[dataset_copy[list[i][0]]
                    dataset_filtered = tmp
        self.result = dataset_filtered
        print (f'Filtered CSV :\n', self.result)
     """
  
    def run(self):
        
        col_namess = self._dataset.columns
        # Сортируем данные по значениям колонки "LKG" и сохраняем результат в атрибуте result
        # self.result = self.sort_data_by_col(self._dataset, "selling_price", True)

        # удаляем выбросы
        self.emossions()
        
        # создаем колонку для разбития на категории
        new_colname = "period_cathegory"
        catheg_colname = "Release_year"
        self.cathegories(new_colname,catheg_colname)

        print (self.set_param("Release_year", 2009))

        # делаем выборку по значению параметра
        #self.check_values_arr(["Action","Adventure"], 0 )

    def print_result(self):
       print(f'Running CSV-file processor!\n', self.result)










# Реализация класса-обработчика txt-файлов
class TxtDataProcessor(DataProcessor):
    # Реализация метода для чтения TXT-файла
    def read(self):
        try:
            self._dataset = pandas.read_table(self._datasource, sep='\s+', engine='python')
            col_names = self._dataset.columns
            if len(col_names) < 2:
                return False
            return True
        except Exception as e:
            print(str(e))
            return False

    def run(self):
        self.result = self.sort_data_by_col(self._dataset, "Production countries", True)

    def print_result(self):
        print(f'Running TXT-file processor!\n', self.result)