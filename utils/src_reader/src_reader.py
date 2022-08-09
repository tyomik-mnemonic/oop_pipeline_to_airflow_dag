from typing import Dict, List
from abc import ABC
import chardet
import json
import pandas as pd
from datetime import datetime
import glob, os

class AbcFileParser(ABC):
    """
    path - путь к директории c источниками
    struc - ключи и их индекс
    """
    def __init__(self, path:str,  enc:str, struc:Dict[int,str]):
        pass
    
    def read(self):
        pass
    
    def to_df(self):
        pass
    
    #удаляем лишние столбцы. dict объект в перспективе может давать информацию о индексе столбца
    def cut_df(self):
        for i in self.df.columns:
            if i not in self.struc.values():
                self.df = self.df.drop([i], axis=1)
            else:
                pass
            
    def calc_total(self, price_column:str='Цена, ед.', quantity_column:str='Кол-во, шт'):
        self.df['total_count'] = a.df[price_column] * a.df[quantity_column]
    
    
class JsonFileParser(AbcFileParser):
    def __init__(self, path:str,  enc:str, struc: Dict[int,str]):
        super().__init__(path=path, enc=enc, struc=struc)
        self.path = path
        self.enc = enc
        self.struc = struc
        self.hub_name = path.replace('./','').replace(' ','')
  
    def read(self):
        with open(self.path, 'r', encoding=self.enc) as file:
            self.data = json.loads(file.read())
            
    def to_df(self):
        self.read()
        self.df = pd.read_json(json.dumps(self.data))
        
class CsvFileParser(AbcFileParser):
    def __init__(self, path:str,  enc:str, struc: Dict[int,str]):
        super().__init__(path=path, enc=enc, struc=struc)
        self.path = path
        self.enc = enc
        self.struc = struc
        self.hub_name = path.replace('./','').replace(' ','')

    def to_df(self):
        self.df = pd.read_csv(self.path)


class XlsxFileParser(AbcFileParser):
    def __init__(self, path:str,  enc:str, struc: Dict[int,str], sheet_name:str='Sheet1', engine:str='openpyxl'):
        super().__init__(path=path, enc=enc, struc=struc)
        self.path = path
        self.enc = enc
        self.struc = struc
        self.sheet_name = sheet_name
        self.engine = engine
        self.hub_name = path.replace('./','').replace(' ','')

    def to_df(self):
        self.df = pd.read_excel(self.path, sheet_name=self.sheet_name, engine=self.engine)



class Counter:
    def __init__(self, objs:List[AbcFileParser]):
        self.objs = objs

    def create(self):
        data = []
        for df in self.objs:
            count = df.df['total_count'].sum()
            quantity = df.df['Кол-во, шт'].sum()
            data.append((df.hub_name, quantity, count))
        self.data = data
    
    def to_xlsx(self, path:str='./', sheet_name:str='repopt') -> None:
        df_data = {}
        df_data['Склад'] = [i[0] for i in self.data]
        df_data['Кол-во, шт'] = [i[1] for i in self.data]
        df_data['Стоимость, руб.'] = [i[2] for i in self.data]
        
        new_df = pd.DataFrame(data = df_data)
        new_df.to_excel("report.xlsx",
             sheet_name=sheet_name)
        return None


class CheckDate:
    
    def __init__(self):
        date_map = {
            1:'январь', 2:'февраль', 3:'март', 4:'апрель',
            5:'май', 6:'июнь', 7:'июль', 8:'август',
            9:'сентябрь', 10:'октябрь', 11:'ноябрь', 12:'декабрь'
        }
        self.date = date_map.get(datetime.now().month)


class DocFinder:
    
    def __init__(self, a_date:str, path_dir:str = './'):
        self.a_date = a_date
        self.cur_dir = os.getcwd()
        self.path_dir = path_dir
        self.csv = []
        self.json = []
        self.xlsx = []
        
    def get_files(self):
        os.chdir(self.path_dir)
        for file in glob.glob("*.csv"):
            if file.find(self.a_date) != -1:
                self.csv.append(file)
                
        for file in glob.glob("*.json"):
            if file.find(self.a_date) != -1:
                self.json.append(file)
                
        for file in glob.glob("*.xlsx"):
            if file.find(self.a_date) != -1:
                self.xlsx.append(file)


class Pipe:
    
    def __init__(self, srcs:DocFinder, strucs: Dict[int,Dict[int,str]] = None):
        #strucs может использоваться для конфигурирования FileParser
        #однаком имхо выходит за рамки тестовой работы
        #в реальности оговаривается и описывается в доке, арх. данных и постановках
        self.srcs = srcs
        self.dfs = []
        self.strucs = strucs
    
    def to_df(self):
        for i in self.srcs.csv:
            #TODO: в сервисах корректнее использовать для FileParser'ов- фабрику
            #      реализовать FileParserFabric
            try:
                self.dfs.append(CsvFileParser(i,
                  'utf-8',
                  {1:'Код товара 2',
                    2:'Товар',
                    3:'Цена, ед.',
                    4:'Кол-во, шт',
                    5:'Времени до окончания срока годности, %',
                    6:'Отпустил товар'
                   }
                 )
            )
            except Exception as ex:
                print(ex)
        for i in self.srcs.json:
            try:
                self.dfs.append(JsonFileParser(i,
                  'utf-8',
                  {1:'Код товара 2',
                    2:'Товар',
                    3:'Цена, ед.',
                    4:'Кол-во, шт',
                    5:'Времени до окончания срока годности, %',
                    6:'Отпустил товар'
                   }
                 )
            )
            except Exception as ex:
                print(ex)
                
        for i in self.srcs.xlsx:
            try:
                self.dfs.append(XlsxFileParser(i,
                  'utf-8',
                  {1:'Код товара 1',
                    2:'Товар',
                    3:'Цена, ед.',
                    4:'Кол-во, шт',
                    5:'Времени до окончания срока годности, %',
                    6:'Отпустил товар'
                   }
                 )
            )
            except Exception as ex:
                print(ex)
                
        for i in self.dfs:
            i.to_df()
        
    def to_clean(self):
        for i in self.dfs:
            i.cut_df()
        
    def to_calculate(self):
        for i in self.dfs:
            i.calc_total()
                
    def unite(self):
        self.united = Counter(self.dfs)
        self.united.create()
            
    def save(self):
        self.united.to_xlsx()



