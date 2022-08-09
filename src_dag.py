from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
#path нужно сконфигурировать, дабы имелся доступ к модулю-обработчику
sys.path.append('/home/projects/airflow_sandbox/src_reader')
from src_reader import *

def task_failure_alert(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")

default_args = {
    "owner":"airflow",
    "description":"выгрузка логов ТМК бота",
    "depends_on_past":False,
    "start_date":datetime(2022, 8, 6),
    "email_on_failure":['vins90nuar@gmail.com'],
    "email_on_retry": True,
    'retries': 5,
    'max_retry_delay':timedelta(seconds=10)

}


with DAG("tmk_logs",
        schedule_interval= timedelta(days=30),
        catchup=False,
        template_searchpath='/opt/airflow',
        default_args=default_args

) as dags:
    
    def do_report():
        #проверяем текущий месяц
        a_date = CheckDate()

        #проверяем директорию на соответствие известным форматам и текущему месяцу
        #у DocFinder имеется параметр path_dir:str, по умолчанию './'
        #в случае если целевая директория с файлами находится в другом каталоге,
        #необходимо задать соответствующий абсолютный/относительный путь
        files = DocFinder(a_date.date)
        #получение данных из N файлов за текущий месяц
        files.get_files()

        #инициация обработчика
        pipeline = Pipe(files)
        #оборачивание данных в ДФ, чистка, подсчёт итога
        pipeline.to_df()
        pipeline.dfs
        pipeline.to_clean()
        pipeline.to_calculate()

        #свод
        pipeline.unite()
        #сохранение отчёта 
        pipeline.save()

    
    t1 = PythonOperator(task_id="do_report", python_callable=do_report)

    t1