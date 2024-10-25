# BI-Consult-Test
Тестовое задание junior DE

1. Airflow

Для написания дага использовала PythonOperator, PostgresOperator, PostgresHook. 
Определила необходимые функции: определение типа учебного заведения, загрузку данных о заведениях из апи в формате json. 
Подключение к базе данных осуществила с помощью PostgresHook. Для реализации инкрементальной загрузки создала временную таблицу universities_tmp, где перед загрузкой новых данных происходит очистка таблицы. В основную таблицу загружаются данные, которые игнорируют дубликаты с помощью конструкции ON CONFLICT DO NOTHING. 
Затем определила даг, задала параметры и расписание - ежедневная загрузка в 3 ночи. Создала задачи для создания временной и основной таблиц, загрузки данных во временную таблицу и вставки новых данных в основную таблицу и установила зависимости.

[Ссылка на файл](https://github.com/AnastasiaKotelnikova/BI-Consult-Test/blob/main/load_universities_dag.py)

2. SQL

Создала функцию для загрузки json в таблицу lamoda_orders, загрузила данные. Соединила таблицу lamoda_orders и представление для получения отчета.

[Ссылка на файл](https://github.com/AnastasiaKotelnikova/BI-Consult-Test/blob/main/ddl.txt)

![Итоговая таблица](https://github.com/AnastasiaKotelnikova/BI-Consult-Test/blob/main/table.PNG)

