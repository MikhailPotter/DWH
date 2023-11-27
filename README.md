# Как все запускать:

1. Запустить PostgreSQL с Docker Desktop.

2. Создать и настроить коннектор Debezium, выполнив следующую команду:

curl -X POST --location "http://localhost:8083/connectors" -H "Content-Type: application/json" -H "Accept: application/json" -d @debezium.json

3. Запустить консумера Kafka для чтения изменений данных:

curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" -d '{"name": "dwh_group", "format": "binary", "auto.offset.reset": "latest"}' http://localhost:8082/consumers/dwh_group

4. Установить и настроить подключение к базе данных.

Например, с помощью psycopg2:

import psycopg2
connection = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='postgres',
    host='localhost',
    port='5432'
)

Теперь, база данных будет реплицировать изменения в PostgreSQL.
<<<<<<< HEAD

P.S. 
Чтобы создались топики, необходимо добавить хотя бы одну строчку в каждую таблицу (debezium прописано авто создание топиков).
=======
>>>>>>> 7a44f4307334e6fa6bf9a1a79b5bb6e5a975aee7
