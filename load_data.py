from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values
import psycopg2

#establishing the connection
conn = psycopg2.connect(
   database="werkspot", user='postgres', password='password', host='localhost', port= '5432'
)
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Doping EMPLOYEE table if already exists.
cursor.execute("DROP TABLE IF EXISTS STAGING_EVENTS CASCADE")

#Creating table as per requirement
sql ='''CREATE TABLE STAGING_EVENTS(
   EVENT_ID INT NOT NULL,
   EVENT_TYPE CHAR(64),
   PROFESSIONAL_ID_ANONYMIZED INT NOT NULL,
   CREATED_AT TIMESTAMP,
   META_DATA CHAR(128)
)'''
cursor.execute(sql)
print("Table created successfully........")

with open('event_log.csv', 'r') as f:
    next(f)
    cursor.copy_from(f, 'staging_events', sep=';')


#Closing the connection
conn.close()
