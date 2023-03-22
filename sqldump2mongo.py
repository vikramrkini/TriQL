import sqlite3
import json

# Connect to the SQLite database
conn = sqlite3.connect('example.db')

# Get a cursor object
cursor = conn.cursor()

# Execute the SQL commands in the SQL file
with open('northwind.sql', 'r') as sql_file:
    sql = sql_file.read()
    cursor.executescript(sql)

# Get table information
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
table_names = [row[0] for row in cursor.fetchall()]
del[table_names[0]]
primary_key_map = {table : [] for table in table_names}
foreign_key_map = {table : [] for table in table_names}

for table in table_names:
    try :
        cursor.execute(f"SELECT l.name from pragma_table_info('{table}') as l where l.pk <> 0 ;")
        op = cursor.fetchall()
        if op :
            for j in op :
                primary_key_map[table].append(j[0])
    except sqlite3.OperationalError as s :
      
        continue



for table in table_names:
    try :
        cursor.execute(f"PRAGMA foreign_key_list('{table}')")
        op = cursor.fetchall()
        if op :
            for j in op:
                foreign_key_map[table].append((j[2],j[3]))
    except sqlite3.OperationalError as s:
        pass
print('------------------')
print(foreign_key_map)
print('-----------------')
print(primary_key_map)
# print(table_names)