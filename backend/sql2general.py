# from importlib import metadata
from sqlalchemy import create_engine, MetaData , Table, distinct, func, inspect , select
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import text
from sqlalchemy.orm import class_mapper
import sqlite3
import json


from collections import defaultdict

import base64

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            # Convert binary data to Base64-encoded string
            return base64.b64encode(obj).decode('utf-8')
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, object):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# Creating a sqlite database 
# con = sqlite3.connect('schema/northwind.db')
# cur = con.cursor()
# with open('schema/northwind.sql', 'r') as sql_file:
#     sql = sql_file.read()
#     cur.executescript(sql)


# # Replace the placeholders with your database credentials
# engine = create_engine('sqlite:///northwind.db?')
# inspector = inspect(engine)
# cursor = engine
# metadata = MetaData()
# metadata.reflect(bind=engine)

# Define a function to determine whether a table is an entity or a relationship
def get_table_type(table_name,metadata):
    # Get the table metadata from the reflected database
    try : 
        table_metadata = metadata.tables[table_name]

        # Check if the table has a primary key
        if not table_metadata.primary_key:
            return "relationship"

        # Get the columns that are foreign keys
        fk_columns = [
            c for c in table_metadata.columns
            if c.foreign_keys
        ]

        # If there are no foreign key columns, this is an entity
        if not fk_columns:
            return "entity"

        # If there is exactly one foreign key column, this is a many-to-one relationship
        if len(fk_columns) == 1:
            fk_column = fk_columns[0]
            fk_table = fk_column.foreign_keys[0].column.table.name
            fk_table_metadata = metadata.tables[fk_table]
            if not fk_table_metadata.primary_key:
                return "relationship"
            else:
                return "entity"

        # If there are more than one foreign key columns, this is a many-to-many relationship
        if len(fk_columns) > 1:
            return "relationship"
        
    except TypeError as t :
        pass



#---------------------------------------------------------------------------

def get_list_of_table_names(inspector):
    # table_names = engine.table_names()
    table_names = inspector.get_table_names()
    #Removing the last sqllite_sequence from the table 
    table_names.pop(-1)
    return table_names

def get_list_of_table_foreign_keys(table_names,metadata):
    table_foreign_keys = {table_names[t]:[] for t in range(len(table_names))}
    for t in table_names:
        table = Table(t,metadata,autoload =True)
        for foreign_key in table.foreign_key_constraints :
            table_foreign_keys[t].append((foreign_key.column_keys[0],foreign_key.referred_table.name))
    return table_foreign_keys

def get_list_of_table_primary_keys(table_names,metadata):
    table_primary_keys = {table_names[t]:[] for t in range(len(table_names))}
    for t in table_names:
        table = Table(t,metadata,autoload =True)
        for primary_key in table.primary_key.columns:
            table_primary_keys[t].append(primary_key.name)
    return table_primary_keys



#Logic 
#If table has no foriegn key the it is a entity
#If the primary key and the foreign key are referencing the same attribute then it is a relation else it is a entity
def determine_entity_relation(table,table_names,metadata):
    table_primary_keys = get_list_of_table_primary_keys(table_names,metadata)
    table_foreign_keys = get_list_of_table_foreign_keys(table_names,metadata)
    #If a table has no foreign keys then return entity
    # print(table_foreign_keys[table])
    if table_foreign_keys[table] == []:
        return 'entity'
    count = 0
    for tp in sorted(table_primary_keys[table]):
        for tf in sorted(table_foreign_keys[table]):
            if tp == tf[0]:
                # print(tp,tf[0])
                count += 1
            
    return 'relationship' if count == len(table_foreign_keys[table]) else 'entity'

#Function to get mapping cardinalities 
def find_cardinality(table,table_names,metadata):
    #CustomerCustomerDemo': [('CustomerTypeID', 'CustomerDemographics'), ('CustomerID', 'Customers')] 
    table_primary_keys = get_list_of_table_primary_keys(table_names,metadata)
    table_foreign_keys = get_list_of_table_foreign_keys(table_names,metadata)
    if not table_foreign_keys[table] : 
        return 
    res = []
    for foreign_keys in table_foreign_keys[table]:
    #CustomerCustomerDemo': [('CustomerTypeID', 'CustomerDemographics'), ('CustomerID', 'Customers')]  
        res.append(table + '(N) --> '+ foreign_keys[1] +'(1)')
    return res    

#Function to get attributes and their data types 

def get_attributes_and_constraints(table,engine):
    inspector = inspect(engine)
    columns = inspector.get_columns(table)
    result = []
    for column in columns:
        column_dict = {}
        for key in column:
            column_dict[key] = column[key]
        result.append(column_dict)
        result_json = json.loads(json.dumps(result, cls=CustomEncoder))
    return result_json

#To Do : Add the data, relationship direction

def direction_of_relation(table,table_names,metadata):
    table_foreign_keys = get_list_of_table_foreign_keys(table_names,metadata)
    if table in table_foreign_keys:
        return table,table_foreign_keys[table]



def get_data(table_name,engine,metadata):
    table = Table(table_name, metadata, autoload=True)
    # Select all rows from the table and convert to a list of dictionaries
    with engine.connect() as conn:
        rows = conn.execute(table.select()).fetchmany(20)
    data = []
    # Loop over each row and create a dictionary of column name:value pairs
    for row in rows:
        row_mapping = row._mapping
        row_dict = {}
        for column, value in row_mapping.items():
            row_dict[column] = value
        data.append(row_dict)
    # return data
    return json.loads(json.dumps(data, cls=CustomEncoder))
    # return decode_fetchall_output(op)

def decode_fetchall_output(result_set):
    """
    Decode an SQL Alchemy fetchall() output into a list of dictionaries.
    """
    column_names = [col[0] for col in result_set.cursor.description]
    rows = result_set.fetchall()
    result = []
    for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
            row_dict[column_names[i]] = value
        result.append(row_dict)
    return result

#Todo : Can add more logic using joins 

def get_mapping_cardinality(table_name,metadata):
    table = metadata.tables[table_name]
    cardinality = []
    primary_keys = [(str(key).split('.'))[1] for key in table.primary_key.columns]
    for fk in table.foreign_key_constraints:
        referred_table = fk.referred_table
        ref_col_name = fk.column_keys[0]
        primary_keys_of_referenced_relation = [(str(key).split('.'))[1] for key in referred_table.primary_key.columns]
        # print(primary_keys_of_referenced_relation)
        if ref_col_name in primary_keys:
            if ref_col_name in primary_keys_of_referenced_relation:
                cardinality.append([referred_table.name,'1->1'])
            else:
                cardinality.append([referred_table.name,'1->M'])
        else:
            if ref_col_name in primary_keys_of_referenced_relation:
                cardinality.append([referred_table.name,'M->1'])
            else:
                cardinality.append([referred_table.name,'M->N'])
        # print(referred_table.name,ref_col_name,referred_table.primary_key.columns[0])
    return cardinality
   
import pprint as pp 
# if __name__ == '__main__' :
#     schema = {}
#     table_names = get_list_of_table_names()
#     # print(get_list_of_table_foreign_keys(table_names))
#     # print("--------------------------------------------------------------------------------------")
#     # print(get_list_of_table_primary_keys(table_names))
#     # pp.pprint(get_mapping_cardinality('Orders'), indent = 4)
#     for table in table_names:
#         schema[table] =  {'Type' : determine_entity_relation(table,table_names) , 'Cardinality' : get_mapping_cardinality(table) , 'Attributes' : get_attributes_and_constraints(table) ,'From' : direction_of_relation(table,table_names)[0],'To' : direction_of_relation(table,table_names)[1], 'Data' : get_data(table)} 
   
#     with open('general.json', 'w') as json_file:
#         json.dump(schema, json_file, indent=4)
    