#This code will generate the data-log query from the given Input.
import json
import re
#Read the input file. 
# with open(f'schema/northwind-general.json','r') as f:
#         schema = json.load(f)

def get_joining_attribute(schema, tables):
    seen = set()
    join = []
    for table in tables:
        #   print(table)
          if schema[table]['To']:
            #    print(schema[table]['To'])
               for table_join in schema[table]['To']:
                    if table_join[1] in tables and table != table_join[1]:
                         
                         #Join the table 
                         join.append(table +'.'+table_join[0] + ' = ' + table_join[1]+'.'+table_join[0])
    return join

#If there is no common attribute then perform cross join i.e remove the joining conditions
# def generate_datalog_query(schema, tables, attributes, conditions):
#     # Define the head of the query using the attributes
#     head = "(" + ", ".join(attributes) + ")"
#     body = ''
#     #Get all the attribute names from the selected tables.
#     for idx, table in enumerate(tables) :
#         table_attributes = []
#         for attr in schema[table]['Attributes']:
#                  table_attributes.append(attr['name'])
#         body += table +'(' +" , ".join(table_attributes) + ') ,' if idx < len(tables) - 1 else table +'(' +" , ".join(table_attributes) + ') ; '
#     # print(body)
#     # print(table_attributes)
#     # Define the body of the query using the table names and join conditions
#     # body = ", ".join([t + "(" + ", ".join(attributes) + ")" for t in tables])
#     join_conditions = get_joining_attribute(schema,tables)
#     if join_conditions:
#         body +=  '('+  ",".join(join_conditions) +')'  +'; ' 


#     if conditions:
#         # if join_conditions:
#         #     body += ", "
#         # else:
#         #     body += ":- "
#         body += " , ".join(conditions)
#     # Combine the head and body to form the complete query
#     query = head + " :- " + body 
#     return query


def generate_datalog_query(schema, tables, attributes, formatted_conditions):
    # Define the head of the query using the attributes
    head = "(" + ", ".join(attributes) + ")"
    body = ''
    #Get all the attribute names from the selected tables.
    for idx, table in enumerate(tables) :
        table_attributes = []
        for attr in schema[table]['Attributes']:
                 table_attributes.append(attr['name'])
        body += table +'(' +" , ".join(table_attributes) + ') ,' if idx < len(tables) - 1 else table +'(' +" , ".join(table_attributes) + ') ; '
    # print(body)
    # print(table_attributes)
    # Define the body of the query using the table names and join conditions
    # body = ", ".join([t + "(" + ", ".join(attributes) + ")" for t in tables])
    join_conditions = get_joining_attribute(schema,tables)
    print(join_conditions)
    if join_conditions:
        body +=  '('+  ",".join(join_conditions) +')'  +'; ' 

    if formatted_conditions:
        body += " , ".join(formatted_conditions)
    # Combine the head and body to form the complete query
    query = head + " :- " + body 
    return query

# tables = ["Order Details","Orders","Customers"]
# attributes = ["OrderID","UnitPrice","OrderDate"]
# conditions = ["OrderID > 5000","UnitPrice = 4000"]
# query = generate_datalog_query(schema,tables, attributes, conditions)
# print(query)