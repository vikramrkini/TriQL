import json
import re
from generateDatalog import generate_datalog_query
# from datalog2sql import datalog_to_sql

with open(f'schema/northwind-general.json','r') as f:
        schema = json.load(f)

def parse_query(query):
    # split the query into head and body
    head, body = query.split(':-')

    # parse the head
    head = [h.strip() for h in head.strip()[1:-1].split(',')]

    # parse the body
    body = body.strip().split(';')
    relations = [rel.strip() for rel in body[0].split(',')]
    body.pop(0)
    join_conditions = []
    if body and re.findall('\((.*?)\)',body[0]):
        join_conditions = [cond.strip() for cond in body[0][2:-1].split(',')]
        body.pop(0)
    where_clause = ''
    if body:
    # parse the where clause
        where_clause = body[0].strip()

    # construct the parsed query
    parsed_query = {
        'head': head,
        'relations': relations,
        'join_conditions': join_conditions,
        'where_clause': where_clause
    }

    return parsed_query

def datalog_to_mongo(query):
    #constructing the project operation 
    mongo_operator = {'>=' : ('{$gte:', '}'), '<=' : ('{$lte:', '}'), '!=' : ('{$ne:', '}'),\
             '>' : ('{$gt:', '}'), '<' : ('{$lt:', '}'), '=' : ('{$eq:', '}')}
    aggregation_function = {'COUNT', 'MAX', 'MIN', 'AVG', 'SUM'}
    parsed_query = parse_query(query)
    join_operation = parsed_query['join_conditions']
    where_clause = parsed_query['where_clause']
    where_clause = where_clause.split(',')
    if join_operation:
        join_tables = re.findall('(\w+)\.(\w+\s*)', join_operation[0])
    else:
    # Handle the case when join_operation is empty
    # You can either set join_tables to an empty list or another default value
        join_tables = []
    table1,table1_attr = join_tables[0][0] ,join_tables[0][1]
    # join_tables = re.findall('(\w+)\.(\w+\s*)',join_operation[0])
    if join_operation:
    # print(join_tables)
        table2 = join_tables[0][0],join_tables[1][0]
        table2_attr = join_tables[0][1],join_tables[1][1]
    #Constructing the lookup query 
        lookup_operation = ''
  
        lookup_operation = "{ $lookup : { from : " + table2 + ', localField : ' + table1_attr + ', foreignField : ' + table2_attr + ', as : ' + table1 +'_'+table2 + '}} , { $unwind : $' +  table1 +'_'+table2 + "}"
    match_conditions = ''
    if where_clause != ['']:
        if len(where_clause) > 1:
            match_conditions = "{ $match : { $and : ["
            op = []
            for clause in where_clause:
                left,operator,right = clause.strip().split(" ")
                op.append('{' + left + ' : ' + mongo_operator[operator][0] + right + mongo_operator[operator][1] + '}')
            match_conditions += ','.join(op)
            match_conditions += ']}}'
        elif len(where_clause) == 1:
            match_conditions = "{ $match : {"
            op = []
            for clause in where_clause:
                left,operator,right = clause.strip().split(" ")
                op.append(left + ' : ' + mongo_operator[operator][0] + right + mongo_operator[operator][1])
            match_conditions += ','.join(op)
            match_conditions += '}'
                   
    #Project opertation 
    project_operation = " { $project : { _id : 0 "
    for attr in parsed_query['head']:
         project_operation += f', {attr} : 1'
    project_operation += '} }'
    # query = 'db.' + table1 + '.aggregate([' + lookup_operation + ',' + match_conditions + ',' + project_operation + '])'
    query = f'db.{table1}.aggregate([{lookup_operation}{"," if match_conditions else ""}{match_conditions}{"," if project_operation else ""}{project_operation}])'

    return query 
        


datalog = "(Orders.OrderID) :- Orders(OrderID , CustomerID , EmployeeID , OrderDate , RequiredDate , ShippedDate , ShipVia , Freight , ShipName , ShipAddress , ShipCity , ShipRegion , ShipPostalCode , ShipCountry) ; Orders.OrderID < 10250"
# # print(parse_query(query))
# query = "(Customers.CompanyName) :- Customers(CustomerID , CompanyName , ContactName , ContactTitle , Address , City , Region , PostalCode , Country , Phone , Fax) ,Orders(OrderID , CustomerID , EmployeeID , OrderDate , RequiredDate , ShippedDate , ShipVia , Freight , ShipName , ShipAddress , ShipCity , ShipRegion , ShipPostalCode , ShipCountry) ; (Orders.CustomerID = Customers.CustomerID);"
mongo_query = datalog_to_mongo(datalog)
print(mongo_query)

 