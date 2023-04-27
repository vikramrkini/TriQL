import json
import re
from generateDatalog import generate_datalog_query
# from datalog2sql import datalog_to_sql

# with open(f'schema/northwind-general.json','r') as f:
#         schema = json.load(f)

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

def datalog_to_mongo(tables,query):
    #constructing the project operation 
    mongo_operator = {'>=' : ('{$gte:', '}'), '<=' : ('{$lte:', '}'), '!=' : ('{$ne:', '}'),\
             '>' : ('{$gt:', '}'), '<' : ('{$lt:', '}'), '=' : ('{$eq:', '}')}
    aggregation_function = {'COUNT', 'MAX', 'MIN', 'AVG', 'SUM'}
    parsed_query = parse_query(query)
    join_operation = parsed_query['join_conditions']
    where_clause = parsed_query['where_clause']
    where_clause = where_clause.split(',')
    # if join_operation:
    #     join_tables = re.findall('(\w+)\.(\w+\s*)', join_operation[0])    
    # # join_tables = re.findall('(\w+)\.(\w+\s*)',join_operation[0])
    # # print(join_tables)
    #     table1,table1_attr = join_tables[0][0] ,join_tables[0][1]
    #     table2 = join_tables[0][0],join_tables[1][0]
    #     table2_attr = join_tables[0][1],join_tables[1][1]    
    #     lookup_operation = "{ $lookup : { from : \"" + table2 + '\", localField : \"' + table1_attr + '\", foreignField : \"' + table2_attr + '\", as : \"' + table1 +'_'+table2 + '\"}} , { $unwind : \"$' +  table1 +'_'+table2 + "\"}"
    # else:
    #     lookup_operation = ''
    #     table1 = tables[0]
    # print(where_clause)
    # match_conditions = ''
    # if where_clause != ['']:
    #     if len(where_clause) > 1:
    #         match_conditions = "{ $match : { $and : ["
    #         op = []
    #         for clause in where_clause:
    #             left,operator,right = clause.strip().split(" ")
    #             op.append('{' + left + ' : ' + mongo_operator[operator][0] + right + mongo_operator[operator][1] + '}')
    #         match_conditions += ','.join(op)
    #         match_conditions += ']}}'
    #     elif len(where_clause) == 1:
    #         match_conditions = "{ $match : {"
    #         op = []
    #         for clause in where_clause:
    #             left,operator,right = clause.strip().split(" ")
    #             op.append(left + ' : ' + mongo_operator[operator][0] + right + mongo_operator[operator][1])
    #         match_conditions += ','.join(op)
    #         match_conditions += '}'
    if join_operation:
        join_tables = re.findall('(\w+)\.(\w+\s*)', join_operation[0])
        table1, table1_attr = join_tables[0][0], join_tables[0][1]
        table2, table2_attr = join_tables[1][0], join_tables[1][1]
        lookup_operation = f'{{ $lookup : {{ from : \"{table2}\", localField : \"{table1_attr.strip()}\", foreignField : \"{table2_attr.strip()}\", as : \"{table1.strip()}_{table2.strip()}\" }} }}, {{ $unwind : \"${table1.strip()}_{table2.strip()}\" }}'
    else:
        lookup_operation = ''
        table1 = tables[0]
    match_conditions = ''
    # if where_clause != ['']:
    #     if len(where_clause) > 1:
    #         match_conditions = '{ $match : { $and : ['
    #         op = []
    #         for clause in where_clause:
    #             # left, operator, right = clause.strip().split(" ")
    #             parse_condition = re.findall('^(\w+(?:\.\w+)?\s*)(>=|<=|!=|<|>|=)\s*(\'.+?\'|".+?"|\w+)',clause)
    #             # print(parse_condition)
    #             left,operator,right = parse_condition[0][0].strip(), parse_condition[0][1].strip(), parse_condition[0][2].strip()
                
    #             op.append(f'{{ \"{left}\" : {mongo_operator[operator][0]}{right}{mongo_operator[operator][1]} }}')
    #         match_conditions += ','.join(op)
    #         match_conditions += ']}}'
    #     elif len(where_clause) == 1:
    #         match_conditions = '{ $match : {'
    #         op = []
    #         print(where_clause)
    #         for clause in where_clause:
                
    #             parse_condition = re.findall('^(\w+(?:\.\w+)?\s*)(>=|<=|!=|<|>|=)\s*(\'.+?\'|".+?"|\w+)',clause)
    #             # print(parse_condition)
    #             left,operator,right = parse_condition[0][0].strip(), parse_condition[0][1].strip(), parse_condition[0][2].strip()
                
    #             op.append(f'\"{left}\" : {mongo_operator[operator][0]}{right}{mongo_operator[operator][1]}')
    #         match_conditions += ','.join(op)
    #         match_conditions += '}}'
    if where_clause != ['']:
        if len(where_clause) > 1:
            match_conditions = '{ $match : { $and : ['
            op = []
            
            where_clause = [where_clause[clause].strip() for clause in range(len(where_clause))]
            for clause in where_clause:
                
                parse_condition = re.findall('^(\w+(?:\.\w+)?\s*)(>=|<=|!=|<|>|=)\s*(\'.+?\'|".+?"|\w+)', clause)
                # print(parse_condition)
                left, operator, right = parse_condition[0][0].strip(), parse_condition[0][1].strip(), parse_condition[0][2].strip()
                left_table = left.split('.')[0]
                # print(left_table,table2)
                if join_operation and left_table == table2:
                    left = f'{table1}_{table2}.{left.split(".")[1]}'
                else:
                    left = f'{left.split(".")[1]}'
                op.append(f'{{ \"{left}\" : {mongo_operator[operator][0]}{right}{mongo_operator[operator][1]} }}')
            match_conditions += ','.join(op)
            match_conditions += ']}}'
        elif len(where_clause) == 1:

            match_conditions = '{ $match : {'
            op = []
            
            for clause in where_clause:
                parse_condition = re.findall('^(\w+(?:\.\w+)?\s*)(>=|<=|!=|<|>|=)\s*(\'.+?\'|".+?"|\w+)', clause)
                left, operator, right = parse_condition[0][0].strip(), parse_condition[0][1].strip(), parse_condition[0][2].strip()
                left_table = left.split('.')[0]
                # print(left_table,table2)
                if join_operation and left_table == table2 :
                    left = f'{table1}_{table2}.{left.split(".")[1]}'
                else:
                    left = f'{left.split(".")[1]}'
                op.append(f'\"{left}\" : {mongo_operator[operator][0]}{right}{mongo_operator[operator][1]}')
            match_conditions += ','.join(op)
            match_conditions += '}}'

    # project_operation = ' { "$project" : { "_id" : 0 '
    # for attr in parsed_query['head']:
    #     project_operation += f', \"{attr.replace(table1+".","") }\" : 1'
    # project_operation += '}}'
    parsed_head = []
    for attr in parsed_query['head']:
        table_name, field_name = attr.split('.')
        parsed_head.append(field_name.strip())

    # project_operation = ' { "$project" : { "_id" : 0 '
    # for attr in parsed_head:
    #     project_operation += f', \"{attr}\" : 1'
    # project_operation += '}}'
    parsed_head = []
    for attr in parsed_query['head']:
        table_name, field_name = attr.split('.')
        parsed_head.append((table_name.strip(), field_name.strip()))

    project_operation = ' { "$project" : { "_id" : 0 '
    for table, attr in parsed_head:
        if join_operation and table == table2:
            project_operation += f', \"{attr}\" : \"${table1}_{table2}.{attr}\"'
        else:
            project_operation += f', \"{attr}\" : 1'
    project_operation += '}}'

    query = f'db.{table1}.aggregate([{lookup_operation}{" , " if lookup_operation else ""}{match_conditions}{" , " if match_conditions else ""}{project_operation}])'

    return query
    query = f'db.{table1}.aggregate([{lookup_operation}{" , " if lookup_operation else ""}{match_conditions}{" , " if match_conditions else ""}{project_operation}])'               
    # #Project opertation 
    # project_operation = " { \"$project\" : { \"_id\" : 0 "
    # for attr in parsed_query['head']:
    #      project_operation += f', \"{attr}\" : 1'
    # project_operation += '} }'
    # query = 'db.' + table1 + '.aggregate([' + lookup_operation + ',' + match_conditions + ',' + project_operation + '])'
    query = f'db.{table1}.aggregate([{lookup_operation}{"," if lookup_operation else ""}{match_conditions}{"," if match_conditions else ""}{project_operation}])'

    return query 
        

# tables = ["Orders"]
# datalog = "(Orders.ShipName, Orders.OrderDate, Orders.OrderID, Customers.ContactName, Customers.CompanyName) :- Customers(CustomerID , CompanyName , ContactName , ContactTitle , Address , City , Region , PostalCode , Country , Phone , Fax) ,Orders(OrderID , CustomerID , EmployeeID , OrderDate , RequiredDate , ShippedDate , ShipVia , Freight , ShipName , ShipAddress , ShipCity , ShipRegion , ShipPostalCode , ShipCountry) ; (Orders.CustomerID = Customers.CustomerID); Customers.CompanyName != 'Antonio Moreno Taquería' , Orders.OrderID < '10302'"
# # # print(parse_query(query))
# # query = "(Customers.CompanyName) :- Customers(CustomerID , CompanyName , ContactName , ContactTitle , Address , City , Region , PostalCode , Country , Phone , Fax) ,Orders(OrderID , CustomerID , EmployeeID , OrderDate , RequiredDate , ShippedDate , ShipVia , Freight , ShipName , ShipAddress , ShipCity , ShipRegion , ShipPostalCode , ShipCountry) ; (Orders.CustomerID = Customers.CustomerID);"
# mongo_query = datalog_to_mongo(tables,datalog)
# print(mongo_query)

 