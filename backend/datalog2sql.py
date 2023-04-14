import json
import re
from generateDatalog import generate_datalog_query


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


def datalog_to_sql(tables,query):
    # Parse the Datalog query
    parsed_query = parse_query(query)

    # Extract the head, body, and conditions of the Datalog query
    head = parsed_query['head']
    relations = parsed_query['relations']
    join_conditions = parsed_query['join_conditions']
    where_clause = parsed_query['where_clause']

    # Construct the SQL SELECT statement
    select = ', '.join(head)

    # Construct the SQL FROM clause
    # from_clause = ', '.join(relations)
    from_clause = ''
    join_condition_strs = []
    joined_relations = set()
    queue = [relations[0]]
    joined_tables = set()
    join_conditions_list = []
    if join_conditions:
    # create an empty list to store the join conditions
        

        # create a queue to store the relations
    # loop through the queue until all relations have been processed
        while queue:
            # get the first relation in the queue
            curr_rel = queue.pop(0)
            # add the current relation to the set of joined tables
            joined_tables.add(curr_rel)
            # loop through the join conditions
            for join_condition in join_conditions:
                # check if the current relation is in the join condition
                if curr_rel in join_condition:
                    # split the join condition into left and right sides
                    left, right = join_condition.split('=')
                    # get the left and right relations and columns
                    left_rel, left_col = left.split('.')
                    right_rel, right_col = right.split('.')
                    # check if the left relation is the current relation
                    if left_rel == curr_rel:
                        # check if the right relation has not been joined yet
                        if right_rel not in joined_tables:
                            # add the right relation to the queue
                            queue.append(right_rel)
                            # create the join condition string
                            join_condition_str = f'{left_rel} JOIN {right_rel} ON {left} = {right}'
                            # add the join condition string to the list
                            join_conditions_list.append(join_condition_str)
                            # add the right relation to the set of joined tables
                            joined_tables.add(right_rel)
                    # check if the right relation is the current relation
                    elif right_rel == curr_rel:
                        # check if the left relation has not been joined yet
                        if left_rel not in joined_tables:
                            # add the left relation to the queue
                            queue.append(left_rel)
                            # create the join condition string
                            join_condition_str = f'{right_rel} JOIN {left_rel} ON {right} = {left}'
                            # add the join condition string to the list
                            join_conditions_list.append(join_condition_str)
                            # add the left relation to the set of joined tables
                            joined_tables.add(left_rel)
    
    else :
         from_clause += ','.join(tables)


    # join_clause = ''.join(join_condition_strs)
    join_condition_strs = []
    for join_condition in join_conditions:
        left, right = join_condition.split('=')
        left_rel, left_col = left.split('.')
        right_rel, right_col = right.split('.')
        join_condition_str = f'{left_rel} JOIN {right_rel} ON {left} = {right}'
        join_condition_strs.append(join_condition_str)
    
    if len(join_condition_strs) > 1:
         for idx in range(1,len(join_condition_strs)):
            check = (join_condition_strs[idx].split())
            for i,item in enumerate(check):
                if item not in ['JOIN','ON','='] and item in join_condition_strs[idx-1]:
                     if i == 0: 
                          check.pop(0)
            join_condition_strs[idx] = ' '.join(check)                    

                
              
              
              
              
    # print(join_condition_strs)
    join_clause = ' '.join(join_condition_strs)
    # print(join_clause)
    # Construct the SQL WHERE clause
    where_clause = where_clause.replace(',', ' AND ')

    # Construct the SQL query
    sql_query = f'SELECT {select} FROM {from_clause} {join_clause} WHERE {where_clause}' if where_clause else f'SELECT {select} FROM {from_clause} {join_clause}'

    return sql_query


# tables = ['Customers', 'Order Details', 'Orders'] 
# attributes = ['Orders.CustomerID', 'Customers.CompanyName', 'Order Details.Quantity']
# conditions =  ['Order Details.Quantity > 10']
# query = generate_datalog_query(schema,tables, attributes, conditions)
# # print(query)
# parsed_query = parse_query(query)
# # print(parsed_query)
# SQL_query = datalog_to_sql(tables,query)
# print(SQL_query)
