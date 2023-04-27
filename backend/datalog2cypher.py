import re
import json
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

def load_relationships(schema):
    relationships = {}
    for table in schema:
        if schema[table]['Type'] == 'relationship' and schema[table]['To']:
            from_node = schema[table]['To'][0][1]
            to_node = schema[table]['To'][1][1]
            relationship_name = f"{from_node}_TO_{to_node}"
            relationships[(from_node, to_node)] = relationship_name
    return relationships

# def datalog_to_cypher(datalog_query, relationships):
#     parsed_query = parse_query(datalog_query)

#     # # Construct match clause
#     # match_clause = "MATCH "
#     # node_names = set([head.split('.')[0] for head in parsed_query['head']])
#     # for node_name in node_names:
#     #     match_clause += f"({node_name}:{node_name})"

#     # # Add relationships to match clause
#     # for pair in relationships:
#     #     from_node, to_node = pair
#     #     if from_node in node_names and to_node in node_names:
#     #         match_clause += f"-[:{relationships[pair]}]->"
     

#     # Construct match clause
#     match_clause = "MATCH "
#     node_names = set([head.split('.')[0] for head in parsed_query['head']])
    
#     # Add relationships to match clause
#     for pair in relationships:
#         from_node, to_node = pair
#         if from_node in node_names and to_node in node_names:
#             match_clause += f"({from_node}:{from_node})-[:{relationships[pair]}]->({to_node}:{to_node}),"
    
#     match_clause = match_clause.rstrip(',')
#     # Construct join conditions
#     join_conditions = ""
#     if parsed_query['join_conditions']:
#         join_conditions = "WHERE "
#         for join_condition in parsed_query['join_conditions']:
#             left, operator, right = join_condition.split(" ")
#             join_conditions += f"{left} {operator} {right} AND "
#         join_conditions = join_conditions[:-5]

#     # Construct where clause
#     where_clause = ""
#     if parsed_query['where_clause']:
#         where_clause = parsed_query['where_clause']
#         where_clause = where_clause.replace(',', ' AND ')
#         if join_conditions:
#             where_clause = "AND " + where_clause
#         else:
#             where_clause = "WHERE " + where_clause

#     # Construct return clause
#     return_clause = "RETURN "
#     for attr in parsed_query['head']:
#         return_clause += f"{attr}, "
#     return_clause = return_clause[:-2]

#     cypher_query = f"{match_clause} {join_conditions} {where_clause} {return_clause}"
#     return cypher_query


def datalog_to_cypher(schema,datalog_query):
    parsed_query = parse_query(datalog_query)
    relationships = load_relationships(schema)
    match_clause = "MATCH "
    node_names = set([head.split('.')[0] for head in parsed_query['head']])
    
    # for pair in relationships:
    #     from_node, to_node = pair
    #     if from_node in node_names and to_node in node_names:
    #         match_clause += f"({from_node}:{from_node})-[:{relationships[pair]}]->({to_node}:{to_node}),"
    
    connected_nodes = set()
    for pair in relationships:
        from_node, to_node = pair
        if from_node in node_names and to_node in node_names:
            match_clause += f"({from_node}:{from_node})-[:{relationships[pair]}]->({to_node}:{to_node}),"
            connected_nodes.add(from_node)
            connected_nodes.add(to_node)
    
    disconnected_nodes = node_names - connected_nodes
    for node in disconnected_nodes:
        match_clause += f"({node}:{node}),"
    
    match_clause = match_clause.rstrip(',')
    
    match_clause = match_clause.rstrip(',')
    # Construct join conditions
    join_conditions = ""
    if parsed_query['join_conditions']:
        join_conditions = "WHERE "
        for join_condition in parsed_query['join_conditions']:
            left, operator, right = join_condition.split(" ")
            operator = "<>" if operator == '!=' else operator
            join_conditions += f"{left} {operator} {right} AND "
        join_conditions = join_conditions[:-5]

    # Construct where clause
    where_clause = ""
    if parsed_query['where_clause']:
        where_clause = parsed_query['where_clause']
        where_clause = where_clause.replace(',', ' AND ')
        if join_conditions:
            where_clause = "AND " + where_clause
        else:
            where_clause = "WHERE " + where_clause

    # Construct return clause
    return_clause = "RETURN "
    for attr in parsed_query['head']:
        return_clause += f"{attr}, "
    return_clause = return_clause[:-2]

    cypher_query = f"{match_clause} {join_conditions} {where_clause} {return_clause}"
    return cypher_query




# # Load schema JSON
# with open('schema/northwind-general.json', 'r') as f:
#     schema = json.load(f)

# # Load relationships from the schema
# relationships = load_relationships(schema)
# print(relationships)
# # Datalog query
# datalog_query = "(Orders.OrderID, Products.ProductID, Products.ProductName) :- Orders(OrderID , CustomerID , EmployeeID , OrderDate , RequiredDate , ShippedDate , ShipVia , Freight , ShipName , ShipAddress , ShipCity , ShipRegion , ShipPostalCode , ShipCountry) ,Products(ProductID , ProductName , SupplierID , CategoryID , QuantityPerUnit , UnitPrice , UnitsInStock , UnitsOnOrder , ReorderLevel , Discontinued) ; "

# # Convert Datalog query to Cypher query
# cypher_query = datalog_to_cypher(datalog_query, relationships)
# print(cypher_query)
