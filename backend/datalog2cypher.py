import json
import re
from neo4j import GraphDatabase
from py2neo import Graph

# def convert_datalog_to_cypher(datalog_query, schema):
#     # Extract entities and conditions from Datalog query
#     entities = re.findall(r'([A-Za-z]+)\(', datalog_query)
#     conditions = re.findall(r'\((.*?)\)', datalog_query)[-1].split(';')

#     # Find relationships between entities
#     relationships = {}
#     for table in schema:
#         if schema[table]['Type'] == 'relationship' and schema[table]['To']:
#             from_node = schema[table]['To'][0][1]
#             to_node = schema[table]['To'][1][1]
#             if from_node in entities and to_node in entities:
#                 relationships[from_node] = to_node

#     # Generate Cypher MATCH clause
#     match_clause = "MATCH "
#     for entity, related_entity in relationships.items():
#         match_clause += f"({entity.lower()}: {entity})-[:RELATED_TO]->({related_entity.lower()}: {related_entity}), "

#     match_clause = match_clause.rstrip(', ')

#     # Generate Cypher WHERE clause
#     where_clause = ""
#     for condition in conditions:
#         condition = condition.strip()
#         if condition:
#             if where_clause:
#                 where_clause += " AND "
#             else:
#                 where_clause += "WHERE "
#             where_clause += f"{condition} "

#     # Generate Cypher RETURN clause
#     return_clause = "RETURN " + re.findall(r'\((.*?)\)', datalog_query)[0]

#     # Combine clauses into Cypher query
#     cypher_query = f"{match_clause} {where_clause} {return_clause}"

#     return cypher_query

def convert_datalog_to_cypher(datalog_query, schema):
    # Extract entities and conditions from Datalog query
    entities = re.findall(r'([A-Za-z]+)\(', datalog_query)
    conditions = re.findall(r'\((.*?)\)', datalog_query)[-1].split(';')

    # Generate Cypher MATCH clause
    match_clause = "MATCH "
    for entity in entities:
        match_clause += f"({entity.lower()}: {entity}), "

    match_clause = match_clause.rstrip(', ')

    # Generate Cypher WHERE clause
    where_clause = ""
    for condition in conditions:
        condition = condition.strip()
        if condition:
            if where_clause:
                where_clause += " AND "
            else:
                where_clause += "WHERE "
            where_clause += f"{condition} "

    # Generate Cypher RETURN clause
    return_clause = "RETURN " + re.findall(r'\((.*?)\)', datalog_query)[0]

    # Combine clauses into Cypher query
    cypher_query = f"{match_clause} {where_clause} {return_clause}"

    return cypher_query


# Datalog query
datalog_query = "(Customers.CustomerID, Orders.ShipName, Customers.ContactName) :- Customers(CustomerID , CompanyName , ContactName , ContactTitle , Address , City , Region , PostalCode , Country , Phone , Fax) ,Orders(OrderID , CustomerID , EmployeeID , OrderDate , RequiredDate , ShippedDate , ShipVia , Freight , ShipName , ShipAddress , ShipCity , ShipRegion , ShipPostalCode , ShipCountry) ; (Orders.CustomerID = Customers.CustomerID); Customers.ContactName < 30"

# Load schema JSON
with open('schema/northwind-general.json', 'r') as f:
    schema = json.load(f)

# Convert Datalog query to Cypher query
cypher_query = convert_datalog_to_cypher(datalog_query, schema)
print(cypher_query)
