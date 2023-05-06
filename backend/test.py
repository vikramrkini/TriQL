from general2neo import create_neo4j_nodes, create_neo4j_relationships
from py2neo import Graph, Node, Relationship
import requests
import json
import re

# Neo4j server credentials
neo4j_url = 'http://localhost:7474/db/neo4j/tx/commit'
headers = {'Content-Type': 'application/json'}
username = 'neo4j'
password = '12345678'

# Authenticate the REST API request
auth = (username, password)

# Function to execute the Cypher query using the REST API
def run_cypher_query(query):
    data = {'statements': [{'statement': query}]}
    response = requests.post(neo4j_url, headers=headers, auth=auth, json=data)
    return response.json()

# Function to extract nodes and relationships from the JSON response
def extract_nodes_relationships(response):
    
    nodes = []
    relationships = []
    
    for result in response['results']:
        for row_data in result['data']:
            for meta_item in row_data['meta']:
                if meta_item['type'] == 'node':
                    nodes.append(meta_item)
                elif meta_item['type'] == 'relationship':
                    relationships.append(meta_item)
                    
    return nodes, relationships
def modify_cypher_query(query):
    # Extract all parts of the query that match the pattern (label:label)
    matches = re.findall(r'\(([a-zA-Z]+):[a-zA-Z]+\)', query)
    matches.extend(re.findall(r'\[([a-zA-Z0-9_]*):',query))
    print(matches)
    
    # Replace the RETURN statement with the new one
    return_statement = 'RETURN ' + ', '.join([f'{match}' for match in matches])
    modified_query = re.sub(r'RETURN .+', return_statement, query)

    return modified_query

# run_cypher_query_endpoint()
print(modify_cypher_query("MATCH (Products:Products)-[Products_TO_Orders:Products_TO_Orders]->(Orders:Orders)   WHERE Products.ProductName <> 'Chocolade' RETURN Products.UnitPrice, Orders.ShipName, Orders.OrderDate, Products.ProductName"))