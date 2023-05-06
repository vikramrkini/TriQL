from general2neo import create_neo4j_nodes,create_neo4j_relationships
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase
import json
# def load_neo(schema):
#     # with open('example.json','r') as f:
#     #     schema = json.load(f)
#     graph = Graph("bolt://localhost:7687", auth=("neo4j","12345678"))
#     graph.delete_all()
#     create_neo4j_nodes(schema,graph)
#     create_neo4j_relationships(schema,graph)
def get_nodes_and_relationships(driver, query):
    nodes = []
    relationships = []

    with driver.session() as session:
        result = session.run(query)

        for record in result:
            print(record)
            for value in record.values():
                print(value)
                if isinstance(value, list):
                    for element in value:
                        if isinstance(element, Node):
                            nodes.append(element)
                        elif isinstance(element, Relationship):
                            relationships.append(element)
                elif isinstance(value, Node):
                    nodes.append(value)
                elif isinstance(value, Relationship):
                    relationships.append(value)

    return nodes, relationships

def run_cypher_query(driver,query):
        node,relationship = get_nodes_and_relationships(driver,query)
        print(node,relationship)
        with driver.session() as session:
            data = session.read_transaction(extract_graph_data, query)
            return data
            # if not graph_data['edges']:
            #     print("Only nodes:")
            #     print(graph_data['nodes'])
            # else:
            #     print("Graph data:")
            

def extract_graph_data(tx, query):
    result = tx.run(query)
    print(result)
    data = []
    for r in result:
        row = []
        for key in r.keys():
            row.append(r[key])
        data.append(row)
    return data


# def run_cypher_query_endpoint():
#     # data = request.json
#     # schema_name = data['schemaName']
#     # cypher_query = data['cypherQuery']
#     cypher_query = "MATCH (Orders:Orders)-[:Orders_TO_Products]->(Products:Products)   RETURN Orders.OrderDate, Products.ProductID, Products.ProductName, Products.UnitPrice"
#     with open(f'schema/northwind-general.json', 'r') as f:
#         schema = json.load(f)
#     load_neo(schema)
#     driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
#     print(run_cypher_query(driver,cypher_query))
    

    
    
    
    
    
# run_cypher_query_endpoint()