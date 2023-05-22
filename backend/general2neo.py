import json
from neo4j import GraphDatabase
from py2neo import Graph

def create_neo4j_nodes(schema, graph):
    for table in schema:
        if schema[table]['Type'] == 'entity':
            data = schema[table]['Data']
            for d in data:  # Iterate through the entire data list
                if d:
                    for key, value in d.items():
                        if value == None:
                            d[key] = 'NULL'
                        elif isinstance(value, str):
                            d[key] = value.replace("'", "\\'")  # Escape single quotes
                    d_str = ", ".join([f"{k}:'{v}'" if isinstance(v, str) else f"{k}:{v}" for k, v in d.items()])

                    query = f"CREATE (:{table} {{{d_str}}})"
                    result = graph.run(query)


def create_neo4j_relationships(schema, graph):
    for table in schema:
        if schema[table]['Type'] == 'relationship' and schema[table]['To']:
            # print(table)
            data = schema[table]['Data']
            d = data[0] if data else {}
            from_node = schema[table]['To'][0][1]
            to_node = schema[table]['To'][1][1]
            properties_str = ", ".join([f"{k}:'{v}'" if isinstance(v, str) else f"{k}:{v}" for k, v in d.items()])
            relationship_name = f"{from_node}_TO_{to_node}"  # Changed the relationship name format
            query = f"MATCH (from:`{from_node}`), (to:`{to_node}`) CREATE (from)-[r:{relationship_name} {{{properties_str}}}]->(to) RETURN r"
            result = graph.run(query).data()
        elif schema[table]['Type'] == 'relationship' and not schema[table]['To']:
            print(f"No 'To' field found for relationship '{table}'")
# Parse the Schema
# with open(f'schema/northwind-general.json','r') as f:
#         schema = json.load(f)

# graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))

# create_neo4j_nodes(schema, graph)
# create_neo4j_relationships(schema, graph)
