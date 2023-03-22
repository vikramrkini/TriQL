#To do : Fix the To and From fields. From will not be the table name
#Optimise the generalised schema more 




import json
from neo4j import GraphDatabase 
from py2neo import Graph

# Parse the Schema
with open('example.json','r') as f:
    schema = json.load(f)



graph = Graph("bolt://localhost:7687", auth=("neo4j","12345678"))

# Create nodes in the Neo4j database

def create_neo4j_nodes(schema):
  for table in schema:
      if schema[table]['Type'] == 'entity':
          data =  schema[table]['Data']
          d = data[0] if data else {}
          for key , value in d.items():
              if value == None : 
                  d[key] = 'NULL'
          d_str = ", ".join([f"{k}:'{v}'" if isinstance(v, str) else f"{k}:{v}" for k, v in d.items()])
          
          query = f"CREATE (:{table} {{{d_str}}})"
          result = graph.run(query)
          print(table + ' Done')
          # print(result)

# def create_neo4j_relationships(schema):
#     for table in schema:
#       if schema[table]['Type'] == 'relationship' and schema[table]['To']:
#           data = schema[table]['Data']
#           d = data[0] if data else {}
#           from_node = schema[table]['From']
#           for to_node in schema[table]['To'][1]:       
#             properties_str = ", ".join([f"{k}:'{v}'" if isinstance(v, str) else f"{k}:{v}" for k, v in d.items()])
#             query = f"MATCH (`{from_node}`:`{from_node}`), (`{to_node}`:(`{to_node}`)) CREATE (`{from_node}`)-[:RELATED_TO {{{properties_str}}}]->(`{to_node}`)"
#             result = graph.run(query).data()
#             print(result)
#             print(table + ' Done')
#               # print(result)
def create_neo4j_relationships(schema):
    for table in schema:
        if schema[table]['Type'] == 'relationship' and schema[table]['To']:
            data = schema[table]['Data']
            d = data[0] if data else {}
            from_node = schema[table]['To'][0][1]
            to_node = schema[table]['To'][1][1]

            properties_str = ", ".join([f"{k}:'{v}'" if isinstance(v, str) else f"{k}:{v}" for k, v in d.items()])
            query = f"MATCH (from:`{from_node}`), (to:`{to_node}`) CREATE (from)-[r:RELATED_TO {{{properties_str}}}]->(to) RETURN r"
            print(query) # Print the query to check for any errors
            result = graph.run(query).data()
            print(result)
            print(table + ' Done')
        elif schema[table]['Type'] == 'relationship' and not schema[table]['To']:
            print(f"No 'To' field found for relationship '{table}'") # Print a message for empty 'To' fields

            
            
create_neo4j_nodes(schema)
create_neo4j_relationships(schema)





# driver.close()
