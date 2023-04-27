import os
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from sqlalchemy import create_engine, MetaData , Table, distinct, func, inspect , select
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import text
from sqlalchemy.orm import class_mapper
import sqlite3
import json
import re
from pymongo import MongoClient
from neo4j import GraphDatabase
from py2neo import Graph
import pandas as pd
import pprint
from sql2general import execute_sql_file , get_list_of_table_names, determine_entity_relation, direction_of_relation,get_mapping_cardinality,get_attributes_and_constraints,get_data
from general2mongo import delete_all_collections, general2mongo,load_schema_data_in_mongo,convert_general_to_mongo
from general2neo import create_neo4j_nodes, create_neo4j_relationships
from generateDatalog import generate_datalog_query
from datalog2sql import datalog_to_sql
from datalog2mongo import datalog_to_mongo
from datalog2cypher import datalog_to_cypher
import ast
from runneo4j import run_cypher_query
from runquery import run_SQL_query
app = Flask(__name__)
CORS(app)

@app.route('/home')
def home():
    return render_template('index.html')

@app.route('/sqlschema', methods=['POST'])
def sqlschema():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Empty file name provided'})
    
    if not file.filename.endswith('.sql'):
        return jsonify({'error': 'Invalid file type'})
    
    schema_dir = 'schema'
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir)

    file_path = os.path.join(schema_dir, file.filename)
    file.save(file_path)
    filename = file.filename
    #Creating a sqlite database
    print(os.listdir("schema")) 
    # con = sqlite3.connect(f"schema/{filename[:-4]}.db")
    # cur = con.cursor()
    # with open(f"schema/{filename}", 'r') as sql_file:
    #     sql = sql_file.read()
    #     cur.executescript(sql)
    
# Replace the placeholders with your database credentials
    engine = create_engine(f'sqlite:///schema/{filename[:-4]}.db?')
    with open(f'schema/{filename[:-4]}.sql', "r") as sql_file:
        sql = sql_file.read()

    # Split the SQL script into separate statements
    statements = sql.split(';')

    with engine.begin() as conn:
        for statement in statements:
            # Execute each statement
            conn.execute(statement)

    # execute_sql_file(f'schema/{filename[:-4]}',engine)
    inspector = inspect(engine)
    # cursor = engine
    metadata = MetaData()
    metadata.reflect(bind=engine)
    schema = {}
    table_names = get_list_of_table_names(inspector)
    for table in table_names:
        schema[table] =  {'Type' : determine_entity_relation(table,table_names,metadata) , 'Cardinality' : get_mapping_cardinality(table,metadata) , 'Attributes' : get_attributes_and_constraints(table,engine) ,'From' : direction_of_relation(table,table_names,metadata)[0],'To' : direction_of_relation(table,table_names,metadata)[1], 'Data' : get_data(table,engine,metadata)} 
   
    with open(f'schema/{filename[:-4]}-general.json', 'w') as json_file:
        json.dump(schema, json_file, indent=4)
    with open(f'schema/{filename[:-4]}-general.json','r') as f:
        schema = json.load(f)
    
    # table_name_attr = retrieve_info(f'sqlite:///schema/{filename[:-4]}.db?')
    table_name_attr = retrieve_info(f'{filename[:-4]}.db')
    load_neo(schema)
    load_mongo(schema)
    # print(neo4j,mongo)
    # return jsonify({'Loaded' : schema })\
    print(table_name_attr)
    return table_name_attr

def load_mongo(schema):
    client = MongoClient('mongodb://localhost:27017/')
    db = client["mydatabase"]
    # with open(f'schema/{file_name}','r') as f:
    #     schema = json.load(f)
    delete_all_collections("mydatabase")
    general2mongo(schema,db)
    # return "MongoDB - Loaded"
    
def load_neo(schema):
    # with open('example.json','r') as f:
    #     schema = json.load(f)
    graph = Graph("bolt://localhost:7687", auth=("neo4j","12345678"))
    graph.delete_all()
    create_neo4j_nodes(schema,graph)
    create_neo4j_relationships(schema,graph)
    
    # return "Neo4j - Loaded"

@app.route('/listschemas')
def get_schemas():
    directory = '/Users/vikramkini/CS597/sql2generalise/backend/schema' # Replace with the path to your directory
    files = [f.replace('-general.json','') for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.endswith('.json')]
    return jsonify({'fileList': files})

@app.route('/show_schema/<string:schema_name>', methods=['GET'])
# def show_schema(schema_name):
#     schema_path = os.path.join('schema', f'{schema_name}-general.json')
#     if os.path.exists(schema_path):
#         with open(schema_path, 'r') as f:
#             schema = json.load(f)
        
#         return jsonify(schema)
#     else:
#         return jsonify({'error': 'Schema not found'}), 404
def show_schema(schema_name):
    schema_path = os.path.join('schema', f'{schema_name}-general.json')
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        # Iterate over all keys in the schema
        for key in schema.keys():
            # Remove the "Data" key from the current dictionary if it exists
            if 'Data' in schema[key]:
                del schema[key]['Data']
        
        return jsonify(schema)
    else:
        return jsonify({'error': 'Schema not found'}), 404


@app.route('/get_table_attrs/<string:file_name>' ,methods = ['GET'])
def retrieve_info(file_name):
    # Create a SQLAlchemy engine for the database
    database_path = f'sqlite:///schema/{file_name}?'
    engine = create_engine(database_path)

    # Create a SQLAlchemy inspector to get table information
    inspector = inspect(engine)

    # Get all table names in the database
    tables = inspector.get_table_names()

    table_attr_dict = {}

    # Iterate over each table
    for table in tables:
        # Get all column names in the table
        columns = [column['name'] for column in inspector.get_columns(table)]
        table_attr_dict[table] = columns
    # table_attr_dict.pop('sqlite_sequence')
    return jsonify(table_attr_dict)


@app.route('/generate_datalog', methods=['POST'])
def generate_datalog():
    data = request.json
    schema_name = data['schemaName']
    tables = data['tables']
    attributes = data['attributes']
    conditions = data['conditions']
    print(schema_name,tables,attributes,conditions)
    with open(f'schema/{schema_name}-general.json', 'r') as f:
        schema = json.load(f)

    query = generate_datalog_query(schema, tables, attributes, conditions)
    return jsonify({'query': query})

@app.route('/datalog_to_sql', methods=['POST'])
def datalog_to_sql_endpoint():
    data = request.json
    tables = data['tables']
    query = data['query']
    SQL_query = datalog_to_sql(tables, query)
    print(SQL_query)
    return jsonify({'SQL_query': SQL_query})

@app.route('/run_sql_query', methods=['POST'])
def run_sql_query_endpoint():
    data = request.json
    schema_name = data['schemaName']
    sql_query = data['sqlQuery']
    database_path = f'schema/{schema_name}.db'
    result = run_SQL_query(database_path, sql_query)
    print(result)
    return jsonify({'data':result})
    # result_list = [dict(row) for row in result]
    # return jsonify({'data': result_list})


@app.route('/datalog_to_mongo', methods=['POST'])
def datalog_to_mongo_endpoint():
    data = request.json
    tables = data['tables']
    query = data['query']
    mongo_query = datalog_to_mongo(tables,query)
    # print(query)
    return jsonify({'mongo_query': mongo_query})

@app.route('/run_mongo_query', methods=['POST'])
def run_mongo_query_endpoint():
    client = MongoClient('mongodb://localhost:27017/')
    db = client["mydatabase"]
    data = request.json
    filename = data.get('filename')
    query = data.get('query')

    with open(f'schema/{filename}-general.json', 'r') as f:
        schema = json.load(f)
    load_mongo(schema)
    collection = re.findall(r'db\.(\w+)\.aggregate', query)[0]

    # Extract aggregation pipeline
    pipeline = re.findall(r'aggregate\((.+)\)', query)[0]
    print(pipeline)

    # Convert the pipeline string into a list
    pipeline = re.sub(r'(?<!")(\$\w+|from|localField|foreignField|as)(?!")', r'"\1"', pipeline)
    pipeline = eval(pipeline)
    
    print(pipeline)

    try:
        result = list(db[collection].aggregate(pipeline))
    except Exception as e:
        print("An error occurred while executing the query:", e)
        result = []
    return jsonify({'data': result})

@app.route('/datalog_to_cypher', methods=['POST'])
def convert_datalog_to_cypher():
    data = request.get_json()
    query = data['query']
    filename = data['filename']
    with open(f'schema/{filename}-general.json', 'r') as f:
        schema = json.load(f)
    cypher_query = datalog_to_cypher(schema,query)
    return jsonify({'cypher_query': cypher_query})

# @app.route('/run_cypher_query', methods=['POST'])
# def run_cypher_query_endpoint():
#     data = request.json
#     schema_name = data['schemaName']
#     cypher_query = data['cypherQuery']
    
#     with open(f'schema/{schema_name}-general.json', 'r') as f:
#         schema = json.load(f)
#     load_neo(schema)
#     driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))

#     def execute_query(tx, query):
#         result = tx.run(query)
#         return [record.data() for record in result]

#     with driver.session() as session:
#         result = session.execute_read(execute_query, cypher_query)
#     print(result)
#     return jsonify({'data': result})

@app.route('/run_cypher_query', methods=['POST'])
def run_cypher_query_endpoint():
    data = request.json
    schema_name = data['schemaName']
    cypher_query = data['cypherQuery']

    with open(f'schema/{schema_name}-general.json', 'r') as f:
        schema = json.load(f)
    load_neo(schema)
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
    data = run_cypher_query(driver,cypher_query)
    return jsonify({"data" : data})

if __name__ == '__main__':
    app.debug = True
    app.run()
