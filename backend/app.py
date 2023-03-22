import os
from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine, MetaData , Table, distinct, func, inspect , select
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import text
from sqlalchemy.orm import class_mapper
import sqlite3
import json
from sql2general import get_list_of_table_names, determine_entity_relation, direction_of_relation,get_mapping_cardinality,get_attributes_and_constraints,get_data
app = Flask(__name__)

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
    con = sqlite3.connect(f"schema/{filename[:-4]}.db")
    cur = con.cursor()
    with open(f"schema/{filename}", 'r') as sql_file:
        sql = sql_file.read()
        cur.executescript(sql)
    
# Replace the placeholders with your database credentials
    engine = create_engine(f'sqlite:///schema/{filename[:-4]}.db?')
    inspector = inspect(engine)
    cursor = engine
    metadata = MetaData()
    metadata.reflect(bind=engine)
    schema = {}
    table_names = get_list_of_table_names(inspector)
    for table in table_names:
        schema[table] =  {'Type' : determine_entity_relation(table,table_names,metadata) , 'Cardinality' : get_mapping_cardinality(table,metadata) , 'Attributes' : get_attributes_and_constraints(table,engine) ,'From' : direction_of_relation(table,table_names,metadata)[0],'To' : direction_of_relation(table,table_names,metadata)[1], 'Data' : get_data(table,engine,metadata)} 
   
    with open(f'schema/{filename[:-4]}-general.json', 'w') as json_file:
        json.dump(schema, json_file, indent=4)
    
    return jsonify({'Loaded' : schema})


if __name__ == '__main__':
    app.debug = True
    app.run()
