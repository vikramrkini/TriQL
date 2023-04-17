import re
import json

def parse_sql_file(file_path):
    with open(file_path, 'r') as f:
        sql_data = f.read()
    
    # Remove comments
    sql_data = re.sub(r'--.*', '', sql_data)
    
    # Identify CREATE TABLE statements
    table_statements = re.findall(r'CREATE TABLE.*?;', sql_data, re.DOTALL)
    
    schema = {}
    
    for table_statement in table_statements:
        # Get table name
        table_name_match = re.search(r'CREATE TABLE\s+`?(\w+)`?', table_statement)
        if not table_name_match:
            print(f"Table name not found in statement: {table_statement}")
            continue
        table_name = table_name_match.group(1)

        
        # Get column definitions
        column_defs = re.findall(r'\w+ [A-Za-z]+\([0-9,]*\)|\w+ [A-Za-z]+', table_statement)
        
        # Extract column details
        columns = []
        for column_def in column_defs:
            column_name, column_type = column_def.split(' ', 1)
            columns.append({
                'name': column_name,
                'type': column_type,
                'nullable': True,
                'default': None,
                'autoincrement': None,
                'primary_key': 0
            })

        # Extract primary key constraint
        primary_key_constraint = re.search(r'PRIMARY KEY \((.*?)\)', table_statement)
        if primary_key_constraint:
            primary_key_columns = primary_key_constraint.group(1).split(',')
            for column in columns:
                if column['name'] in primary_key_columns:
                    column['primary_key'] = 1
        
        # Extract foreign key constraints
        foreign_key_constraints = re.findall(r'FOREIGN KEY \((.*?)\) REFERENCES (\w+)\((.*?)\)', table_statement)
        cardinalities = []
        for fk_constraint in foreign_key_constraints:
            cardinalities.append({
                'from_table': table_name,
                'from_column': fk_constraint[0],
                'to_table': fk_constraint[1],
                'to_column': fk_constraint[2],
                'type': 'OneToMany' # Assuming OneToMany by default, can be modified based on specific rules
            })
        
        # Identify if table is an entity or relationship
        if foreign_key_constraints:
            table_type = 'relationship'
        else:
            table_type = 'entity'
        
        schema[table_name] = {
            'Type': table_type,
            'Cardinality': cardinalities,
            'Attributes': columns,
            'From': table_name,
            'To': [constraint[1] for constraint in foreign_key_constraints],
            'Data': []
        }
        
    return schema

file_path = 'northwind.sql'
schema = parse_sql_file(file_path)

with open('generalized_schema.json', 'w') as f:
    json.dump(schema, f, indent=4)
