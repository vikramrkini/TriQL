import sqlite3
from py2neo import Graph, Node, Relationship

# Set up connection to the Neo4j database
graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))

# Function to create nodes from SQLite tables
def create_nodes(cursor, table_name):
    cursor.execute(f"SELECT * FROM '{table_name}'")
    columns = [column[0] for column in cursor.description]
    
    nodes = []
    for row in cursor.fetchall():
        node_properties = {columns[i]: row[i] for i in range(len(columns))}
        node = Node(table_name, **node_properties)
        graph.create(node)
        nodes.append(node)
    
    return nodes

# Function to get primary key column for a table
def get_primary_key_column(cursor, table_name):
    cursor.execute(f"PRAGMA table_info('{table_name}');")
    table_info = cursor.fetchall()
    for column_info in table_info:
        if column_info[5] == 1:
            return column_info[1]
    return None

# Function to get foreign key relationships
def get_foreign_keys(cursor, table_name):
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
    foreign_keys = cursor.fetchall()
    return [(fk[3], fk[4]) for fk in foreign_keys]

# Read the SQL file and parse the tables
sql_file = "northwind.sql"
conn = sqlite3.connect(":memory:")
conn.executescript(open(sql_file, "r").read())
cursor = conn.cursor()

# Get the list of tables in the SQLite database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [table[0] for table in cursor.fetchall()]

# Create nodes for each table and extract foreign keys
table_nodes = {}
table_foreign_keys = {}
for table in tables:
    table_nodes[table] = create_nodes(cursor, table)
    table_foreign_keys[table] = get_foreign_keys(cursor, table)

# Create relationships based on foreign keys
for table, foreign_keys in table_foreign_keys.items():
    for (fk_column, ref_table) in foreign_keys:
        ref_pk_column = get_primary_key_column(cursor, ref_table)

        # Create relationships between nodes based on the foreign key column
        query = f"""
        MATCH (a:{table}), (b:{ref_table})
        WHERE a.{fk_column} = b.{ref_pk_column} AND a.{fk_column} IS NOT NULL
        MERGE (a)-[r:{table}_REFERENCES_{ref_table}]->(b)
        """
        graph.run(query)

# Close SQLite connection
conn.close()
