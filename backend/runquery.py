import pandas as pd
from sqlalchemy import create_engine
import sqlite3
import re
# database_path = 'schema/northwind.db'
# sql_query = "SELECT Customers.CustomerID, Customers.CompanyName FROM Orders JOIN Customers ON Orders.CustomerID = Customers.CustomerID"

def run_SQL_query(database_path, query):
    # Connect to the database
    conn = sqlite3.connect(database_path)

    # Create a cursor object
    cursor = conn.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch all rows as a list of tuples
    result = cursor.fetchall()

    # Convert tuples to lists
    result = [list(item) for item in result]

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return result


