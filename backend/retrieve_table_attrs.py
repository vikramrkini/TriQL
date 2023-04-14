def retrieve_info(database_path):
    # Create a SQLAlchemy engine for the database
    engine = create_engine(f'sqlite:///{database_path}')

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

    return table_attr_dict