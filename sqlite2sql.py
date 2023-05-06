import subprocess
import os

def sqlite_to_sql_file(input_file, output_file):
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' does not exist")

    # Run the sqlite3 command-line utility to dump the .sqlite file into a .sql file
    try:
        result = subprocess.run(["sqlite3", input_file, ".dump"], capture_output=True, text=True, check=True)

        # Write the output to the .sql file
        with open(output_file, "w") as file:
            file.write(result.stdout)

        print(f"Successfully dumped '{input_file}' to '{output_file}'")

    except subprocess.CalledProcessError as e:
        print(f"Error: {e.output}")
        raise

# Example usage
input_file = "movies.sqlite"
output_file = "movies.sql"
sqlite_to_sql_file(input_file, output_file)
