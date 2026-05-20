import duckdb  # Import the DuckDB library to interact with DuckDB databases

# Connect to the DuckDB database file
conn = duckdb.connect(r"D:\Depi\Project\dbt\G3_project\dev.duckdb")

# Execute SQL query to get all tables in the database
tables = conn.execute("SHOW TABLES").fetchall()

# Print the names of all tables
print("Tables in the database:", [t[0] for t in tables])

# Loop through each table
for table in tables:
    table_name = table[0]  # Extract table name from tuple

    print(f"\nData from table '{table_name}':")

    # Select first 5 rows from the current table
    data = conn.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()

    # Print each row
    for row in data:
        print(row)

# Close the database connection
conn.close()
