import sqlite3
import pandas as pd
import sys
import os

def csv_to_sqlite(csv_file, db_file=None):
    # If no db file specified, use the csv name with .db extension
    if db_file is None:
        db_file = os.path.splitext(csv_file)[0] + '.db'
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found.")
        return None
    
    # Read the CSV file
    print(f"Reading data from {csv_file}...")
    df = pd.read_csv(csv_file)
    
    # Create a table name from the CSV filename
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
    
    # Connect to SQLite database
    print(f"Creating SQLite database at {db_file}...")
    conn = sqlite3.connect(db_file)
    
    # Write the data to SQLite
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Created table '{table_name}' with {len(df)} rows and {len(df.columns)} columns")
    
    # Display column names and types
    print("\nColumns:")
    for column in df.columns:
        print(f"- {column} ({df[column].dtype})")
    
    return conn, table_name

def run_query_prompt(conn, table_name):
    print(f"\nEnter SQL queries to run against the '{table_name}' table.")
    print("Type 'exit' to quit, 'help' for assistance, or 'schema' to see the table structure.")
    
    while True:
        query = input("\nSQL> ").strip()
        
        if query.lower() in ('exit', 'quit'):
            break
        elif query.lower() == 'help':
            print("\nExample queries:")
            print(f"- SELECT * FROM {table_name} LIMIT 5")
            print(f"- SELECT COUNT(*) FROM {table_name}")
            print(f"- SELECT column1, column2 FROM {table_name} WHERE column3 > 100")
        elif query.lower() == 'schema':
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            print("\nTable schema:")
            for col in cursor.fetchall():
                print(f"- {col[1]} ({col[2]})")
        elif query:
            try:
                # Execute the query
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Fetch and display results
                results = cursor.fetchall()
                if results:
                    # Get column names
                    column_names = [description[0] for description in cursor.description]
                    print("\nResults:")
                    print(' | '.join(column_names))
                    print('-' * (sum(len(name) for name in column_names) + 3 * (len(column_names) - 1)))
                    
                    # Print rows (limit to first 20)
                    MAX_ROWS = 100
                    for i, row in enumerate(results):
                        if i < MAX_ROWS:
                            print(' | '.join(str(value) for value in row))
                    
                    if len(results) > MAX_ROWS:
                        print(f"\n... and {len(results) - MAX_ROWS} more rows")
                    
                    print(f"\nTotal: {len(results)} rows")
                else:
                    if query.lower().startswith(('update', 'insert', 'delete')):
                        print(f"Query executed successfully. Rows affected: {cursor.rowcount}")
                    else:
                        print("Query returned no results.")
            except sqlite3.Error as e:
                print(f"Error executing query: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py result.csv [output.db]")
        return
    
    csv_file = sys.argv[1]
    db_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    result = csv_to_sqlite(csv_file, db_file)
    if result:
        conn, table_name = result
        run_query_prompt(conn, table_name)
        conn.close()

if __name__ == "__main__":
    main()