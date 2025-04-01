import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import numpy as np
import re
import pymongo
import json
from datetime import datetime
import sqlite3
import io

def parse_table(html_content):
    """
    Parse tables that match the specified attributes from HTML content.
    Returns a list of pandas DataFrames, one for each matching table.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find tables with the specified attributes
    tables = soup.find_all('table', {
        'cellspacing': '0',
        'cellpadding': '3',
        'rules': 'cols',
        'id': 'gv_detail',
        'style': lambda value: value and 'color:Black;background-color:White;border-color:#EFE6F7;border-width:1px;border-style:Solid;font-size:9pt;border-collapse:collapse;' in value
    })
    
    if not tables:
        print("No matching tables found!")
        return []
    
    dataframes = []
    
    for table_num, table in enumerate(tables, 1):
        # Extract headers
        headers = []
        header_row = table.find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # Extract rows
        rows = []
        data_rows = table.find_all('tr')[1:] if headers else table.find_all('tr')
        
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            row_data = [cell.get_text(strip=True) for cell in cells]
            rows.append(row_data)
        
        # Create pandas DataFrame
        if headers and len(headers) == len(rows[0]) if rows else False:
            df = pd.DataFrame(rows, columns=headers)
        else:
            df = pd.DataFrame(rows)
        
        # Process the class code column and fill empty cells
        fill_empty_class_codes(df)
        
        # Convert period format for SQL compatibility
        convert_period_for_sql(df)
        
        # Extract day, start time, and end time from Period column
        extract_time_and_day(df)
        
        print(f"Table {table_num} parsed successfully with {len(df)} rows")
        dataframes.append(df)
    
    return dataframes

def fill_empty_class_codes(df):
    """
    When class code column is empty:
    1. Fill it with the value from the previous row
    2. Copy all empty cells in that row from the previous row
    """
    # Try to find class code column by common names
    class_code_col = None
    potential_names = ['class code', 'classcode', 'class_code', 'code', 'course code', 'coursecode']
    
    for col in df.columns:
        if any(name in col.lower() for name in potential_names):
            class_code_col = col
            break
    
    if class_code_col is None and len(df.columns) > 0:
        # If we couldn't find it by name, ask user to identify the class code column
        print("\nCouldn't automatically identify the class code column. Available columns are:")
        for i, col in enumerate(df.columns):
            print(f"{i}: {col}")
        
        try:
            col_idx = int(input("Enter the number of the class code column (or -1 if none exists): "))
            if 0 <= col_idx < len(df.columns):
                class_code_col = df.columns[col_idx]
        except ValueError:
            print("Invalid input. Proceeding without class code identification.")
    
    if class_code_col:
        print(f"Identified class code column as: {class_code_col}")
        
        # Process each row sequentially
        prev_row = None
        for i in range(len(df)):
            current_code = df.iloc[i][class_code_col]
            
            # If the class code is empty
            if pd.isna(current_code) or current_code == '':
                if prev_row is not None:
                    # Fill the class code with previous row's value
                    df.at[i, class_code_col] = prev_row[class_code_col]
                    
                    # For all other columns that are empty in current row, 
                    # copy values from previous row
                    for col in df.columns:
                        current_val = df.iloc[i][col]
                        if pd.isna(current_val) or current_val == '':
                            df.at[i, col] = prev_row[col]
            
            # Update previous row reference
            prev_row = df.iloc[i].copy()
    else:
        # If we couldn't identify the class code column,
        # just forward fill all empty cells in the DataFrame
        print("No class code column identified. Forward-filling all empty cells.")
        for col in df.columns:
            df[col] = df[col].replace('', np.nan).fillna(method='ffill')

def convert_period_for_sql(df):
    """
    Convert period column formats to be SQL-friendly.
    Looks for period columns and reformats them.
    """
    # Find period column
    period_col = None
    for col in df.columns:
        if 'period' in col.lower():
            period_col = col
            break
    
    if period_col:
        print(f"Found period column: {period_col}")
        
        # Clean and standardize period values
        df[period_col] = df[period_col].apply(lambda x: sanitize_period(x))
        
        # Create a new column with SQL-friendly period if needed
        new_col = f"{period_col}_sql"
        df[new_col] = df[period_col].apply(lambda x: convert_to_sql_period(x))
        
        print(f"Created SQL-friendly period column: {new_col}")
    else:
        print("No period column found")

def extract_time_and_day(df):
    """
    Extract day, start time, and end time from Period column.
    Example input: "Th 09:30AM - 12:15PM"
    Output: New columns with day (1-7), start_time (0-2400), end_time (0-2400)
    """
    # Find period column
    period_col = None
    for col in df.columns:
        if 'period' in col.lower():
            period_col = col
            break
    
    if not period_col:
        print("No period column found for time and day extraction.")
        return
    
    print(f"Extracting time and day from column: {period_col}")
    
    # Define day mapping (standard academic week starts with Monday=1)
    day_mapping = {
        'mo': 1, 'mon': 1, 'monday': 1, 'm': 1,
        'tu': 2, 'tue': 2, 'tues': 2, 'tuesday': 2, 't': 2,
        'we': 3, 'wed': 3, 'wednesday': 3, 'w': 3,
        'th': 4, 'thu': 4, 'thur': 4, 'thurs': 4, 'thursday': 4,
        'fr': 5, 'fri': 5, 'friday': 5, 'f': 5,
        'sa': 6, 'sat': 6, 'saturday': 6, 's': 6,
        'su': 7, 'sun': 7, 'sunday': 7
    }
    
    # Function to extract day code
    def extract_day(period_str):
        if pd.isna(period_str) or not period_str:
            return None
        
        # Look for day abbreviation at the beginning of the string
        match = re.search(r'^([a-zA-Z]{1,3})\s', str(period_str))
        if match:
            day_abbr = match.group(1).lower()
            # Check if we have an exact match
            if day_abbr in day_mapping:
                return day_mapping[day_abbr]
            
            # Check for partial matches
            for key in day_mapping:
                if key.startswith(day_abbr):
                    return day_mapping[key]
        return None
    
    # Function to extract time in 24-hour format (0-2400)
    def extract_time(time_str):
        if pd.isna(time_str) or not time_str:
            return None
        
        # Match time pattern like "09:30AM" or "9:30 AM" or "9:30am"
        match = re.search(r'(\d{1,2}):?(\d{2})?\s*(am|pm|AM|PM)?', str(time_str))
        if not match:
            return None
        
        hour = int(match.group(1))
        minute = int(match.group(2)) if match.group(2) else 0
        ampm = match.group(3).lower() if match.group(3) else None
        
        # Convert to 24-hour format
        if ampm == 'pm' and hour < 12:
            hour += 12
        elif ampm == 'am' and hour == 12:
            hour = 0
        
        # Format as HHMM (0-2400)
        return hour * 100 + minute
    
    # Function to extract both start and end times
    def extract_start_end_times(period_str):
        if pd.isna(period_str) or not period_str:
            return None, None
        
        # Split by common delimiters
        parts = re.split(r'\s*[\-–—]\s*', str(period_str))
        
        # If we have at least two parts (before and after delimiter)
        if len(parts) >= 2:
            # Find the parts with time patterns
            time_parts = []
            for part in parts:
                if re.search(r'\d{1,2}[:.]?\d{0,2}\s*(am|pm|AM|PM)?', part):
                    time_parts.append(part)
            
            if len(time_parts) >= 2:
                start_time = extract_time(time_parts[0])
                end_time = extract_time(time_parts[1])
                return start_time, end_time
        
        # If we couldn't split properly, try to find both times in the string
        times = re.findall(r'(\d{1,2}[:.]?\d{0,2}\s*(am|pm|AM|PM)?)', str(period_str))
        if len(times) >= 2:
            start_time = extract_time(times[0][0])
            end_time = extract_time(times[1][0])
            return start_time, end_time
        
        return None, None
    
    # Apply extraction functions to create new columns
    df['day_num'] = df[period_col].apply(extract_day)
    
    # Extract start and end times
    start_times = []
    end_times = []
    
    for period in df[period_col]:
        start, end = extract_start_end_times(period)
        start_times.append(start)
        end_times.append(end)
    
    df['start_time'] = start_times
    df['end_time'] = end_times
    
    # Print statistics
    day_count = df['day_num'].count()
    start_count = df['start_time'].count()
    end_count = df['end_time'].count()
    total_rows = len(df)
    
    print(f"Extracted days for {day_count}/{total_rows} rows ({day_count/total_rows*100:.1f}%)")
    print(f"Extracted start times for {start_count}/{total_rows} rows ({start_count/total_rows*100:.1f}%)")
    print(f"Extracted end times for {end_count}/{total_rows} rows ({end_count/total_rows*100:.1f}%)")

def sanitize_period(period_str):
    """Clean the period string"""
    if pd.isna(period_str) or period_str == '':
        return ''
    
    # Remove extra spaces and standardize format
    period_str = re.sub(r'\s+', ' ', str(period_str).strip())
    return period_str

def convert_to_sql_period(period_str):
    """Convert a period string to SQL-friendly format"""
    if pd.isna(period_str) or period_str == '':
        return None
    
    # Replace periods, slashes, etc. with underscores
    sql_period = re.sub(r'[\.\/\s\-\:]', '_', str(period_str))
    
    # Remove any other non-alphanumeric chars
    sql_period = re.sub(r'[^a-zA-Z0-9_]', '', sql_period)
    
    # Ensure it doesn't start with a number
    if sql_period and sql_period[0].isdigit():
        sql_period = 'p_' + sql_period
    
    return sql_period

def load_to_mongodb(dataframes, mongodb_uri=None, db_name=None, collection_prefix=None):
    """
    Load dataframes into MongoDB collections
    """
    # Get MongoDB connection parameters if not provided
    if not mongodb_uri:
        mongodb_uri = input("Enter MongoDB URI (default: mongodb://localhost:27017): ") or "mongodb://localhost:27017"
    
    if not db_name:
        db_name = input("Enter database name (default: parsed_tables): ") or "parsed_tables"
    
    if not collection_prefix:
        collection_prefix = input("Enter collection prefix (default: table): ") or "table"
    
    # Connect to MongoDB
    try:
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        print(f"Connected to MongoDB database: {db_name}")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False
    
    # Create timestamp for versioning
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load each dataframe into MongoDB
    for i, df in enumerate(dataframes, 1):
        # Create collection name
        collection_name = f"{collection_prefix}_{i}_{timestamp}"
        
        # Convert DataFrame to list of dictionaries
        records = json.loads(df.to_json(orient='records'))
        
        # Add metadata to each record
        for record in records:
            record['_imported_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record['_source'] = 'html_parser'
            record['_table_index'] = i
        
        # Insert into MongoDB
        try:
            collection = db[collection_name]
            collection.insert_many(records)
            print(f"Inserted {len(records)} records into collection: {collection_name}")
        except Exception as e:
            print(f"Error inserting into MongoDB: {e}")
    
    return True

def load_to_sqlite_memory(dataframes):
    """
    Load dataframes into an in-memory SQLite database
    Returns the connection object for further use
    """
    # Create in-memory SQLite database
    conn = sqlite3.connect(':memory:')
    print("Created in-memory SQLite database")
    
    # Create timestamp for table naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load each dataframe into a SQLite table
    for i, df in enumerate(dataframes, 1):
            
        # Create table name
        table_name = f"table_{i}_{timestamp}"
        
        # Add metadata columns
        # df['_imported_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # df['_source'] = 'html_parser'
        # df['_table_index'] = i
        
        # Write to SQLite
        try:
            df.to_sql(table_name, conn, index=False)
            print(f"Created table '{table_name}' with {len(df)} rows in in-memory SQLite")
            
            # Verify data was loaded
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"Verified {count} records in table")
            
            # Show table structure
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print(f"Table structure: {[col[1] for col in columns]}")
        except Exception as e:
            print(f"Error creating SQLite table: {e}")
    
    print("\nIn-memory SQLite database is ready for querying.")
    print("Note: This database will be lost when the program exits.")
    print("Example query: SELECT * FROM table_1_... LIMIT 5")
    
    return conn

def load_to_pandas_store(dataframes):
    """
    Stores dataframes in an in-memory pandas HDFStore
    Returns the store object for further use
    """
    # Create in-memory HDFStore
    store = pd.HDFStore('memory:///parsed_tables', mode='w')
    print("Created in-memory pandas HDFStore")
    
    # Create timestamp for key naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Store each dataframe
    for i, df in enumerate(dataframes, 1):
        # Create key name
        key = f"/table_{i}_{timestamp}"
        
        # # Add metadata columns
        # df['_imported_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # df['_source'] = 'html_parser'
        # df['_table_index'] = i
        
        # Write to store
        try:
            store.put(key, df)
            print(f"Stored dataframe at key '{key}' with {len(df)} rows")
        except Exception as e:
            print(f"Error storing dataframe: {e}")
    
    print("\nIn-memory pandas HDFStore is ready for access.")
    print("Note: This store will be lost when the program exits.")
    print("Available keys:", store.keys())
    
    return store

def interactive_sqlite_session(conn):
    """
    Provide an interactive SQLite query session for the in-memory database
    """
    cursor = conn.cursor()
    
    # List available tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("\nAvailable tables:")
    for table in tables:
        print(f"- {table[0]}")
    
    # Interactive query session
    print("\nEnter SQL queries (type 'exit' to quit):")
    while True:
        query = input("\nSQL> ")
        if query.lower() in ('exit', 'quit', 'q'):
            break
        
        try:
            cursor.execute(query)
            if query.lower().strip().startswith(('select', 'pragma', 'explain')):
                results = cursor.fetchall()
                if results:
                    # Get column names
                    column_names = [description[0] for description in cursor.description]
                    print("\nColumns:", column_names)
                    
                    # Print results in a formatted way
                    for i, row in enumerate(results[:20]):  # Limit to first 20 rows
                        print(f"Row {i}:", row)
                    
                    if len(results) > 20:
                        print(f"... and {len(results) - 20} more rows")
                    
                    print(f"\nTotal: {len(results)} rows returned")
                else:
                    print("Query returned no results")
            else:
                conn.commit()
                print(f"Query executed successfully. Rows affected: {cursor.rowcount}")
        except Exception as e:
            print(f"Error: {e}")
    
    print("Exiting SQLite session")

def main(file_path, filename=None):

    with open(file_path, 'r', encoding='utf-8') as file:
                    html_content = file.read()
    
    # Parse tables
    dataframes = parse_table(html_content)
    
    if not dataframes:
        print("No tables found to process.")
        return
    
    # Display and save results
    for i, df in enumerate(dataframes, 1):
        # Clean column names for SQLite compatibility
        df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col) for col in df.columns]
        print(f"\nTable {i} preview:")
        # Set display options for this specific output
        with pd.option_context('display.max_columns', None, 
                              'display.width', None,
                              'display.max_colwidth', None):
            print(df.head())
        
        # Show column info
        print(f"\nColumns in Table {i}:")
        for col_num, col_name in enumerate(df.columns):
            print(f"{col_num}: {col_name}")
    
    # Ask about storage choice
    # print("\nChoose a storage option:")
    # print("1. In-memory SQLite (with interactive query session)")
    # print("2. In-memory pandas HDFStore")
    # print("3. MongoDB")
    # print("4. Save to CSV only")
    
    # storage_choice = input("Enter choice (1-4): ")
    storage_choice = '4'


    if storage_choice == '1':
        conn = load_to_sqlite_memory(dataframes)
        interactive_sqlite_session(conn)
    elif storage_choice == '2':
        store = load_to_pandas_store(dataframes)
        print("\nDataframes are stored in memory.")
        print("You can access them from the 'store' variable if running in interactive mode.")
    elif storage_choice == '3':
        load_to_mongodb(dataframes)
    elif storage_choice == '4':
        pass  # Proceed to CSV option
    else:
        print("Invalid choice, defaulting to CSV option")

    import os    
    # Ask about saving to CSV

    for i, df in enumerate(dataframes, 1):
        if filename == None:
          filename = input(f"Enter filename for Table {i} (default: table_{i}.csv): ") or f"table_{i}.csv"
        
        # Check if file exists
        file_exists = False
        try:
            file_exists = os.path.isfile(filename)
        except:
            pass
        
        if file_exists:
            # append_option = input(f"File '{filename}' already exists. Append to it? (y/n): ")
            # if append_option.lower() == 'y':
                # Read existing file to check headers
                existing_df = pd.read_csv(filename)
                
                # Check if columns match
                if set(existing_df.columns) == set(df.columns):
                    # Append without writing headers
                    df.to_csv(filename, mode='a', header=False, index=False)
                    print(f"Data appended to {filename}")
                else:
                    print("Column mismatch between existing file and new data.")
                    print("old set:", set(existing_df.columns), "new set: ", set(df.columns))
                    column_option = input("Save anyway (may cause data structure issues)? (y/n): ")
                    if column_option.lower() == 'y':
                        df.to_csv(filename, mode='a', header=False, index=False)
                        print(f"Data appended to {filename} (with column mismatch)")
                    else:
                        alt_filename = f"new_{filename}"
                        df.to_csv(alt_filename, index=False)
                        print(f"Data saved to {alt_filename} instead")
        else:
            # New file
            df.to_csv(filename, index=False)
            print(f"Table saved to {filename}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
      file_path = sys.argv[1]
      main(file_path, "new.csv")
    else:
      print("example usage: python save.py acct.html")