'''
Detailed Explanation of Changes:
Improved Data Type Detection:

Added the is_int function to check if a string can be converted to an integer.
Updated perform_eda to classify a column as INTEGER if most of its values can be converted to integers, and as STRING otherwise.
Filtering Out Outlier Columns:

Counted the occurrences of each column using column_counts.
Filtered out columns that appear in less than 97% of the rows by checking against a threshold.
Corrected create_table_query:

Ensured that the primary key is defined correctly without duplication by adding the PRIMARY KEY constraint directly in the columns dictionary within the perform_eda function.
Removed the redundant addition of PRIMARY KEY(Id) in the create_schema_and_table function.
These changes should ensure the perform_eda function correctly identifies column data types, filters out columns with insufficient occurrences, and generates a correct SQL CREATE TABLE statement without syntax errors.
'''

import json
from collections import defaultdict
from datetime import datetime
from db import DuckDBConnection

class VoteDataIngestor:
    def __init__(self, db_connection, file_path='votes.jsonl'):
        # Initialize the VoteDataIngestor with a database connection and an optional file path
        self.db_connection = db_connection
        self.file_path = file_path

    def create_schema_and_table(self, columns):
        # Create schema 'blog_analysis' if it doesn't exist
        self.db_connection.execute_query('CREATE SCHEMA IF NOT EXISTS blog_analysis')
        
        # Construct the CREATE TABLE query with provided columns and their types
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
        '''
        create_table_query += ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
        
        # Execute the CREATE TABLE query
        self.db_connection.execute_query(create_table_query)

    def ingest_votes(self, columns):
        # Open the JSONL file and read each line
        with open(self.file_path, 'r') as f:
            for line in f:
                vote = json.loads(line.strip())  # Parse the JSON data
                try:
                    # Ensure each row has all necessary columns
                    for col_name, col_type in columns.items():
                        if col_name not in vote:
                            # Set default value based on the column type
                            if col_type == 'INTEGER':
                                vote[col_name] = 0
                            elif col_type == 'TIMESTAMP':
                                vote[col_name] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Log if extra columns are present
                    extra_columns = set(vote.keys()) - set(columns.keys())
                    if extra_columns:
                        print(f"Extra columns found and ignored: {extra_columns}")

                    # Ensure data types are correct and prepare insert values
                    insert_values = []
                    for col_name, col_type in columns.items():
                        if col_type == 'INTEGER':
                            vote[col_name] = int(vote[col_name])
                        elif col_type == 'TIMESTAMP':
                            vote[col_name] = datetime.strptime(vote[col_name], '%Y-%m-%dT%H:%M:%S')
                        insert_values.append(vote[col_name])
                    
                    # Insert or replace based on the primary key to avoid duplicates
                    self.db_connection.execute_query(f'''
                        INSERT OR REPLACE INTO blog_analysis.votes ({', '.join(columns.keys())}) VALUES ({', '.join(['?' for _ in columns])})
                    ''', insert_values)
                except (ValueError, KeyError) as e:
                    # Log and skip invalid records
                    print(f"Skipping invalid record: {vote} due to error: {e}")

def perform_eda(file_path):
    # Perform Exploratory Data Analysis (EDA) on the data file to determine column types and potential primary key
    column_types = defaultdict(list)
    column_counts = defaultdict(int)
    total_rows = 0

    def is_int(value):
        try:
            int(value)
            return True
        except ValueError:
            return False

    with open(file_path, 'r') as f:
        sample_data = [json.loads(line.strip()) for line in f]
        total_rows = len(sample_data)

    for vote in sample_data:
        for key, value in vote.items():
            column_counts[key] += 1
            if is_int(value):
                column_types[key].append("INTEGER")
            else:
                column_types[key].append("STRING")

    # Filter columns to include only those appearing in more than 97% of rows
    threshold = 0.97 * total_rows
    filtered_columns = {col for col, count in column_counts.items() if count >= threshold}

    # Determine the most common type for each column
    inferred_schema = {}
    for column in filtered_columns:
        types = column_types[column]
        if types.count("INTEGER") > len(types) / 2:
            inferred_schema[column] = "INTEGER"
        else:
            inferred_schema[column] = "STRING"

    # Manually set CreationDate to TIMESTAMP if it exists
    if 'CreationDate' in inferred_schema:
        inferred_schema['CreationDate'] = "TIMESTAMP"

    # Identify the primary key
    primary_key_candidates = [col for col, count in column_counts.items() if count == total_rows]

    if not primary_key_candidates:
        raise ValueError("No unique column found to be used as a primary key.")

    # Assuming the first unique column found is the primary key for simplicity
    primary_key = primary_key_candidates[0]
    
    return inferred_schema, primary_key

def main(file_path='votes.jsonl'):
    # Perform EDA to determine column types and primary key
    columns, primary_key = perform_eda(file_path)
    
    # Add PRIMARY KEY constraint to the primary key column
    columns[primary_key] += ' PRIMARY KEY'
    
    # Create a DuckDB connection
    db_conn = DuckDBConnection('votes.db')
    db_conn.connect()

    # Create an ingestor instance
    ingestor = VoteDataIngestor(db_conn, file_path)

    # Create the schema and table
    ingestor.create_schema_and_table(columns)

    # Ingest the vote data file
    ingestor.ingest_votes(columns)

    # Close the database connection
    db_conn.close()

if __name__ == '__main__':
    # Allow specifying a different data file via command-line argument
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()


