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
        create_table_query += ', PRIMARY KEY(Id))'  # Ensuring Id is the primary key to prevent duplicates
        
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
    with open(file_path, 'r') as f:
        sample_data = [json.loads(line.strip()) for line in f]

    column_types = {}
    value_counts = defaultdict(lambda: defaultdict(int))
    
    for vote in sample_data:
        for key, value in vote.items():
            # Determine column types
            if key not in column_types:
                if isinstance(value, int):
                    column_types[key] = 'INTEGER'
                elif isinstance(value, str):
                    try:
                        datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
                        column_types[key] = 'TIMESTAMP'
                    except ValueError:
                        column_types[key] = 'STRING'
            # Count occurrences of each value in the column
            value_counts[key][value] += 1

    # Identify the primary key
    primary_key_candidates = []
    for column, counts in value_counts.items():
        if all(count == 1 for count in counts.values()):
            primary_key_candidates.append(column)
    
    if not primary_key_candidates:
        raise ValueError("No unique column found to be used as a primary key.")

    # Assuming the first unique column found is the primary key for simplicity
    primary_key = primary_key_candidates[0]
    
    return column_types, primary_key

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
