'''
Explanation of Changes:
Parameterized Table Name:

The OutlierDetector class now accepts a table_name parameter, which defaults to 'votes' if not provided.
The table_name is used in the SQL query to ensure the correct table is referenced.
Command-line Argument Handling:

In the main function, it checks if a table name is provided as a command-line argument. If not, it defaults to 'votes'.
Running the Script:
To run the script with a different table name, you can use:

sh
Copy code
python outliers.py your_table_name
If no table name is provided, it defaults to using 'votes'. This update ensures flexibility and aligns with the requirements for parameterizing the table name.
'''

from datetime import datetime
from db import DuckDBConnection
import sys

class OutlierDetector:
    def __init__(self, db_connection, table_name='votes'):
        self.db_connection = db_connection
        self.table_name = table_name
    
    def create_view(self):
        # Create schema if not exists
        self.db_connection.execute_query('CREATE SCHEMA IF NOT EXISTS blog_analysis')

        # Define the query to create the view
        query = f'''
        CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
        WITH vote_counts AS (
            SELECT 
                DATE_TRUNC('week', CreationDate) AS Week,
                COUNT(*) AS VoteCount
            FROM blog_analysis.{self.table_name}
            WHERE VoteTypeId = 2
            GROUP BY Week
        ),
        stats AS (
            SELECT 
                AVG(VoteCount) AS mean_votes,
                STDDEV_POP(VoteCount) AS stddev_votes
            FROM vote_counts
        )
        SELECT 
            STRFTIME(Week, '%Y') AS Year,
            STRFTIME(Week, '%W') AS WeekNumber,
            VoteCount
        FROM vote_counts, stats
        WHERE VoteCount > (mean_votes + 2 * stddev_votes)
        '''

        # Execute the query to create the view
        self.db_connection.execute_query(query)
    
    def detect_outliers(self):
        # Create the view
        self.create_view()

        # Verify the view has been created
        verify_query = '''
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type='VIEW' 
        AND table_name='outlier_weeks' 
        AND table_schema='blog_analysis';
        '''

        result = self.db_connection.execute_query(verify_query).fetchall()
        if len(result) != 1:
            raise Exception("View 'outlier_weeks' was not created successfully")

        # Verify the view has data
        data_check_query = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
        result = self.db_connection.execute_query(data_check_query).fetchall()
        if result[0][0] == 0:
            raise Exception("View 'outlier_weeks' has no data")

def main():
    # Check if a table name is provided as a command-line argument
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
    else:
        table_name = 'votes'  # Default table name

    # Initialize the DuckDB connection
    db_conn = DuckDBConnection()
    db_conn.connect()

    # Create the OutlierDetector instance
    detector = OutlierDetector(db_conn, table_name)

    # Detect outliers
    detector.detect_outliers()

    # Close the database connection
    db_conn.close()

if __name__ == "__main__":
    main()
