'''
Explanation of the Code:
OutlierDetector Class:

The __init__ method initializes the object with a db_connection.
The create_view method creates a view named outlier_weeks in the blog_analysis schema. It calculates the weeks with vote counts more than 2 standard deviations above the mean.
The detect_outliers method ensures the view is created and has data.
main Function:

Connects to the database using the DuckDBConnection class.
Instantiates the OutlierDetector class and calls the detect_outliers method.
Closes the database connection.
This code should meet the requirements of the test cases provided, ensuring the view is created correctly and contains data.
'''

from datetime import datetime
from db import DuckDBConnection

class OutlierDetector:
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def create_view(self):
        # Create schema if not exists
        self.db_connection.execute_query('CREATE SCHEMA IF NOT EXISTS blog_analysis')

        # Define the query to create the view
        query = '''
        CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
        WITH vote_counts AS (
            SELECT 
                DATE_TRUNC('week', CreationDate) AS Week,
                COUNT(*) AS VoteCount
            FROM blog_analysis.votes
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
    # Initialize the DuckDB connection
    db_conn = DuckDBConnection()
    db_conn.connect()

    # Create the OutlierDetector instance
    detector = OutlierDetector(db_conn)

    # Detect outliers
    detector.detect_outliers()

    # Close the database connection
    db_conn.close()

if __name__ == "__main__":
    main()
