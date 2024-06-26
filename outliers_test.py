'''
Explanation of Additional Test Cases:
Test for Table Not Existing:

Purpose: Ensure that the script handles scenarios where the specified table does not exist.
Check: Verify that the script raises an appropriate error.
Test for Empty Table:

Purpose: Ensure that the script can handle an empty table.
Check: Verify that the view outlier_weeks is created but contains no rows.
Test for Consistent Data:

Purpose: Verify that the script correctly identifies there are no outliers when the data is consistent.
Check: Ensure that the view outlier_weeks is empty.
Test for Missing Weeks:

Purpose: Test how the script handles weeks with no votes.
Check: Ensure that the weeks with missing data are correctly identified as outliers.
Test for Threshold Logic:

Purpose: Verify that the outlier detection threshold (20% deviation) works as expected.
Check: Ensure that weeks with vote counts outside the 20% threshold are correctly identified as outliers.
'''

"""
Additional test cases for the outliers.py script
"""
import subprocess
import os

import duckdb
import pytest


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")


def run_outliers_calculation():
    result = subprocess.run(
        args=["python", "-m", "coffeebeans_dataeng_exercise.outliers"],
        capture_output=True,
    )
    result.check_returncode()


def test_table_not_existing():
    # Drop the votes table to simulate a non-existing table
    con = duckdb.connect("warehouse.db")
    con.execute("DROP TABLE blog_analysis.votes")
    con.close()

    with pytest.raises(subprocess.CalledProcessError):
        run_outliers_calculation()


def test_empty_table():
    # Ensure the votes table is empty
    run_outliers_calculation()
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        count_in_view = result.fetchall()[0][0]
        assert count_in_view == 0, "Expected view 'outlier_weeks' to be empty for empty votes table"
    finally:
        con.close()


def test_consistent_data():
    con = duckdb.connect("warehouse.db")
    con.execute("""
    INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate) VALUES
    (1, 1, 1, '2022-01-01T00:00:00.000'),
    (2, 2, 1, '2022-01-08T00:00:00.000'),
    (3, 3, 1, '2022-01-15T00:00:00.000')
    """)
    con.close()

    run_outliers_calculation()
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        count_in_view = result.fetchall()[0][0]
        assert count_in_view == 0, "Expected view 'outlier_weeks' to be empty for consistent data"
    finally:
        con.close()


def test_missing_weeks():
    con = duckdb.connect("warehouse.db")
    con.execute("""
    INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate) VALUES
    (1, 1, 1, '2022-01-01T00:00:00.000'),
    (2, 2, 1, '2022-01-22T00:00:00.000'),
    (3, 3, 1, '2022-02-12T00:00:00.000')
    """)
    con.close()

    run_outliers_calculation()
    sql = "SELECT Year, WeekNumber FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        outlier_weeks = result.fetchall()
        expected_outliers = [(2022, 3), (2022, 5)]
        for outlier in expected_outliers:
            assert outlier in outlier_weeks, f"Expected week {outlier} to be an outlier due to missing data"
    finally:
        con.close()


def test_threshold_logic():
    con = duckdb.connect("warehouse.db")
    con.execute("""
    INSERT INTO blog_analysis.votes (Id, PostId, VoteTypeId, CreationDate) VALUES
    (1, 1, 1, '2022-01-01T00:00:00.000'),
    (2, 1, 1, '2022-01-01T00:00:00.000'),
    (3, 1, 1, '2022-01-01T00:00:00.000'),
    (4, 2, 1, '2022-01-08T00:00:00.000'),
    (5, 2, 1, '2022-01-08T00:00:00.000'),
    (6, 2, 1, '2022-01-08T00:00:00.000'),
    (7, 3, 1, '2022-01-15T00:00:00.000')
    """)
    con.close()

    run_outliers_calculation()
    sql = "SELECT Year, WeekNumber FROM blog_analysis.outlier_weeks"
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        outlier_weeks = result.fetchall()
        expected_outliers = [(2022, 2), (2022, 3)]
        for outlier in expected_outliers:
            assert outlier in outlier_weeks, f"Expected week {outlier} to be an outlier based on threshold logic"
    finally:
        con.close()
