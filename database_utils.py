import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_database_connection():
    """
    Create a database connection using environment variables.
    
    Returns:
    sqlalchemy.engine.base.Engine: Database connection engine
    """
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')

    CONNECTION_STRING = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(CONNECTION_STRING)

def execute_query(query, params=None):
    """
    Execute a SQL query and return results as a pandas DataFrame.
    
    Args:
    query (str): SQL query to execute
    params (dict, optional): Parameters for parameterized queries
    
    Returns:
    pandas.DataFrame: Query results
    """
    engine = get_database_connection()
    
    try:
        if params:
            import numpy as np
            if isinstance(params, dict):
                params = {k: to_python_type(v) for k, v in params.items()}
            elif isinstance(params, (list, tuple)):
                params = [to_python_type(item) if isinstance(item, (np.number, np.ndarray)) else item for item in params]
        
        with engine.connect() as connection:
            if params:
                result = pd.read_sql(text(query), connection, params=params)
            else:
                result = pd.read_sql(query, connection)
            return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

import pandas as pd
from sqlalchemy import text
import logging

def execute_many_query(query, params_list):
    """
    Execute a batch of SQL queries and return results.
    
    Args:
    query (str): SQL query template to execute
    params_list (list): List of parameter dictionaries for batch execution
    
    Returns:
    int: Number of rows affected or -1 on error
    """
    engine = get_database_connection()
    
    try:
        with engine.begin() as connection:
            # Use the `text()` function to ensure the query is treated as executable
            query = text(query)
            
            # Loop over params_list and execute each set of parameters
            for params in params_list:
                connection.execute(query, params)
            
            # Return the number of rows affected (count of updates)
            return len(params_list)
    
    except Exception as e:
        logging.error(f"Unexpected error in execute_many_query: {e}")
        print(f"Unexpected error: {e}")
        return -1

def get_titles():
    query = """ 
    SELECT "movieId","title"
    FROM movies
    """
    return execute_query(query)

def get_movies():
    query = """ 
    SELECT *
    FROM movies
    """
    return execute_query(query)

def update_clean_titles(clean_titles):
    """
    Update the newly added clean_titles column with provided values.
    
    Args:
        clean_titles (list): List of clean titles to update in the table
    
    Returns:
        bool: Result of executing the UPDATE query
    """
    # Assuming clean_titles is a list of tuples with (movieId, clean_title)
    query = """
    UPDATE movies
    SET clean_title = :clean_title
    WHERE "movieId" = :movieId;
    """
    
    params_list = [{"clean_title": title, "movieId": movieId} for title, movieId in clean_titles]

    # Batch update for better performance
    return execute_many_query(query, params_list)

def get_similar_recs(movieId):
    query = """ 
    WITH similarity_calculations AS (
        SELECT "movieId", 
               CAST(count("userId") AS FLOAT) / (
                   SELECT count("userId")
                   FROM ratings
                   WHERE "movieId" = :movie_id
                   AND "rating" > 4
               ) AS similarity_score
        FROM ratings
        WHERE "rating" > 4 AND "userId" in (
            SELECT "userId"
            FROM ratings
            WHERE "movieId" = :movie_id
            AND "rating" > 4
        )
        GROUP BY "movieId"
    )
    SELECT "movieId", similarity_score
    FROM similarity_calculations
    WHERE similarity_score > 0.1
    ORDER BY similarity_score DESC
    """
    return execute_query(query, {'movie_id': movieId})

def get_ratings(movieId):
    query = """ 
    SELECT *
    FROM ratings
    """
    return execute_query(query)

def get_all_user_recs(movieId):
    query = """
        WITH frequent_users AS (
            SELECT "userId"
            FROM ratings
            WHERE "movieId" = :movie_id
            AND "rating" > 4
        ),
        movie_counts AS (
            SELECT r."movieId",
                   COUNT(r."userId") AS user_count,
                   CAST(COUNT(r."userId") AS FLOAT) / (
                       SELECT COUNT(*)
                       FROM frequent_users
                   ) AS similarity_score
            FROM ratings r
            INNER JOIN frequent_users fu ON r."userId" = fu."userId"
            WHERE r."rating" > 4
            GROUP BY r."movieId"
            HAVING COUNT(r."userId") > 0
        ),
        similar_movies AS (
            SELECT "movieId"
            FROM movie_counts
            WHERE similarity_score > 0.1
        )
        SELECT r.*
        FROM ratings r
        INNER JOIN similar_movies sm ON r."movieId" = sm."movieId"
        WHERE r."rating" > 4;
    """
    return execute_query(query, {'movie_id': movieId})

def to_python_type(value):
    """Convert NumPy types to Python native types."""
    import numpy as np
    if isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    return value

def create_indexes():
    """Create recommended indexes for the movie recommendation database."""
    index_queries = [
        'CREATE INDEX IF NOT EXISTS idx_ratings_movieid ON ratings ("movieId");',
        'CREATE INDEX IF NOT EXISTS idx_ratings_userid ON ratings ("userId");',
        'CREATE INDEX IF NOT EXISTS idx_ratings_rating ON ratings ("rating");',
        'CREATE INDEX IF NOT EXISTS idx_ratings_movieid_rating ON ratings ("movieId", "rating");',
        'CREATE INDEX IF NOT EXISTS idx_ratings_userid_rating ON ratings ("userId", "rating");',
        'CREATE INDEX IF NOT EXISTS idx_movies_movieid ON movies ("movieId");'
    ]
    
    engine = get_database_connection()
    
    try:
        with engine.begin() as connection:
            for query in index_queries:
                connection.execute(text(query))
                print("Database index created successfully: ", query)
        return True
    except Exception as e:
        print(f"Error creating indexes: {e}")
        return False