# File: pg_dbconnect.py
import psycopg2

hostname="localhost"
port=5432
username="admin"
password="admin"
database="mydb"

def create_connection():
    """
    Create a database connection to the PostgreSQL database.
    
    Returns:
        connection: psycopg2 connection object
    """
    try:
        connection = psycopg2.connect(
            host=hostname,
            port=port,
            user=username,
            password=password,
            dbname=database
        )
        print("Connection to the database established successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def close_connection(connection):
    """
    Close the database connection.
    
    Args:
        connection: psycopg2 connection object
    """
    if connection:
        connection.close()
        print("Database connection closed.")    

if __name__ == "__main__":
    conn = create_connection()
    if conn:
        # Perform database operations here
        close_connection(conn)
    else:
        print("Failed to create a database connection.")    