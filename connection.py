import psycopg2

# Replace these values with your actual PostgreSQL credentials
HOST = "localhost"
PORT = 5432
DATABASE = "postgres"
USER = "postgres"
PASSWORD = "Admin"

try:
    conn = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print("Connection failed!")
    print(e)
