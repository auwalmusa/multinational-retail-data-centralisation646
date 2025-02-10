# schema_update.py
"""
Example script to alter the columns in orders_table and dim_users
so they match the required star schema.
Using Approach 1 (increasing VARCHAR length to avoid truncation errors).
"""

import psycopg2
import yaml

def update_schema():
    """
    Connects to local Postgres, then executes ALTER TABLE statements
    to cast columns in orders_table and dim_users to the desired data types.
    """
    # 1) Load local DB credentials (or hard-code them if you prefer)
    with open("local_creds.yaml", "r", encoding="utf-8") as f:
        creds = yaml.safe_load(f)

    host = creds["RDS_HOST"]
    user = creds["RDS_USER"]
    password = creds["RDS_PASSWORD"]
    database = creds["RDS_DATABASE"]
    port = creds["RDS_PORT"]

    # 2) Connect to local Postgres
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=database,
        port=port
    )
    conn.autocommit = False  # We'll commit manually

    try:
        with conn.cursor() as cur:
            # ------------------------------
            # TASK 1: orders_table
            #    Using Approach 1:
            #    - card_number -> VARCHAR(50)
            #    - store_code  -> VARCHAR(50)
            #    - product_code -> VARCHAR(50)
            #    Feel free to change 50 to 100 or 255 if needed.
            # ------------------------------
            alter_orders_sql = """
            ALTER TABLE orders_table
            -- cast date_uuid from TEXT to UUID
            ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
            -- cast user_uuid from TEXT to UUID
            ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
            -- card_number -> varchar(50)
            ALTER COLUMN card_number TYPE varchar(50),
            -- store_code -> varchar(50)
            ALTER COLUMN store_code TYPE varchar(50),
            -- product_code -> varchar(50)
            ALTER COLUMN product_code TYPE varchar(50),
            -- product_quantity: bigint -> smallint
            ALTER COLUMN product_quantity TYPE smallint;
            """

            cur.execute(alter_orders_sql)
            print("[INFO] orders_table columns updated successfully.")

            # ------------------------------
            # TASK 2: dim_users
            #    Changing columns to match the star schema:
            #    - first_name -> VARCHAR(255)
            #    - last_name  -> VARCHAR(255)
            #    - date_of_birth -> DATE
            #    - country_code  -> VARCHAR(10)
            #    - user_uuid -> UUID
            #    - join_date -> DATE
            # ------------------------------
            alter_users_sql = """
            ALTER TABLE dim_users
            ALTER COLUMN first_name TYPE varchar(255),
            ALTER COLUMN last_name TYPE varchar(255),
            ALTER COLUMN date_of_birth TYPE date USING date_of_birth::date,
            ALTER COLUMN country_code TYPE varchar(10),
            ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
            ALTER COLUMN join_date TYPE date USING join_date::date;
            """

            cur.execute(alter_users_sql)
            print("[INFO] dim_users columns updated successfully.")

        # If all went well, commit the transaction
        conn.commit()

    except Exception as e:
        conn.rollback()
        print("[ERROR] Something went wrong. Rolling back changes!")
        print(e)
    finally:
        conn.close()

if __name__ == "__main__":
    update_schema()
    print("[INFO] Schema update script completed.")
