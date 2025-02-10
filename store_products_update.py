# store_products_update.py

import psycopg2
import yaml

def update_store_products():
    with open("local_creds.yaml", "r", encoding="utf-8") as f:
        creds = yaml.safe_load(f)

    host = creds["RDS_HOST"]
    user = creds["RDS_USER"]
    password = creds["RDS_PASSWORD"]
    dbname = creds["RDS_DATABASE"]
    port = creds["RDS_PORT"]

    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=dbname,
        port=port
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:

            # ------------------------------------------------------
            # PART 1: dim_store_details
            # ------------------------------------------------------
            # a) Convert 'N/A' to NULL in locality column
            na_to_null_sql = """
            UPDATE dim_store_details
            SET locality = NULL
            WHERE locality = 'N/A';
            """
            cur.execute(na_to_null_sql)
            print("[INFO] Changed 'N/A' in locality to NULL.")

            # b) Cast columns to required data types
            alter_store_sql = """
            ALTER TABLE dim_store_details
            ALTER COLUMN longitude TYPE numeric USING longitude::numeric,
            ALTER COLUMN locality TYPE varchar(255),
            ALTER COLUMN store_code TYPE varchar(50),
            ALTER COLUMN staff_numbers TYPE smallint USING staff_numbers::smallint,
            ALTER COLUMN opening_date TYPE date USING opening_date::date,
            ALTER COLUMN store_type TYPE varchar(255),
            ALTER COLUMN latitude TYPE numeric USING latitude::numeric,
            ALTER COLUMN country_code TYPE varchar(10),
            ALTER COLUMN continent TYPE varchar(255);
            """
            cur.execute(alter_store_sql)
            print("[INFO] dim_store_details schema updated successfully.")

            # ------------------------------------------------------
            # PART 2: dim_products
            # ------------------------------------------------------
            # a) Remove '£' from product_price
            remove_symbol_sql = """
            UPDATE dim_products
            SET product_price = REPLACE(product_price, '£', '');
            """
            cur.execute(remove_symbol_sql)
            print("[INFO] Removed '£' from product_price in dim_products.")

            # b) Cast product_price to numeric
            alter_price_sql = """
            ALTER TABLE dim_products
            ALTER COLUMN product_price TYPE numeric USING product_price::numeric;
            """
            cur.execute(alter_price_sql)
            print("[INFO] Cast product_price to numeric.")

            # c) Add weight_class column if not exists
            add_weight_class_sql = """
            ALTER TABLE dim_products
            ADD COLUMN IF NOT EXISTS weight_class varchar(50);
            """
            cur.execute(add_weight_class_sql)
            print("[INFO] Added weight_class column to dim_products.")

            # d) Populate weight_class
            update_weight_class_sql = """
            UPDATE dim_products
            SET weight_class = CASE
                WHEN weight_kg < 2 THEN 'Light'
                WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
                WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
                ELSE 'Truck_Required'
            END;
            """
            cur.execute(update_weight_class_sql)
            print("[INFO] Populated weight_class in dim_products.")

        conn.commit()
        print("[SUCCESS] All store + product updates completed!")

    except Exception as e:
        conn.rollback()
        print("[ERROR] Rolling back due to:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    update_store_products()
