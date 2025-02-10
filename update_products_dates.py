# update_products_dates.py
"""
Script to update dim_products and dim_date_times per the instructions:
Task 5: 
  - rename removed -> still_available and cast to boolean
  - cast product_price (TEXT -> NUMERIC)
  - cast weight (TEXT -> NUMERIC)
  - cast EAN, product_code, weight_class (TEXT -> VARCHAR(...))
  - cast date_added (TEXT -> DATE)
  - cast uuid (TEXT -> UUID)

Task 6:
  - cast dim_date_times columns:
    month, year, day, time_period (TEXT -> VARCHAR(...))
    date_uuid (TEXT -> UUID)
"""

import psycopg2
import yaml

def update_products_dates():
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
            # -------------------------------------------------
            # PART A: dim_products
            # -------------------------------------------------

            # 1) Rename `removed` column to `still_available`
            #    only if 'removed' exists. 
            rename_sql = """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='dim_products' 
                      AND column_name='ean'
                ) THEN
                    ALTER TABLE dim_products
                    RENAME COLUMN removed TO still_available;
                END IF;
            END $$;
            """
            cur.execute(rename_sql)
            print("[INFO] Renamed 'removed' to 'still_available' if it existed.")

            # 2) Convert text values in still_available to valid boolean strings
            #    For example, if you previously stored "Removed" => 'false', 
            #    "Still_avaliable" => 'true'. Adjust if needed.
            fix_bool_sql = """
            UPDATE dim_products
            SET still_available = CASE
                WHEN lower(still_available) = 'removed' 
                     OR lower(still_available) = 'false'
                THEN 'false'
                ELSE 'true'
            END
            WHERE still_available IS NOT NULL;
            """
            cur.execute(fix_bool_sql)
            print("[INFO] Mapped still_available values to 'true'/'false'.")

            # 3) Cast columns to required data types
            #    - product_price => numeric
            #    - weight => numeric
            #    - EAN => varchar(50)
            #    - product_code => varchar(50)
            #    - date_added => date
            #    - uuid => uuid
            #    - still_available => bool
            #    - weight_class => varchar(50)
            alter_products_sql = """
            ALTER TABLE dim_products
            ALTER COLUMN product_price TYPE numeric USING product_price::numeric,
            ALTER COLUMN weight TYPE numeric USING weight::numeric,
            ALTER COLUMN EAN TYPE varchar(50),
            ALTER COLUMN product_code TYPE varchar(50),
            ALTER COLUMN date_added TYPE date USING date_added::date,
            ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
            ALTER COLUMN still_available TYPE boolean USING (CASE
                WHEN lower(still_available) in ('true','t') THEN true
                WHEN lower(still_available) in ('false','f') THEN false
                ELSE true
            END),
            ALTER COLUMN weight_class TYPE varchar(50);
            """
            cur.execute(alter_products_sql)
            print("[INFO] dim_products columns cast successfully.")

            # -------------------------------------------------
            # PART B: dim_date_times
            # -------------------------------------------------
            # month, year, day, time_period => varchar(20/50)
            # date_uuid => uuid
            alter_dates_sql = """
            ALTER TABLE dim_date_times
            ALTER COLUMN month TYPE varchar(10),
            ALTER COLUMN year TYPE varchar(10),
            ALTER COLUMN day TYPE varchar(10),
            ALTER COLUMN time_period TYPE varchar(50),
            ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
            """
            cur.execute(alter_dates_sql)
            print("[INFO] dim_date_times schema updated successfully.")

        conn.commit()
        print("[SUCCESS] All product & date times table updates completed!")

    except Exception as e:
        conn.rollback()
        print("[ERROR] Rolling back due to:", e)

    finally:
        conn.close()


if __name__ == "__main__":
    update_products_dates()
