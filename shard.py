import threading
import psycopg
from psycopg import sql

# Connection details for the primary database
PRIMARY_DB = {
    "dbname": "scorrea34",
    "user": "scorrea34",
    "password": "scorrea34",
    "host": "localhost",
    "port": 5432
}

def create_database(conn, tld):
    """Create a new database for the given TLD if it doesn't exist."""
    conn.rollback()

    conn.autocommit = True
    with conn.cursor() as cur:
        try:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(tld)))
            print(f"Database '{tld}' created.")
        except psycopg.errors.DuplicateDatabase:
            pass
        except Exception as e:
            print(f"Database '{tld}' creation error: {e}")
    conn.autocommit = False

def create_table(conn, table_name):
    """Create a table for the given domain if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                timestamp BIGINT,
                url TEXT,
                cert_index BIGINT,
                all_domains TEXT,
                cn TEXT,
                not_before DOUBLE PRECISION,
                not_after DOUBLE PRECISION
            );
        """).format(sql.Identifier(table_name)))
        # print(f"Table '{table_name}' checked/created.")

def insert_row(conn, table_name, row):
    """Insert a row into the specified table."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            INSERT INTO {} (timestamp, url, cert_index, all_domains, cn, not_before, not_after)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """).format(sql.Identifier(table_name)), (
            row['timestamp'], row['url'], row['cert_index'],
            row['all_domains'], row['cn'], row['not_before'], row['not_after']
        ))
        # print(f"Row inserted into table '{table_name}'.")


def __process_batch(batch_number, batch_size, primary_conn):
    """Process rows from ct_logs and create databases, tables, and insert rows."""
    offset = batch_number * batch_size
    offset = max(2422401, offset)
    query = f"""
                SELECT *
                FROM ct_logs
                LIMIT {batch_size}
                OFFSET {offset};
            """

    with psycopg.connect(**PRIMARY_DB).cursor() as curr:
        curr.execute(query)
        rows = curr.fetchall()
        colnames = [desc[0] for desc in curr.description]
        
    for row in rows:
        row_data = dict(zip(colnames, row))
        cn = row_data['cn']
        
        # Extract TLD and table name
        parts = cn.split(".")
        if len(parts) < 2:
            print(f"Skipping invalid CN: {cn}")
            continue
        
        tld = parts[-1]
        table_name = ".".join(parts[-2:])
        
        # Connect to the PostgreSQL server to create the database if needed
        create_database(primary_conn, tld)
        
        # Connect to the TLD-specific database
        with psycopg.connect(dbname=tld,
                             user=PRIMARY_DB["user"],
                             password=PRIMARY_DB["password"],
                             host=PRIMARY_DB["host"],
                             port=PRIMARY_DB["port"]) as tld_conn:
            create_table(tld_conn, table_name)
            insert_row(tld_conn, table_name, row_data)

        if rows.index(row) % 5000 == 0:
            print(f"Inserted {row_data} into {table_name}")

def process_batch(batch_number, batch_size, primary_conn):
    """Process rows from ct_logs and create databases, tables, and insert rows."""
    offset = batch_number * batch_size
    query = f"""
                SELECT *
                FROM ct_logs
                LIMIT {batch_size}
                OFFSET {offset};
            """
    with psycopg.connect(**PRIMARY_DB).cursor() as curr:
        curr.execute(query)
        rows = curr.fetchall()
        colnames = [desc[0] for desc in curr.description]
        
    for row in rows:
        row_data = dict(zip(colnames, row))
        cn = row_data['cn']
        
        # Extract TLD and table name
        parts = cn.split(".")
        if len(parts) < 2:
            print(f"Skipping invalid CN: {cn}")
            continue
        
        tld = parts[-1]
        table_name = ".".join(parts[-2:])
        
        # Connect to the PostgreSQL server to create the database if needed
        create_database(primary_conn, tld)
        
        # Connect to the TLD-specific database
        with psycopg.connect(dbname=tld,
                             user=PRIMARY_DB["user"],
                             password=PRIMARY_DB["password"],
                             host=PRIMARY_DB["host"],
                             port=PRIMARY_DB["port"]) as tld_conn:
            create_table(tld_conn, table_name)
            insert_row(tld_conn, table_name, row_data)

        if rows.index(row) % 1000 == 0:
            print(f"Inserted {row_data} into {table_name}")


# Main script to create threads
def __main():
    total_threads = 24
    batch_size = 500_000
    threads = []

    conn = psycopg.connect(**PRIMARY_DB)

    for i in range(total_threads):
        thread = threading.Thread(target=__process_batch, args=(i, batch_size, conn))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    
    conn.close()

    print("Processing complete.")

def main():
    try:
        with psycopg.connect(**PRIMARY_DB) as conn:
            process_batch(1, 20_000, conn)
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    __main()
