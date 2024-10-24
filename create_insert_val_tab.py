import psycopg2

# Connect to the PostgreSQL database
target_connection = psycopg2.connect(
    dbname='your_db_name',  # Database Name
    user='your_username',  # Username
    password='your_password',  # Password
    host='localhost',  # Host Name
    port='5432'  # Port
)
source_connection = psycopg2.connect(
    dbname='your_db_name',  # Database Name
    user='your_username',  # Username
    password='your_password',  # Password
    host='localhost',  # Host Name
    port='5432'  # Port
)
source_cursor = source_connection.cursor()
target_cursor = target_connection.cursor()


def fetch_column_data(schema_name, table_name):
    try:
        # Execute the query to fetch column names and data types from the specified schema and table
        source_cursor.execute(f'''
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s;
        ''', (schema_name, table_name))

        # Fetch all rows from the executed query
        rows = source_cursor.fetchall()
        column_names = [row[0] for row in rows]
        data_types = [row[1] for row in rows]
        print("column data fetch successful")
        return column_names, data_types
    except Exception as e:
        print(f"Error occurred while fetching column data: {e}")
        return [], []

def insert_values(source_schema, source_table, target_table, column_names):
    try:
        # Fetch data from the source table
        source_cursor.execute(f'SELECT * FROM {source_schema}.{source_table};')
        rows = source_cursor.fetchall()
        # SQL statement for inserting into the target table
        placeholders = ', '.join(['%s'] * len(column_names))
        insert_query = f'''
            INSERT INTO {target_table} ({', '.join(column_names)})
            VALUES ({placeholders});
        '''
        # Insert each row into the target table
        for row in rows:
            target_cursor.execute(insert_query, row)
        target_connection.commit()
        print("Data transfer successful.")
    except Exception as e:
        print(f"Error occurred during data insertion: {e}")


def create_and_populate_table(schema_name, table_name):
    try:
        target_table_name = table_name + "_stream"
        # Check if the target table exists
        target_cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema_name, target_table_name))
        table_exists = target_cursor.fetchone()[0]
        if not table_exists:
            # Fetch column names and types
            column_names, data_types = fetch_column_data(schema_name, table_name)
            if column_names and data_types:
                # Create the target table if it doesn't exist
                columns_definition = ', '.join([f'{col} {typ}' for col, typ in zip(column_names, data_types)])
                target_cursor.execute(f'''
                    CREATE TABLE {schema_name}.{target_table_name} (
                        {columns_definition}
                    );
                ''')
                target_connection.commit()
                print("Table created successfully.")
                # Insert values into the new table
                insert_values(schema_name, table_name, f"{schema_name}.{target_table_name}", column_names)
                return schema_name, target_table_name
        else:
            print("Table already exists.")
            return schema_name, target_table_name
    except Exception as e:
        print(f"Error occurred during table creation: {e}")
    # finally:
    #     # Close the cursor and connection
    #     if source_cursor:
    #         source_cursor.close()
    #     if source_connection:
    #         source_connection.close()
    #     if target_cursor:
    #         target_cursor.close()
    #     if target_connection:
    #         target_connection.close()


