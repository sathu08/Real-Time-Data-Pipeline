from kafka import KafkaConsumer
import json
import psycopg2
from create_insert_val_tab import create_and_populate_table
from datetime import datetime, timedelta

# PostgreSQL connection setup
def connect_db():
    return psycopg2.connect(
        dbname='your_db_name',  # Database Name
        user='your_username',  # Username
        password='your_password',  # Password
        host='localhost',  # Host Name
        port='5432'  # Port
        )

# Kafka Consumer configuration
TOPIC_NAME = 'dbserver1.public.sales_data'  # Change this to your actual topic name if needed
BOOTSTRAP_SERVERS = ['localhost:9092']
AUTO_OFFSET_RESET = 'earliest'
ENABLE_AUTO_COMMIT = True

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset=AUTO_OFFSET_RESET,
    group_id='sales-group'
    # value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def postgres_date_to_standard(postgres_date):
    # UNIX epoch starts from 1970-01-01
    epoch_start = datetime(1970, 1, 1)
    return (epoch_start + timedelta(days=postgres_date)).strftime('%Y-%m-%d')

def update_table(target_table_name, column_names, values):
   try:
      target_connection = connect_db()
      with target_connection.cursor() as target_cursor:
         set_clause = ', '.join([f"{col} = %s" for col in column_names[1:]])
         update_query = f'''
           UPDATE {target_table_name}
           SET {set_clause}
           WHERE {column_names[0]} = %s;
           '''
         update_values = values[1:] + [values[0]]
         target_cursor.execute(update_query, update_values)
         target_connection.commit()
         print(f"Updated data in {target_table_name}: {update_values}")
   except Exception as e:
      print(f"Error occurred while updating values: {e}")


def insert_table_values(target_table_name, column_names, values):
    try:
        target_connection = connect_db()
        with target_connection.cursor() as target_cursor:
            insert_query = f'''
            INSERT INTO {target_table_name} ({', '.join(column_names)})
            VALUES ({', '.join(['%s'] * len(column_names))});
            '''
            target_cursor.execute(insert_query, values)
            target_connection.commit()
            print(f"Inserted data into {target_table_name}: {values}")
    except Exception as e:
        print(f"Error occurred while inserting values: {e}")

def delete_table(target_table_name, column_names, values):
    try:
        target_connection = connect_db()
        with target_connection.cursor() as target_cursor:
            delete_query = f'''
            DELETE FROM {target_table_name} WHERE {column_names[0]} = {values[0]};
            '''
            target_cursor.execute(delete_query, values)
            target_connection.commit()
            print(f"Deleted Data from {target_table_name}: {values}")
    except Exception as e:
        print(f"Error occurred while inserting values: {e}")

def table_create(value):
    try:
        source_schema_name = value['payload']["source"]["schema"]
        source_table_name = value['payload']["source"]["table"]
        schema_name, table_name = create_and_populate_table(source_schema_name, source_table_name)
        return schema_name, table_name
    except Exception as e:
        print(f"Error in table creation: {e}")
        return None, None


def start_pipeline():
    for msg in consumer:
        if msg.value:
            try:
                message = json.loads(msg.value.decode('utf-8'))
                # Create a Table if not exist
                schema_name, table_name = table_create(message)
                if not schema_name or not table_name:
                    continue
                target_table_name = f"{schema_name}.{table_name}"
                operation = message['payload'].get('op')
                if operation == 'c':
                    after_data = message['payload'].get('after', {})
                    after_data['date'] = postgres_date_to_standard(after_data['date'])
                    insert_table_values(target_table_name, list(after_data.keys()), list(after_data.values()))
                elif operation == 'd':
                    before_data = message['payload'].get('before', {})
                    before_data['date'] = postgres_date_to_standard(before_data['date'])
                    column_value = {}
                    for key, value in before_data.items():
                        if isinstance(value,(int, float)) and value > 0:  # Check if value is a number and greater than 0
                            column_value[key] = value
                    delete_table(target_table_name, list(column_value.keys()), list(column_value.values()))
                elif operation == 'u':
                    after_data = message['payload'].get('after', {})
                    after_data['date'] = postgres_date_to_standard(after_data['date'])
                    update_table(f"{schema_name}.{table_name}", list(after_data.keys()), list(after_data.values()))

            except Exception as e:
                print(f"Error occurred: {e}")
if __name__ == "__main__":
    start_pipeline()
