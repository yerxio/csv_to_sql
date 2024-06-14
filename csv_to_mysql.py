import os
import pandas as pd
import numpy as np
import mysql.connector

def connect_to_db(host, user, password, database):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        connection_timeout=600,
        buffered=True
    )

def create_table(cursor, table_name, columns, charset, column_data_types=None):
    if column_data_types is None:
        column_data_types = {}
    columns_sql = ', '.join(
        [f"`{col}` {column_data_types.get(col, 'VARCHAR(100)')}" for col in columns]
    )
    create_table_sql = f"CREATE TABLE `{table_name}` ({columns_sql}) CHARACTER SET {charset}"
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")  # Ensure table does not exist before creating it
    cursor.execute(create_table_sql)

def insert_data(cursor, table_name, columns, data):
    placeholders = ', '.join(['%s'] * len(columns))
    columns_sql = ', '.join([f"`{col}`" for col in columns])
    insert_sql = f"INSERT INTO `{table_name}` ({columns_sql}) VALUES ({placeholders})"
    cursor.executemany(insert_sql, data)

def read_csv_with_fallback(file_path, chunk_size=50000):
    try:
        print(f"Pandas first Reading {file_path} with UTF-8 encoding.")
        return pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8', low_memory=False), 'utf-8'
    except UnicodeDecodeError:
        print(f"Failed to read {file_path} with UTF-8 encoding. Trying GBK encoding.")
        return pd.read_csv(file_path, chunksize=chunk_size, encoding='gbk', low_memory=False), 'gbk'

def save_all(file_path, cursor, temp_table_name, column_data_types, chunk_size):
    first_chunk, encoding = read_csv_with_fallback(file_path, chunk_size)
    charset = 'utf8mb4' if encoding == 'utf-8' else 'gbk'
    columns = None
    retry_columns = column_data_types if column_data_types else {}

    for i, df in enumerate(first_chunk):
        if i == 0:
            columns = df.columns.tolist()
            create_table(cursor, temp_table_name, columns, charset, retry_columns)

        data = df.replace({np.nan: None}).values.tolist()  # Replace NaN with None

        try:
            insert_data(cursor, temp_table_name, columns, data)
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_DATA_TOO_LONG:
                print(f"{temp_table_name}: Data too long error: {e}")
                # Extract the column name that caused the error
                error_msg = str(e)
                col_name_start = error_msg.find("'") + 1
                col_name_end = error_msg.find("'", col_name_start)
                col_name = error_msg[col_name_start:col_name_end]

                print(f"Column `{col_name}` data is too long. Recreating table with `{col_name}` as larger data type.")

                # Update the column data type
                if col_name in retry_columns:
                    if retry_columns[col_name] == 'TEXT':
                        retry_columns[col_name] = 'MEDIUMTEXT'
                    else:
                        print(f"Column `{col_name}` already set to MEDIUMTEXT and still too large.")
                        raise
                else:
                    retry_columns[col_name] = 'TEXT'

                # Drop the temp table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS `{temp_table_name}`")

                # Recreate the table with updated column data types
                print('重新从头开始插入数据...')
                return (False, retry_columns)
            else:
                print(f"Failed to insert data into table `{temp_table_name}`. Error: {e}")
                raise
    return (True, retry_columns)  # Signal successful insertion

def process_csv_file(file_path, db_connection, chunk_size=50000):
    original_table_name = os.path.splitext(os.path.basename(file_path))[0]
    temp_table_name = f"{original_table_name}_temp"
    
    cursor = db_connection.cursor()
    try:
        db_connection.start_transaction()
        
        success, column_data_types = save_all(file_path, cursor, temp_table_name, None, chunk_size)
        
        while not success:
            success, column_data_types = save_all(file_path, cursor, temp_table_name, column_data_types, chunk_size)
        
        db_connection.commit()
        cursor.execute(f"RENAME TABLE `{temp_table_name}` TO `{original_table_name}`")
        print(f"Table `{original_table_name}` created and data inserted successfully.")
    except Exception as e:
        db_connection.rollback()
        print(f"Transaction rolled back due to error: {e}")
        cursor.execute(f"DROP TABLE IF EXISTS `{temp_table_name}`")
    finally:
        cursor.close()

def process_csv_files(directory, db_connection, chunk_size=50000):
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                process_csv_file(file_path, db_connection, chunk_size)
            except Exception as e:
                print(f"Error processing file `{filename}`: {e}")

if __name__ == "__main__":
    db_config = {
        'host': '',
        'user': '',
        'password': '',
        'database': ''
    } # need to be replaced with your database configuration

    csv_directory = r'' # need to be replaced with your CSV directory path

    db_connection = connect_to_db(**db_config)

    try:
        process_csv_files(csv_directory, db_connection)
    finally:
        db_connection.close()
