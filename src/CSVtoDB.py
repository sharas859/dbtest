import sqlite3 as sq
import pickle
import csv
from compress_pickle import dumps, loads


def read_csv(filename):
    with open(filename, newline='') as csvfile:
        data = list(csv.reader(csvfile, delimiter='\t'))
        metadata = data[:29]
        # skip metadta rows and non-value columns
        measurements = data[34:][3:]
        x_values = data[33][3:]
        # get only first 1000 columns
    #    measurements = [row[:1000] for row in measurements]
    return metadata, measurements, x_values


def metadata_to_db(db, metadata):
    conn = sq.connect(db)
    cursor = conn.cursor()
    # FOR TESTING ONLY
    # cursor.execute('''DROP TABLE IF EXISTS metadata''')
    # ----------------
    coulmn_names = [row[0] for row in metadata]
    metadata_values = [row[1] for row in metadata]
    metadata_table_string = f'''CREATE TABLE IF NOT EXISTS metadata
                    ({', '.join([f'"{col}" TEXT' for col in coulmn_names])})'''
    cursor.execute(metadata_table_string)
    metadata_insert_string = f'''INSERT INTO metadata ({', '.join([f'"{col}"' for col in coulmn_names])})
                    VALUES ({', '.join([f'"{val}"' for val in metadata_values])})'''
    cursor.execute(metadata_insert_string)
    conn.commit()
    conn.close()


def csv_to_db(db, csv,  chunk_size=200):
    metadata, measurements, x_values = read_csv(csv)
    metadata_to_db(db, metadata)
    name = metadata[0][1]
    write_measurements_to_db(db, measurements, name, x_values, chunk_size)


def write_measurements_to_db(db, measurements, name, x_values, chunk_size):

    # one index for each chunk [0, 1000, 2000, ...]
    chunks = list(range(0, len(measurements[0]), chunk_size))
    # create a list of "chunk{index} BLOB" for the sql query
    chunk_names = [f'chunk{i}' for i in range(len(chunks))]

    conn = sq.connect(db)
    cursor = conn.cursor()

    table_is_empty = cursor.execute(
        f'''SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{name}' ''').fetchone()[0] == 0

    if not table_is_empty:
        print(f'Table {name} already exists\n')
        return

    # drop existing table for testing
    print(name)
    conn.execute(f'''DROP TABLE IF EXISTS "{name}"''')
    table_string = f'''CREATE TABLE IF NOT EXISTS "{name}"
                    (timestamp DATE, {', '.join([f'{chunk} BLOB' for chunk in chunk_names])})'''

    conn.execute(table_string)

    # drop existing table for testing
    conn.execute('''DROP TABLE IF EXISTS "chunk_index {name}"''')
    # create table with chunk index: chunk_name Text Priamry Key, start_x float, end_x flaot, chunk size int
    chunk_table_string = f'''CREATE TABLE IF NOT EXISTS  "chunk_index {name}"
                    (chunk_name TEXT PRIMARY KEY, start_x FLOAT, end_x FLOAT, chunk_size INT)'''
    conn.execute(chunk_table_string)

    # insert chunk index into chunk_index table, consider last chunk might be smaller than CHUNK_SIZE
    for i in range(len(chunks)):
        start_x = x_values[i*chunk_size]
        end_x = x_values[(i+1)*chunk_size-1] if i != len(chunks) - \
            1 else x_values[-1]
        chunk_size = chunk_size if i != len(chunks) - 1 else len(
            x_values) - i*chunk_size
        conn.execute(f'''INSERT INTO  "chunk_index {name}" (chunk_name, start_x, end_x, chunk_size)
                        VALUES ('{chunk_names[i]}', {start_x}, {end_x}, {chunk_size})''')

    for row in measurements:
        timestamp = row[0]
        pickled_chunks = [sq.Binary(dumps(row[i:i+chunk_size], compression='gzip',
                                          set_default_extension=False)) for i in chunks]
        insert_string = f'INSERT INTO "{name}" (timestamp, {", ".join(chunk_names)}) VALUES (?, {", ".join(["?"]*len(chunks))})'
        cursor.execute(insert_string, [timestamp, *pickled_chunks])

    conn.commit()
    conn.close()


def query_timestamps(start_timestamp, end_timestamp):
    conn = sq.connect('nopandas.db')
    cursor = conn.cursor()
    cursor.execute(
        f'''SELECT * FROM measurements  WHERE timestamp BETWEEN '{start_timestamp}' AND '{end_timestamp}' ''')
    data = cursor.fetchall()
    conn.close()
    return data


def query_chunks(db, name, start_x=None, end_x=None, start_t=None, end_t=None):
    conn = sq.connect(db)
    cursor = conn.cursor()

    # check if table exists
    table_exists = cursor.execute(
        f'''SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{name}' ''').fetchone()[0] == 1
    if not table_exists:
        print(f'Table {name} does not exist\n')
        return

    # get chunk names from chunk index
    if start_x is None or end_x is None:
        cursor.execute(
            f'''SELECT chunk_name FROM "chunk_index {name}"''')
    else:
        cursor.execute(
            f'''SELECT chunk_name FROM "chunk_index {name}" WHERE end_x > {start_x} AND start_x < {end_x}''')
    chunk_names = cursor.fetchall()

    # get chunks from measurement table
    query = f'''SELECT {", ".join([chunk[0] for chunk in chunk_names])}
            FROM "{name}"
            '''
    if start_t is not None and end_t is not None:
        query += f"WHERE timestamp BETWEEN '{start_t}' AND '{end_t}'"

    cursor.execute(query)
    chunks = cursor.fetchall()

    conn.close()
    return chunks


def unpickle_chunks(chunks):
    unpickled_chunks = [loads(
        chunk, compression='gzip', set_default_extension=False) for chunk in chunks]
    return unpickled_chunks


def unpickle_data(data):
    try:
        unpickled = [unpickle_chunks(row) for row in data]
        unpickled = [[item for chunk in row for item in chunk]
                     for row in unpickled]
        return unpickled
    except:
        print("no data to unpickle")


def get_data(db, name, start_x=None, end_x=None, start_t=None, end_t=None):
    chunks = query_chunks(db, name, start_x, end_x, start_t, end_t)
    data = unpickle_data(chunks)
    # find a way to trim x values outside boundaries from start and end
    return data


def print_dimensions(name, data):
    try:
        print(f'Name: {name}')
        print(f'Number of rows: {len(data)}')
        print(f'Number of columns: {len(data[0])}')
        print('-------------------')
    except:
        print("no data to print")
