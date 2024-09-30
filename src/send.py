import json
import os
from datetime import datetime

import psycopg2
from dotenv import dotenv_values

ENV = dotenv_values('./.env')

DATABASE = ENV['dbname']
USER = ENV['user']
PW = ENV['password']
HOST = ENV['host']


def body_to_text(bodies):
    all_texts = list()
    for body in bodies:
        for key_, val_ in body.items():

            if key_ == 'text':
                text = val_

            if key_ == 'links' and val_ is None:
                val_ = ''

            if key_ == 'links' and type(val_) is list:
                links = ', '.join(val_)
            else:
                links = val_

        result_text = ', '.join([text, links])
        all_texts.append(result_text)

    return ', '.join(all_texts)


def json_to_db(json_file):
    conn = connect_to_db()
    if not conn:
        print('Failed to connect to the database.')
        return False

    with open(json_file, 'r') as f:
        data = json.load(f)

    uuid_dh = data['id']
    title = data['title']
    subtitle = data['subtitle']
    url = data['url']
    created_at = datetime.strptime(data['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    updated_at = datetime.strptime(data['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
    body = body_to_text(data['data'])

    create_table(conn)
    insert_data(
        conn, uuid_dh, title, subtitle, url, created_at, updated_at, body
    )

    conn.close()
    print(f"Insert from: {data['title']} completed.")

    return True


def insert_data(
    conn, uuid_dh, title, subtitle, url, created_at, updated_at, body
):
    """
    Inserts values to table 'news'.
    """
    cur = conn.cursor()

    cur.execute(
        'INSERT INTO datahackers.news (uuid_dh, title, subtitle, url, updated_at, created_at, body) VALUES (%s, %s, %s, %s, %s, %s, %s)',
        (uuid_dh, title, subtitle, url, updated_at, created_at, body),
    )
    conn.commit()
    cur.close()


def connect_to_db():
    """
    Establishes a connection to the PostgreSQL database.
    Returns the connection object.
    """

    try:
        conn = psycopg2.connect(
            dbname=DATABASE,
            user=USER,
            password=PW,
            host=HOST,
            port='5432',
        )
        print('Connected to the database successfully.')
        return conn
    except Exception as e:
        print(f'An error occurred: {e}')
        return None


def create_table(conn):
    """
    Creates the events table if it doesn't exist.
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE if not exists datahackers.news (	
            uuid_dh UUID NOT NULL,
            title VARCHAR(250) NOT NULL,
            subtitle text NOT NULL,
            url VARCHAR(250) NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL,
            body text NOT NULL
        );

        ALTER TABLE datahackers.news ADD COLUMN if not exists ts tsvector
            GENERATED ALWAYS AS (to_tsvector('portuguese', body)) STORED;

        CREATE INDEX if not exists ts_idx ON datahackers.news USING GIN (ts);  
    """
    )
    conn.commit()
    cur.close()


if __name__ == '__main__':
    OUTPUT_DIR = 'src/outputs/'
    files = os.listdir(OUTPUT_DIR)

    for file in files:
        if file.endswith('json'):
            json_to_db(OUTPUT_DIR + file)
