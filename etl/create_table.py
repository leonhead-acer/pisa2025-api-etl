#!/usr/bin/python

import psycopg2

def create_tables(params):
    """ create tables in the PostgreSQL database"""
    commands = (
        '''
        CREATE TABLE delivery_results (
            delivery_execution_id VARCHAR(100) PRIMARY KEY,
            delivery_id character(12) NOT NULL,
            is_deleted BOOL NOT NULL,
            last_update_date BIGINT NOT NULL,
            login VARCHAR(50) NOT NULL,
            test_qti_id VARCHAR(255) NOT NULL,
            test_qti_label VARCHAR(255) NOT NULL,
            test_qti_title VARCHAR(255) NOT NULL,
            raw_data TEXT NOT NULL
        );
        ''',
        # """ CREATE TABLE parts (
        #         part_id SERIAL PRIMARY KEY,
        #         part_name VARCHAR(255) NOT NULL
        #         )
        # """,
        # """
        # CREATE TABLE part_drawings (
        #         part_id INTEGER PRIMARY KEY,
        #         file_extension VARCHAR(5) NOT NULL,
        #         drawing_data BYTEA NOT NULL,
        #         FOREIGN KEY (part_id)
        #         REFERENCES parts (part_id)
        #         ON UPDATE CASCADE ON DELETE CASCADE
        # )
        # """,
        # """
        # CREATE TABLE vendor_parts (
        #         vendor_id INTEGER NOT NULL,
        #         part_id INTEGER NOT NULL,
        #         PRIMARY KEY (vendor_id , part_id),
        #         FOREIGN KEY (vendor_id)
        #             REFERENCES vendors (vendor_id)
        #             ON UPDATE CASCADE ON DELETE CASCADE,
        #         FOREIGN KEY (part_id)
        #             REFERENCES parts (part_id)
        #             ON UPDATE CASCADE ON DELETE CASCADE
        # )
        # """
      )
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_tables(params):
    """ create tables in the PostgreSQL database"""
    commands = (
        '''
        DROP TABLE IF EXISTS "delivery_results";
        ''',
        # """ CREATE TABLE parts (
        #         part_id SERIAL PRIMARY KEY,
        #         part_name VARCHAR(255) NOT NULL
        #         )
        # """,
        # """
        # CREATE TABLE part_drawings (
        #         part_id INTEGER PRIMARY KEY,
        #         file_extension VARCHAR(5) NOT NULL,
        #         drawing_data BYTEA NOT NULL,
        #         FOREIGN KEY (part_id)
        #         REFERENCES parts (part_id)
        #         ON UPDATE CASCADE ON DELETE CASCADE
        # )
        # """,
        # """
        # CREATE TABLE vendor_parts (
        #         vendor_id INTEGER NOT NULL,
        #         part_id INTEGER NOT NULL,
        #         PRIMARY KEY (vendor_id , part_id),
        #         FOREIGN KEY (vendor_id)
        #             REFERENCES vendors (vendor_id)
        #             ON UPDATE CASCADE ON DELETE CASCADE,
        #         FOREIGN KEY (part_id)
        #             REFERENCES parts (part_id)
        #             ON UPDATE CASCADE ON DELETE CASCADE
        # )
        # """
      )
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# if __name__ == '__main__':
#     create_tables()