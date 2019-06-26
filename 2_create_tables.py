import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables listed in drop_table_queries list of sql_queries.py
    :param cur: Cursor object to execute query.
    :param conn: Connection object to commit the query.
    
    :returns: Nothing. Prints to console as each query is executed.
    """
    for query in drop_table_queries:
        cur.execute(query)
        print("Executed: {}".format(query))
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables listed in create_table_queries list of sql_queries.py
    :param cur: Cursor object to execute query.
    :param conn: Connection object to commit the query.
    
    :returns: Nothing. Prints to console as each query is executed.
    """
    for query in create_table_queries:
        cur.execute(query)
        print("Executed: {}".format(query))
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()