from typing import Optional, Any, Iterator, Dict

import psycopg2
import getopt
import sys
import urllib.parse
import csv
import StringIteratorIO

conn = None
pg_connect_string = None
input_file = None
user = None
path = None


class Importer(object):
    def main(self, argv):
        t_input_file = ''
        t_pg_connect_string = ''
        t_user = ''
        t_path = ''

        try:
            opts, args = getopt.getopt(argv, "hi:c:u:p:", ["ifile=, icpg=, user=, path="])
        except getopt.GetoptError:
            self.error_sample()

        if len(opts) == 0:
            self.error_sample()

        for opt, arg in opts:
            if opt == '-h':
                print('test.py -i <inputfile> ')
                sys.exit()
            elif opt in ("-i", "--ifile"):
                t_input_file = arg
            elif opt in ("-c", "--icpg"):
                print(urllib.parse.urlparse(arg))
                t_pg_connect_string = urllib.parse.urlparse(arg)
            elif opt in ("-u", "--user"):
                t_user = arg
            elif opt in ("-p", "--path"):
                t_path = arg

        self.pg_connect_string = t_pg_connect_string
        self.input_file = t_input_file
        self.path = t_path
        self.user = t_user

        command_id = self.create_command()
        # self.copy_insert(command_id)
        self.copy_string_iterator(self.read_csv(), command_id)
        print(command_id)

    def error_sample(self):
        print('test.py -i <inputfile> -c <pg_url>')
        print('test.py -i "/tmp/demo.csv" -c "postgresql://postgres:postgres@localhost:5432/test"')
        sys.exit(2)

    def is_connected(self):
        try:
            self.conn.isolation_level
        except:
            self.connect_pg()

    def connect_pg(self):
        username = self.pg_connect_string.username
        password = self.pg_connect_string.password
        database = self.pg_connect_string.path[1:]
        hostname = self.pg_connect_string.hostname
        port = self.pg_connect_string.port

        self.conn = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )

        self.conn.autocommit = True

    def create_command(self):
        self.is_connected()
        sql = """ INSERT INTO public.commande(utilisateur, storage_path) VALUES (%s, %s) RETURNING command_id; """
        cur = self.conn.cursor()
        cur.execute(sql, (self.user, self.path))
        last_id = cur.fetchone()[0]
        return last_id

    def clean_csv_value(self, value: Optional[Any]) -> str:
        if value is None:
            return r'\N'
        return str(value).replace('\n', '\\n')

    def create_staging_table(self, cursor) -> None:
        cursor.execute("""
            DROP TABLE IF EXISTS tmp_c;
            CREATE TEMP TABLE tmp_c (command_id int, session_id TEXT, status TEXT);
        """)

    def read_csv(self):
        return csv.DictReader(open(self.input_file, encoding='utf-8-sig'))

    def copy_string_iterator(self, csvs: Iterator[Dict[str, Any]], command_id) -> None:
        with self.conn.cursor() as cursor:
            self.create_staging_table(cursor)
            events_string_iterator = StringIteratorIO.StringIteratorIO((
                '|'.join(map(self.clean_csv_value, (
                    command_id,
                    csv["session_id"],
                    "unknown"
                ))) + '\n'
                for csv in csvs
            ))
            cursor.copy_from(events_string_iterator, 'events', sep='|', columns=['command_id', 'session_id', 'status'])

    def copy_insert(self, command_id):
        self.is_connected()
        cur = self.conn.cursor()

        # Create Temp Table
        sql = """ CREATE TEMP TABLE tmp_c (command_id int, session_id TEXT, status TEXT); """
        cur.execute(sql)

        # Import Data to Temp Table
        csv_file_name = self.input_file
        sql = "COPY tmp_c(session_id) FROM STDIN DELIMITER ';' CSV HEADER"
        cur.copy_expert(sql, open(csv_file_name, "r"))

        # Update Temp Table with default values
        sql = """ UPDATE tmp_c SET command_id = %s , status = %s; """
        cur.execute(sql, (command_id, "unknown"))

        # Copy Temp Table Data to Table
        sql = """ INSERT INTO events(command_id, session_id, status) SELECT * FROM tmp_c; """
        cur.execute(sql)

        # Drop Data
        sql = """ DROP TABLE tmp_c; """
        cur.execute(sql)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Importer().main(sys.argv[1:])
