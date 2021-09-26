import psycopg2
import getopt
import sys
import urllib.parse

conn = None
pg_connect_string = None
input_file = None
user = None
path = None


def main(self, argv):
    t_input_file = ''
    t_pg_connect_string = ''
    t_user = ''
    t_path = ''

    try:
        opts, args = getopt.getopt(argv, "hi:c:u:p:", ["ifile=, icpg=, user=, path="])
    except getopt.GetoptError:
        error_sample()

    if len(opts) == 0:
        error_sample()

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

    command_id = create_command()
    copy_insert(command_id)


def error_sample():
    print('test.py -i <inputfile> -c <pg_url>')
    print('test.py -i "/tmp/demo.csv" -c "postgresql://postgres:postgres@localhost:5432/postgres"')
    sys.exit(2)


def is_connected(self):
    try:
        self.conn.isolation_level
    except:
        connect_pg()


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


def create_command(self):
    is_connected()
    sql = """ INSERT INTO public.commande(utilisateur, storage_path) VALUES (%s, %s); """
    cur = self.conn.cursor()
    cur.execute(sql, (self.user, self.path))
    cur.execute('SELECT LASTVAL()')
    lastid = cur.fetchone()['lastval']
    return lastid


def copy_insert(self, command_id):
    is_connected()
    self.conn.autocommit = True
    cur = self.conn.cursor()

    # Create Temp Table
    sql = """ CREATE TEMP TABLE tmp_c (command_id int, session_id TEXT, status TEXT); """
    cur.execute(sql)

    # Import Data to Temp Table
    csv_file_name = self.input_file
    sql = "COPY tmp_c(session_id) FROM STDIN DELIMITER ';' CSV HEADER"
    cur.copy_expert(sql, open(csv_file_name, "r"))

    # Update Temp Table with default values
    sql = """ UPDATE temp_c SET  command_id = %s AND status = %s FROM tmp_x; """
    cur.execute(sql, (command_id, "unknown"))

    # Copy Temp Table Data to Table
    sql = """ INSERT INTO events(command_id, session_id, status) SELECT * FROM temp_c; """
    cur.execute(sql)

    # Drop Data
    sql = """ DROP TABLE tmp_c; """
    cur.execute(sql)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(sys.argv[1:])
