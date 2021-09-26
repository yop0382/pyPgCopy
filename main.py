import psycopg2
import getopt
import sys
import urllib.parse


def main(argv):
    input_file = ''
    pg_connect_string = ''

    try:
        opts, args = getopt.getopt(argv, "hi:c:", ["ifile=, icpg="])
    except getopt.GetoptError:
        error_sample()

    if len(opts) == 0:
        error_sample()

    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputfile> ')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_file = arg
        elif opt in ("-c", "--icpg"):
            print(urllib.parse.urlparse(arg))
            pg_connect_string = urllib.parse.urlparse(arg)

    copy_insert(input_file, pg_connect_string)


def error_sample():
    print('test.py -i <inputfile> -c <pg_url>')
    print('test.py -i "/tmp/demo.csv" -c "postgresql://postgres:postgres@localhost:5432/postgres"')
    sys.exit(2)


def copy_insert(input_file, pg_connect_string):
    username = pg_connect_string.username
    password = pg_connect_string.password
    database = pg_connect_string.path[1:]
    hostname = pg_connect_string.hostname
    port = pg_connect_string.port

    conn = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname,
        port=port
    )

    cur = conn.cursor()
    csv_file_name = input_file
    sql = "COPY events(command_id, session_id, status) FROM STDIN DELIMITER ';' CSV HEADER"
    cur.copy_expert(sql, open(csv_file_name, "r"))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(sys.argv[1:])