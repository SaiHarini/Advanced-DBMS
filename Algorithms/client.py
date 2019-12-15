import socket
import pickle
import psycopg2


USER_ID_COLNAME = 'userId'
MOVIE_ID_COLNAME = 'movieId'
RATING_COLNAME = 'rating'
partitiontablename = "PARTITION1"


def getopenconnection(user='postgres', password='1234', dbname='OriginalDatabase'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def createtable(tablename, cursor):

    try:
        query = "CREATE TABLE {0} ( {1} integer, {2} integer,{3} real);".format(tablename,
                                                                                USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
        cursor.execute(query)
    except Exception as ex:
        print("Failed to create table: ", ex)
    print("Created table:  ", tablename)


if __name__ == '__main__':
    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # get local machine name
    host = "192.168.43.128"
    # socket.gethostname()

    port = 41922

    # connection to hostname on the port.
    s.connect((host, port))

    list_to_recv = pickle.loads(s.recv(2048))
    print list_to_recv
    try:
        # create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        # after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            cur = con.cursor()
            createtable(partitiontablename, cur)

            for d in list_to_recv:
                query = "INSERT into {0} VALUES ({1}, {2}, {3})".format(
                    partitiontablename, d[0], d[1], d[2])
                cur.execute(query)
            con.commit()
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
