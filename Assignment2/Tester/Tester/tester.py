#
# Tester
#

from subprocess import call
import os
import shutil
import psycopg2
import sys
import Assignment2_Interface as Assignment2

DATABASE_NAME = 'dds_assignment2'

def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)

    cur.execute("CREATE TABLE "+ratingstablename+" (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")

    loadout = open(ratingsfilepath,'r')

    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")

    cur.close()
    openconnection.commit()

def loadMovies(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)

    cur.execute("CREATE TABLE "+ratingstablename+" (MovieId1 INT,  Title VARCHAR(100),  Genre VARCHAR(100))")

    loadout = open(ratingsfilepath,'r')

    cur.copy_from(loadout,ratingstablename,sep = '_',columns=('MovieId1','Title','Genre'))
    #cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")

    cur.close()
    openconnection.commit()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print("Creating Database named as dds_assignment2")
        createDB(DATABASE_NAME);

        # Getting connection to the database
        print("Getting connection from the dds_assignment2 database")
        con = getOpenConnection(dbname = DATABASE_NAME);

        #Loading two tables ratings and movies
        loadRatings('ratings', 'ratings.dat', con);
        loadMovies('movies', 'movies.dat', con);

        # Calling ParallelSort
        print("Performing Parallel Sort")
        Assignment2.ParallelSort('ratings', 'Rating', 'parallelSortOutputTable', con);
        a = input("Check in database and once you are done, press enter to delete all tables: ");
        deleteTables('ALL', con);

        #Loading two tables ratings and movies
        loadRatings('ratings', 'ratings.dat', con);
        loadMovies('movies', 'movies.dat', con);
        # Calling ParallelJoin
        print("Performing Parallel Join")
        Assignment2.ParallelJoin('ratings', 'movies', 'MovieId', 'MovieId1', 'parallelJoinOutputTable', con);
        a = input("Check in database and once you are done, press enter to delete all tables: ");
        deleteTables('ALL', con);

        if con:
            con.close()

    except Exception as detail:
        print("Something bad has happened!!! This is the error ==> ", detail)
