# -*- coding: utf-8 -*-

import psycopg2
import traceback


RATINGS_TABLE = 'ratings'
INPUT_FILE_PATH = r'c:\Users\Lokeshwar\Documents\MS\DDS\ml-10M100K\ratings.txt'
#INPUT_FILE_PATH = r'C:\Users\Lokeshwar\Documents\MS\DDS\Assignment1\test_data1.txt'



con=psycopg2.connect(
    host='localhost',
    database='postgres',
    user='postgres',
    password='1234',
    port='5432'
)


def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    create_query = """CREATE TABLE """+ ratingstablename + """(
                                                            userid INTEGER,
                                                            dummycol1 CHAR,
                                                            movieid INTEGER,
                                                            dummycol2 CHAR,
                                                            rating FLOAT,
                                                            dummycol3 CHAR,
                                                            timestamp BIGINT);"""
    
    delete_query = """ALTER TABLE """ + ratingstablename + """ DROP COLUMN dummycol1,
                                                             DROP COLUMN dummycol2,
                                                             DROP COLUMN dummycol3,
                                                             DROP COLUMN timestamp;"""
    
    select_query="""SELECT count(*) FROM """+ ratingstablename
    try:
        cursor = openconnection.cursor()
        cursor.execute(create_query) #create ratings table
        openconnection.commit() #commit the transaction
        cursor.copy_from(open(ratingsfilepath,'r'),ratingstablename,sep=':') #insert data into the ratings table
        cursor.execute(delete_query) #delete dummy columns from ratings table
        openconnection.commit() #commit the transaction
        cursor.execute(select_query)
        print("{0} records are  successfully inserted into the {1} table".format(cursor.fetchone()[0],ratingstablename))

            
    except(Exception, psycopg2.DatabaseError) as dberror:
        print("DB Error: {0}".format(dberror))
        if openconnection: openconnection.rollback() #rollback the transcation if any error
          
    except IOError as ioerror:
        print("IO Error: {0}".format(ioerror))
        if openconnection: openconnection.rollback() #rollback the transcation if any error

    finally:
        cursor.close()



def rangePartition(ratingstablename, numberofpartitions,openconnection):
    if numberofpartitions==1:
        print('No need to partition the table')
    else:
        step=5/numberofpartitions
        partition_intervals=[]
        start_range=0
        end_range=0
        try:
            cursor = openconnection.cursor()
            for partition in range(0,numberofpartitions):
                RANGE_PARTITION_TABLE= """range_ratings_part"""+str(partition)
                end_range=start_range+step

                create_query = """CREATE TABLE """+ RANGE_PARTITION_TABLE + """(
                                                                userid INTEGER,
                                                                movieid INTEGER,
                                                                rating FLOAT);"""
                
                if partition==0:
                    copy_query= """INSERT INTO """ + RANGE_PARTITION_TABLE +"""(userid, movieid, rating)"""+ """ SELECT userid, movieid,rating from """+ ratingstablename+ \
                                                            """ WHERE rating>=""" + str(start_range) + """ AND """ + """ rating<="""+ str(end_range)
            
                else:
                    copy_query= """INSERT INTO """ + RANGE_PARTITION_TABLE +"""(userid, movieid, rating)"""+ """ SELECT userid, movieid,rating from """+ ratingstablename+ \
                                                            """ WHERE rating>""" + str(start_range) + """ AND """ + """ rating<="""+ str(end_range)

                start_range=end_range

                
                cursor.execute(create_query) #create partition ratings table
                #openconnection.commit() #commit the transaction
                cursor.execute(copy_query) #copy data from ratings table to partition ratings table
                openconnection.commit() #commit the transaction
              
        except(Exception, psycopg2.DatabaseError) as dberror:
            print("Error: {0}".format(dberror))
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        
        finally:
            cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions==1:
        print('No need to partition the table')
    else:
        try:
            cursor = openconnection.cursor()
            for partition in range(0,numberofpartitions):
                RR_PARTITION_TABLE= """round_robin_ratings_part"""+str(partition)

                create_query = """CREATE TABLE """+ RR_PARTITION_TABLE + """(
                                                                userid INTEGER,
                                                                movieid INTEGER,
                                                                rating FLOAT);"""
                
                copy_query= """INSERT INTO """+ RR_PARTITION_TABLE + """(userid, movieid, rating) \
                SELECT userid, movieid, rating FROM ( SELECT userid, movieid, rating, ROW_NUMBER () OVER () AS rownbr FROM ratings ) as temp_table \
                    WHERE (rownbr-1) % """ +str(numberofpartitions) + """ = """ + str(partition)

                cursor.execute(create_query) #create partition ratings table
                #openconnection.commit() #commit the transaction
                cursor.execute(copy_query) #copy data from ratings table to partition ratings table
                openconnection.commit() #commit the transaction
              
        except(Exception, psycopg2.DatabaseError) as dberror:
            print("Error: {0}".format(dberror))
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        
        finally:
            cursor.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating<0 or rating>5:
        print('Rating value should be between 0-5')
    else:
        try:
            #check if the ratings table exists in DB
            cursor = openconnection.cursor()
            select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name="""+ "'"+ratingstablename +"'"
            cursor.execute(select_query)

            #if ratings table exists:
            if cursor.fetchall()[0][0]>0:
                insert_query= """INSERT INTO {0} (userid, movieid, rating) VALUES ({1},{2},{3})""".format(ratingstablename,userid,itemid,rating) #insert new record into the ratings table
                cursor.execute(insert_query)
                openconnection.commit()

                get_row_number_query="""SELECT  ROW_NUMBER () OVER () AS rownbr FROM {0} order by rownbr desc LIMIT 1""".format(ratingstablename) #get row number of the latest record
                cursor.execute(get_row_number_query)
                row_number=cursor.fetchall()[0][0]

                select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';""" #get the count of round robin partition tables
                cursor.execute(select_query)
                number_of_partitions=cursor.fetchall()[0][0]
                if number_of_partitions >1: #if round robin partition tables exists-
                    RR_PARTITION_TABLE= """round_robin_ratings_part"""+str((row_number-1)%number_of_partitions) # get the round robin partition table name where the new record will be inserted
                    insert_query= """INSERT INTO {0} (userid, movieid, rating) VALUES ({1},{2},{3})""".format(RR_PARTITION_TABLE,userid,itemid,rating) #insert the new record into the round robin partition table
                    cursor.execute(insert_query)
                    openconnection.commit() #commit the transaction
                    
        except(Exception, psycopg2.DatabaseError) as dberror:
            print("Error: {0}".format(dberror))
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        
        finally:
            cursor.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating<0 or rating>5:
        print('Rating value should be between 0-5')
    else:
        try:
            #check if the ratings table exists in DB
            cursor = openconnection.cursor()
            select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name="""+ "'"+ratingstablename +"'"
            cursor.execute(select_query)

            #if ratings table exists:
            if cursor.fetchall()[0][0]>0:
                insert_query= """INSERT INTO {0} (userid, movieid, rating) VALUES ({1},{2},{3})""".format(ratingstablename,userid,itemid,rating) #insert new record into the ratings table
                cursor.execute(insert_query)
                openconnection.commit()
           
                select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';""" #get the count of range partition tables
                cursor.execute(select_query)
                numberofpartitions=cursor.fetchall()[0][0]
                if numberofpartitions >1: #if range partition tables exists-
                    if rating==0:
                        partition_number=0
                    else:
                        step=5/numberofpartitions
                        start_range=0
                        end_range=0
                        for i in range(0,numberofpartitions):
                            end_range=start_range+step
                            if rating>start_range and rating<=end_range:
                                partition_number=i
                                break
                            start_range=end_range
                    RANGE_PARTITION_TABLE= """range_ratings_part"""+str(partition_number) # get the range partition table name where the new record will be inserted
                    insert_query= """INSERT INTO {0} (userid, movieid, rating) VALUES ({1},{2},{3})""".format(RANGE_PARTITION_TABLE,userid,itemid,rating) #insert the new record into the range partition table
                    cursor.execute(insert_query)
                    openconnection.commit() #commit the transaction
                    
        except(Exception, psycopg2.DatabaseError) as dberror:
            print("Error: {0}",dberror)
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        
        finally:
            cursor.close()



def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    if ratingMaxValue>5 or ratingMinValue<0:
        print('Ratings should be between 0-5 inclusive')
    else:
        try:
            cursor = openconnection.cursor()
            #Querying round robin partition tables
            select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';""" #get the count of round robin partition tables
            cursor.execute(select_query)
            numberofpartitions=cursor.fetchall()[0][0]
            for i in range(0,numberofpartitions):
                RR_PARTITION_TABLE= """round_robin_ratings_part"""+str(i)
                range_query="""SELECT * FROM {0} WHERE rating>={1} AND rating<={2}""".format(RR_PARTITION_TABLE,ratingMinValue,ratingMaxValue)
                cursor.execute(range_query)
                for record in cursor.fetchall():
                    print("""{0},{1},{2},{3}""".format(RR_PARTITION_TABLE,record[0],record[1],record[2]))
            
            #Querying range partion tables
            table_postfix=[]
            select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';""" #get the count of range partition tables
            cursor.execute(select_query)
            numberofpartitions=cursor.fetchall()[0][0]
            step=5/numberofpartitions
            start_range=0
            end_range=0
            for i in range(0,numberofpartitions):
                end_range=start_range+step
                if start_range<= ratingMinValue<=end_range:
                    table_postfix.append(i)
                if start_range<= ratingMaxValue<=end_range and (i not in table_postfix):
                    table_postfix.append(i)
                start_range=end_range

            for postfix in table_postfix:
                RANGE_PARTITION_TABLE= """round_robin_ratings_part"""+str(postfix)
                range_query="""SELECT * FROM {0} WHERE rating>={1} AND rating<={2}""".format(RANGE_PARTITION_TABLE,ratingMinValue,ratingMaxValue)
                cursor.execute(range_query)
                for record in cursor.fetchall():
                    print("""{0},{1},{2},{3}""".format(RANGE_PARTITION_TABLE,record[0],record[1],record[2]))



        except(Exception, psycopg2.DatabaseError) as dberror:
            print("Error: {0}",dberror)
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        finally:
            cursor.close()


def pointQuery(ratingValue, openconnection, outputPath):
    if ratingValue>5 or ratingValue<0:
        print('Ratings should be between 0-5 inclusive')
    else:
        try:
            cursor = openconnection.cursor()
            #Querying round robin partition tables
            select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';""" #get the count of round robin partition tables
            cursor.execute(select_query)
            numberofpartitions=cursor.fetchall()[0][0]
            for i in range(0,numberofpartitions):
                RR_PARTITION_TABLE= """round_robin_ratings_part"""+str(i)
                range_query="""SELECT * FROM {0} WHERE rating = {1}""".format(RR_PARTITION_TABLE,ratingValue)
                cursor.execute(range_query)
                for record in cursor.fetchall():
                    print("""{0},{1},{2},{3}""".format(RR_PARTITION_TABLE,record[0],record[1],record[2]))
            
            #Querying range partion tables
            select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';""" #get the count of range partition tables
            cursor.execute(select_query)
            numberofpartitions=cursor.fetchall()[0][0]
            if numberofpartitions >1: #if range partition tables exists-
                step=5/numberofpartitions
                start_range=0
                end_range=0
                for i in range(0,numberofpartitions):
                    end_range=start_range+step
                    if ratingValue>start_range and ratingValue<=end_range:
                        partition_number=i
                        break
                    start_range=end_range
                RANGE_PARTITION_TABLE= """range_ratings_part"""+str(i)
                range_query="""SELECT * FROM {0} WHERE rating = {1}""".format(RANGE_PARTITION_TABLE,ratingValue)
                cursor.execute(range_query)
                for record in cursor.fetchall():
                    print("""{0},{1},{2},{3}""".format(RANGE_PARTITION_TABLE,record[0],record[1],record[2]))

        except(Exception, psycopg2.DatabaseError) as dberror:
            print("Error: {0}",dberror)
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        finally:
            cursor.close()