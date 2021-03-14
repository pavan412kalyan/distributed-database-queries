import psycopg2
import os
import sys

#DATABASE_NAME='postgres'
#ratingsfilepath= r"C:\Users\vikky\Desktop\DDS_ASSNMNT\Assignment1\test_data1.txt"
#openconnection = psycopg2.connect("host=localhost dbname=postgres user=postgres password=admin")
#ratingstablename='ratings'
#check pw 


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    
     # Remove this once you are done with implementation 
     
     
 
    #createDB(DATABASE_NAME)
    try :
        con = openconnection
        cursor = con.cursor()
        cursor.execute("create table " + ratingstablename + "(userid integer, sep1 char, movieid integer, sep2 char, rating float, sep3 char, timestamp bigint);")
        cursor.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
        cursor.execute("alter table " + ratingstablename + " drop column sep1, drop column sep2, drop column sep3, drop column timestamp;")
        con.commit()
    
        
    except(Exception, psycopg2.DatabaseError) as dberror:
        print("DatabaseError: {0}".format(dberror))
        if openconnection: openconnection.rollback() 
          
    except IOError as ioerror:
        print("InputOutput Error: {0}".format(ioerror))
        if openconnection: openconnection.rollback()     
        
    finally :        
            cursor.close();

     
     



def rangePartition(ratingstablename, numberofpartitions, openconnection):
    
        range_step=5/numberofpartitions
        start_range=0
        end_range=0
        try:
            cursor = openconnection.cursor()
            for partition in range(0,numberofpartitions):
                RANGE_PARTITION_TABLE_FULLNAME= """range_ratings_part"""+str(partition)
                end_range=start_range+range_step

                create_query = """CREATE TABLE """+ RANGE_PARTITION_TABLE_FULLNAME + """(userid INTEGER,movieid INTEGER,rating FLOAT);"""
                
                if partition==0:
                    insert_to_partiton_query= """INSERT INTO """ + RANGE_PARTITION_TABLE_FULLNAME +"""(userid, movieid, rating)"""+ """ SELECT userid, movieid,rating from """+ ratingstablename+""" WHERE rating>=""" + str(start_range) + """ AND """ + """ rating<="""+ str(end_range)
            
                else:
                    insert_to_partiton_query= """INSERT INTO """ + RANGE_PARTITION_TABLE_FULLNAME +"""(userid, movieid, rating)"""+ """ SELECT userid, movieid,rating from """+ ratingstablename+""" WHERE rating>""" + str(start_range) + """ AND """ + """ rating<="""+ str(end_range)

                start_range=end_range

                
                cursor.execute(create_query) 
                cursor.execute(insert_to_partiton_query) 
                openconnection.commit() 
              
        except(Exception, psycopg2.DatabaseError) as dberror:
            print("DATABASE: {0}".format(dberror))
            if openconnection: 
                openconnection.rollback() #rollback the transcation if any error
        
        finally:
            cursor.close()

    




def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    
    
    #pass
            try:
                cursor = openconnection.cursor()
                for partition in range(0,numberofpartitions):
                    RR_PART_FULLNAME= """round_robin_ratings_part"""+str(partition)
    
                    create_table_query = """CREATE TABLE """+ RR_PART_FULLNAME + """(userid INTEGER,movieid INTEGER,rating FLOAT);"""
                    
                    insert_query= """INSERT INTO """+ RR_PART_FULLNAME + """(userid, movieid, rating) \
                    SELECT userid, movieid, rating FROM ( SELECT userid, movieid, rating, ROW_NUMBER () OVER () AS rownbr FROM ratings ) as temp_table \
                        WHERE (rownbr-1) % """ +str(numberofpartitions) + """ = """ + str(partition)
    
                    cursor.execute(create_table_query) 
                    cursor.execute(insert_query)
                    openconnection.commit()
                  
            except(Exception, psycopg2.DatabaseError) as dberror:
                print("DataBaseError: {0}".format(dberror))
                if openconnection: 
                    openconnection.rollback() 
        
            finally:
                cursor.close()
                
    
     

    


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    #pass # Remove this once you are done with implementation
    con = openconnection
    cursor = con.cursor()
    #query to insert into ratings table
    cursor.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
    #query to get total rows
    cursor.execute("select count(*) from " + ratingstablename + ";");   
    total_rows = (cursor.fetchall())[0][0]
    
    
    #query for total partitions
    select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';""" 
    cursor.execute(select_query)
    numberOfPartitions=cursor.fetchall()[0][0]
    
     #finding the partition table
    RR_PARTITION_TABLE= """round_robin_ratings_part"""+str((total_rows-1)%numberOfPartitions)
    #print(RR_PARTITION_TABLE)
     
    cursor.execute("insert into " + RR_PARTITION_TABLE + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cursor.close()
    con.commit() 

    
     
  
    
    
    


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    #pass # Remove this once you are done with implementation
    con = openconnection
    cursor = con.cursor()
    
    #query to insert into ratings table
    cursor.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    
    
    #query for total partitions
    select_query="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';""" 
    cursor.execute(select_query)
    numberOfPartitions=cursor.fetchall()[0][0]
    
    
    range_step = 5 / numberOfPartitions
    index = int(rating / range_step)
    if rating % range_step == 0 and index != 0:
        index = index - 1
    RR_PART_TABLENAME = "range_ratings_part" + str(index)
    cursor.execute("insert into " + RR_PART_TABLENAME + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cursor.close()
    con.commit()
    
    
    

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #pass #Remove this once you are done with implementation
    
    # to store output
    #output_range_query=[]
    file_object = open(outputPath, 'w+')

    
    
    
    
    con = openconnection
    cursor = con.cursor()
    
    #ROUND ROBIN
    #Query for count of total roundrobin partitions
    select_query_count="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';""" 
    cursor.execute(select_query_count)
    numberofpartitions=cursor.fetchall()[0][0]
    for i in range(0,numberofpartitions):
        ROUND_ROBIN_PARTITION= """round_robin_ratings_part"""+str(i)
        range_query="""SELECT * FROM {0} WHERE rating>={1} AND rating<={2}""".format(ROUND_ROBIN_PARTITION,ratingMinValue,ratingMaxValue)
        cursor.execute(range_query)
        for record in cursor.fetchall():

            line="""{0},{1},{2},{3}""".format(ROUND_ROBIN_PARTITION,record[0],record[1],record[2])
            file_object.write(line)
            file_object.write('\n')


    
    
    #RANGE QUERY
    #query for total range partitions
    select_query_count="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';""" 
    cursor.execute(select_query_count)
    numberOfPartitions=cursor.fetchall()[0][0]
    
    selectedPartition=[]
    range_step=5/numberOfPartitions
    start_range=0
    end_range=0
    
    for  partition in range(0,numberOfPartitions) :
        end_range=start_range + range_step
        
        if  start_range<=ratingMinValue<=end_range or start_range<=ratingMaxValue<=end_range :
            selectedPartition.append(partition)
        start_range=end_range    
    
    #for  partition in range(selectedPartition[0],selectedPartition[-1]+1) :
    for  partition in range(0,numberOfPartitions) :

        
        RANGE_PARTITION= """range_ratings_part"""+str(partition)
        range_query="""SELECT * FROM {0} WHERE rating>={1} AND rating<={2}""".format(RANGE_PARTITION,ratingMinValue,ratingMaxValue)
        cursor.execute(range_query)
        for record in cursor.fetchall():
            #print("""{0},{1},{2},{3}""".format(RANGE_PARTITION,record[0],record[1],record[2]))
            line="""{0},{1},{2},{3}""".format(RANGE_PARTITION,record[0],record[1],record[2])
            file_object.write(line)
            file_object.write('\n')

    


    file_object.close()
    
             
 
        


def pointQuery(ratingValue, openconnection, outputPath):
    #pass # Remove this once you are done with implementation
    
    
    #open file in OutputPath 
    file_object = open(outputPath, 'w+')

    
    con = openconnection
    cursor = con.cursor()
    
    #ROUND ROBIN
    #Query for count of total roundrobin partitions
    select_query_count="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'round_robin_ratings_part%';""" 
    cursor.execute(select_query_count)
    numberofpartitions=cursor.fetchall()[0][0]
    for i in range(0,numberofpartitions):
        ROUND_ROBIN_PARTITION= """round_robin_ratings_part"""+str(i)
        range_query="""SELECT * FROM {0} WHERE rating={1}""".format(ROUND_ROBIN_PARTITION,ratingValue)
        cursor.execute(range_query)
        for record in cursor.fetchall():
            #print("""{0},{1},{2},{3}""".format(ROUND_ROBIN_PARTITION,record[0],record[1],record[2]))
            line="""{0},{1},{2},{3}""".format(ROUND_ROBIN_PARTITION,record[0],record[1],record[2])
            file_object.write(line)
            file_object.write('\n')

    #RANGE QUERY
    #query for total range partitions
    select_query_count="""SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'range_ratings_part%';""" 
    cursor.execute(select_query_count)
    numberOfPartitions=cursor.fetchall()[0][0]
    
    selectedPartition=[]
    range_step=5/numberOfPartitions
    start_range=0
    end_range=0
    
    for  partition in range(0,numberOfPartitions) :
        end_range=start_range + range_step
        
        if   start_range<=ratingValue<=end_range :
            selectedPartition.append(partition)
        start_range=end_range    
    
    for  partition in selectedPartition :
                
        RANGE_PARTITION= """range_ratings_part"""+str(partition)
        range_query="""SELECT * FROM {0} WHERE rating={1}""".format(RANGE_PARTITION,ratingValue)
        cursor.execute(range_query)
        for record in cursor.fetchall():
            
            line="""{0},{1},{2},{3}""".format(RANGE_PARTITION,record[0],record[1],record[2])
            file_object.write(line)
            file_object.write('\n')
    
    



    file_object.close()
    
             
    
    
    
    



def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
