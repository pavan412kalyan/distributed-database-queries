#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading
import math


#InputTable='ratings'
#SortingColumnName= 'Rating'
#OutputTable='parallelSortOutputTable'
#openconnection= psycopg2.connect("dbname='" + 'dds_assignment2' + "' user='" + 'postgres' + "' host='localhost' password='" + 'admin' + "'")


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation
    #range partition
    total_threads=5
    threads=[0]*total_threads
    RANGE_TABLE_PREFIX='RANGE_PARTITION_TABLE'
    con = openconnection
    cursor = con.cursor()
    
    #finding boundaries to find the interval for range partition
    cursor.execute(f'SELECT MIN({SortingColumnName}) AS min_rating, MAX({SortingColumnName}) AS max_rating FROM {InputTable};')
    min_value,max_value=cursor.fetchone()
    range_step=float(max_value-min_value)/total_threads
    
    #Query for creating output table and delete existing table 
    delete_query=(f'DROP TABLE IF EXISTS {OutputTable};')
    create_query=(f'CREATE TABLE {OutputTable} AS SELECT * FROM {InputTable} WHERE 1=2;')
    cursor.execute(delete_query)
    cursor.execute(create_query)
    con.commit()
    
    ###########
    
    for i in range(total_threads):
        TABLE_NAME=RANGE_TABLE_PREFIX+'_'+str(i+1)
        max_value=round(min_value+range_step,2)
        #drop if table exists
        delete_query=(f'DROP TABLE IF EXISTS {TABLE_NAME};')
        #create table, insert records and sort the records
        if i==0:
            creatInsertSortQ=(f'CREATE TABLE {TABLE_NAME} AS SELECT * FROM {InputTable} WHERE {SortingColumnName} >= {min_value} AND {SortingColumnName} <= {max_value} ORDER BY {SortingColumnName} ASC;')
        else:
            creatInsertSortQ=(f'CREATE TABLE {TABLE_NAME} AS SELECT * FROM {InputTable} WHERE {SortingColumnName} > {min_value} AND {SortingColumnName} <= {max_value} ORDER BY {SortingColumnName} ASC;')
        
        threads[i]=threading.Thread(target=lambda: [cursor.execute(query) for query in [delete_query,creatInsertSortQ]])
        threads[i].start()
        min_value=round(max_value,2)
    con.commit()

    #merge sorted tables
    for i in range(total_threads):
        threads[i].join()
        TABLE_NAME=RANGE_TABLE_PREFIX+'_'+str(i+1)
        exists_query=(f"SELECT count(*) FROM information_schema.tables WHERE table_name='{TABLE_NAME}';")
        insert_query=(f'INSERT INTO { OutputTable } SELECT * FROM { TABLE_NAME } ;')     
        cursor.execute(exists_query)
        if cursor.rowcount>0:
            cursor.execute(insert_query)
    con.commit() 
    
    
    #delete temporary partitions
    for i in range(total_threads):
        TABLE_NAME=RANGE_TABLE_PREFIX+'_'+str(i+1)
        delete_query=(f'DROP TABLE IF EXISTS {TABLE_NAME};')
        cursor.execute(delete_query)
    con.commit() 
    if cursor:
        cursor.close()



#InputTable1='ratings'
#InputTable2='movies'
#Table1JoinColumn='MovieId'
#Table2JoinColumn='MovieId1'
#OutputTable='parallelJoinOutputTable'    

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation

    con = openconnection
    
    TEMP_TABLE1_PREFIX='temp_partition_table1'
    TEMP_TABLE2_PREFIX='temp_partition_table2'
    total_threads=5
    threads=[0]*total_threads
    cursor=con.cursor()
    
    #create output table
    delete_query=(f'DROP TABLE IF EXISTS {OutputTable};')
    create_query=(f"""CREATE TABLE {OutputTable} AS SELECT {InputTable1}.*, {InputTable2}.* FROM {InputTable1},{InputTable2} WHERE 1=2 ;""")
    cursor.execute(delete_query)
    cursor.execute(create_query)
    con.commit()
    
     #get boundaries - max and min
    cursor.execute(f'SELECT MIN({Table1JoinColumn}) , MAX({Table1JoinColumn}) FROM {InputTable1};')
    min_value_table1,max_value_table1=cursor.fetchone()
    cursor.execute(f'SELECT MIN({Table2JoinColumn}) , MAX({Table2JoinColumn}) FROM {InputTable2};')
    min_value_table2,max_value_table2=cursor.fetchone()
    total_min,total_max=min(min_value_table1,min_value_table2), max(max_value_table1,max_value_table2)
    range_step=float(total_max- total_min)/total_threads
    
    
    
    #Range partition table1 and table2
    min_value=total_min
    for i in range(total_threads):
        TABLE1_NAME=TEMP_TABLE1_PREFIX+'_'+str(i+1)
        TABLE2_NAME=TEMP_TABLE2_PREFIX+'_'+str(i+1)
        max_value=round(min_value+range_step,2)
        #drop if table exists
        delete_query=(f'DROP TABLE IF EXISTS {TABLE1_NAME},{TABLE2_NAME};')
        #sort in  the partition
        if i==0:
            createInsertQ1=(f'CREATE TABLE {TABLE1_NAME} AS SELECT * FROM {InputTable1} WHERE {Table1JoinColumn} >= {min_value} AND {Table1JoinColumn} <= {max_value} ;')
            createInsertQ2=(f'CREATE TABLE {TABLE2_NAME} AS SELECT * FROM {InputTable2} WHERE {Table2JoinColumn} >= {min_value} AND {Table2JoinColumn} <= {max_value} ;')            
#            createInsertQ1=(f'CREATE TABLE {TABLE1_NAME} AS SELECT * FROM {InputTable1} WHERE {Table1JoinColumn} >= {min_value} AND {Table1JoinColumn} <= {max_value} ORDER BY {Table1JoinColumn} ASC;')
#            createInsertQ2=(f'CREATE TABLE {TABLE2_NAME} AS SELECT * FROM {InputTable2} WHERE {Table2JoinColumn} >= {min_value} AND {Table2JoinColumn} <= {max_value} ORDER BY {Table2JoinColumn} ASC;')

        else:
#            createInsertQ1=(f'CREATE TABLE {TABLE1_NAME} AS SELECT * FROM {InputTable1} WHERE {Table1JoinColumn} > {min_value} AND {Table1JoinColumn} <= {max_value} ORDER BY {Table1JoinColumn} ASC;')
#            createInsertQ2=(f'CREATE TABLE {TABLE2_NAME} AS SELECT * FROM {InputTable2} WHERE {Table2JoinColumn} > {min_value} AND {Table2JoinColumn} <= {max_value} ORDER BY {Table2JoinColumn} ASC;')

            createInsertQ1=(f'CREATE TABLE {TABLE1_NAME} AS SELECT * FROM {InputTable1} WHERE {Table1JoinColumn} > {min_value} AND {Table1JoinColumn} <= {max_value} ;')
            createInsertQ2=(f'CREATE TABLE {TABLE2_NAME} AS SELECT * FROM {InputTable2} WHERE {Table2JoinColumn} > {min_value} AND {Table2JoinColumn} <= {max_value} ;')
#            
#            
        Join_T1_T2=(f'INSERT INTO {OutputTable} (SELECT T1.*,T2.* from {TABLE1_NAME} AS T1 INNER JOIN {TABLE2_NAME} AS T2 ON T1.{Table1JoinColumn}=T2.{Table2JoinColumn});')
        threads[i]=threading.Thread(target=lambda: [cursor.execute(query) for query in [delete_query,createInsertQ1,createInsertQ2,Join_T1_T2]])
        threads[i].start()
        min_value=round(max_value,2)
    con.commit()

    for i in range(total_threads):
        threads[i].join()
    con.commit() 
    
    #####
    for i in range(total_threads):
        TABLE1_NAME=TEMP_TABLE1_PREFIX+'_'+str(i+1)
        TABLE2_NAME=TEMP_TABLE2_PREFIX+'_'+str(i+1)
        drop_query=(f'DROP TABLE IF EXISTS {TABLE1_NAME},{TABLE2_NAME};')
        cursor.execute(drop_query)
    con.commit() 
    if cursor:
        cursor.close()



    

    

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
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

# Donot change this function
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


