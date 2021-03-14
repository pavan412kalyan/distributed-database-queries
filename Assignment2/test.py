import pandas as pd
import threading
import psycopg2
import os
import sys
import math


InputTable1='ratings'
InputTable2='movies'
Table1JoinColumn='MovieId'
Table2JoinColumn='MovieId1'
OutputTable='parallelJoinOutputTable'

con= psycopg2.connect("dbname='" + 'dds_assignment2' + "' user='" + 'postgres' + "' host='localhost' password='" + '1234' + "'")
#######################################################
cursor=con.cursor()
cursor.execute(f'Select * from {OutputTable}')
x=cursor.fetchall()
my_df= pd.DataFrame(x,columns=['a','b','c','d','e','f']) 
###########################################################
cursor.execute(f'select r.*,m.* from {InputTable1} r inner join {InputTable2} m on r.{Table1JoinColumn}=m.{Table2JoinColumn} ')
y=cursor.fetchall()
orig_df= pd.DataFrame(y,columns=['a','b','c','d','e','f']) 

############################################################
orig_df.sort_values(by=['a','b','c','e','f'],inplace=True)
orig_df.reset_index(drop=True,inplace=True)
my_df.sort_values(by=['a','b','c','e','f'],inplace=True)
my_df.reset_index(drop=True,inplace=True)


print(f'my_df shape:{my_df.shape}')
print(f'orig_df shape:{orig_df.shape}')

print(my_df[orig_df.eq(my_df).all(axis=1)==False])

print('Original df ratings value count')
print(orig_df['c'].value_counts())
print('My df ratings value count')
print(my_df['c'].value_counts())



print('Original df userod value count')
print(orig_df['a'].value_counts())
print('My df userid value count')
print(my_df['a'].value_counts())




