#!/usr/bin/python2.7
#


import psycopg2
import os
import sys
import time

RANGE_RATINGS_METADATA = 'rangeratingsmetadata'
ROUND_ROBIN_RATINGS_METADATA = 'roundrobinratingsmetadata'

RANGE_PARTITION_PREFIX = 'rangeratingspart'
ROUND_ROBIN_PARTITION_PREFIX = 'rrobin_part'
# 'roundrobinratingspart'

RANGE_PARTITION_OUTPUT_NAME = 'range_part'
# 'RangeRatingsPart'
ROUND_ROBIN_PARTITION_OUTPUT_NAME = 'rrobin_part'
# 'RoundRobinRatingsPart'

RANGE_QUERY_OUTPUT_FILE = 'RangeQueryOut.txt'
POINT_QUERY_OUTPUT_FILE = 'PointQueryOut.txt'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    
    #
    # Range query on range partitions

    try:
        cur = openconnection.cursor()
        start_time = time.time()
        print("started for range on range partns")

        # finding min boundary range of partition from metadata for given ratingMinValue 
        query = "select  max(minrating) from {0} where minrating <= {1}".format(RANGE_RATINGS_METADATA,ratingMinValue)	
        cur.execute(query)
        minpartboundary = cur.fetchone()[0]

        # finding min boundary range of partition from metadata for given ratingMinValue
        query = "select  min(maxrating) from {0} where maxrating >= {1}".format(RANGE_RATINGS_METADATA,ratingMaxValue)
        cur.execute(query)
        maxpartboundary = cur.fetchone()[0]

        # calculating the paratitions from metadata table where tuples of  given ranges lies 
        query = "select  partitionnum from {0} where maxrating >= {1} and maxrating <= {2}".format(RANGE_RATINGS_METADATA,minpartboundary,maxpartboundary)
        cur.execute(query)
        rows = cur.fetchall()

        if os.path.exists(RANGE_QUERY_OUTPUT_FILE):
            os.remove(RANGE_QUERY_OUTPUT_FILE)

        for i in rows:
            partitionname = RANGE_PARTITION_OUTPUT_NAME + repr(i[0])
            query = "select * from {0} where rating >= {1} and rating <= {2}".format(partitionname, ratingMinValue, ratingMaxValue) 
            cur.execute(query)
            rows2 = cur.fetchall()
            with open(RANGE_QUERY_OUTPUT_FILE,'a+') as f:
                for j in rows2:
                    f.write("%s," % partitionname)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))
        #
        print("My program took", time.time() - start_time, "to run RangeQuery on range partns")
        # Range query on round robin paritions

        # # get no of round robin partitions
        start_time = time.time()
        print("started for range on rrobin partns")
        query = "select partitionnum from {0} ".format(ROUND_ROBIN_RATINGS_METADATA)	
        cur.execute(query)
        rrpartitioncount = int(cur.fetchone()[0])
        
        for i in range(rrpartitioncount):
            partitionname = ROUND_ROBIN_PARTITION_OUTPUT_NAME + repr(i)
            query = "select * from {0} where rating >= {1} and rating <= {2}".format(partitionname, ratingMinValue, ratingMaxValue) 
            cur.execute(query)
            rows2 = cur.fetchall()
            with open(RANGE_QUERY_OUTPUT_FILE,'a+') as f:
                for j in rows2:
                    f.write("%s," % partitionname)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))

        print("My program took", time.time() - start_time, "to run RangeQuery on rrobin partns")
    except Exception as ex:
        print("Exception while processing RangeQuery: ",ex)


def PointQuery(ratingsTableName, ratingValue, openconnection):

    # Point query for range partition 
    try:
        start_time = time.time()
        print("started for point in range partn")
        cur = openconnection.cursor()
        if ratingValue == 0:
            rangepartitionnum = 0
        else:
            query = "select partitionnum from {0} where minrating < {1} and maxrating >= {1}".format(RANGE_RATINGS_METADATA,ratingValue)
            cur.execute(query)
            rangepartitionnum = cur.fetchone()[0]

        partitionname = RANGE_PARTITION_OUTPUT_NAME + repr(rangepartitionnum)
        query = "select * from {0} where rating = {1} ".format(partitionname, ratingValue ) 
        cur.execute(query)
        rows2 = cur.fetchall()

        

        if os.path.exists(POINT_QUERY_OUTPUT_FILE):
            os.remove(POINT_QUERY_OUTPUT_FILE)

        with open(POINT_QUERY_OUTPUT_FILE,'a+') as f:
            for j in rows2:
                f.write("%s," % partitionname)
                f.write("%s," % str(j[0]))
                f.write("%s," % str(j[1]))
                f.write("%s\n" % str(j[2]))
        print("My program took", time.time() - start_time, "to run PointQuery on range partns")

        #
        # Point query for round robin partition
        start_time = time.time()
        print("started for point in rrobin partn")
        query = "select partitionnum from {0} ".format(ROUND_ROBIN_RATINGS_METADATA)	
        cur.execute(query)
        rrpartitioncount = int(cur.fetchone()[0])
        
        for i in range(rrpartitioncount):
            partitionname = ROUND_ROBIN_PARTITION_OUTPUT_NAME + repr(i)
            query = "select * from {0} where rating = {1} ".format(partitionname, ratingValue ) 
            cur.execute(query)
            rows2 = cur.fetchall()

            with open(POINT_QUERY_OUTPUT_FILE,'a+') as f:
                for j in rows2:
                    f.write("%s," % partitionname)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))
        print("My program took", time.time() - start_time, "to run PointQuery on rrobin partns")

    except Exception as ex:
        print("Exception while processing RangeQuery: ",ex)

def getopenconnection(user='postgres', password='1234', dbname='OriginalDatabase'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


if __name__ == '__main__':
	with getopenconnection() as con:
		# RangeQuery('rat',2,5,con)
		# start_time = time.time()
		# print("started for range")
		RangeQuery('ratings',2,5,con)
		# print("My program took", time.time() - start_time, "to run RangeQuery")


		# start_time = time.time()
		# print("started for point")
		PointQuery('ratings',4,con)
		# print("My program took", time.time() - start_time, "to run PointQuery")