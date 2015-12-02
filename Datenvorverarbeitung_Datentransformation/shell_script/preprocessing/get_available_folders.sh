#!/bin/bash
paths=""
for month in 01 02 03 04 05 06 07 08 09 10 11 12
do 
	hadoop fs -test -e /user/flume/keyword/tweets/2015/$month
	exists=$?
	if [[ $exists == 0 ]]; then
		for day in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31; 
		do
			hadoop fs -test -e /user/flume/keyword/tweets/2015/$month/$day
			exists=$?
			#echo $month" "$day
			if [[ $exists == 0 ]]; then
				paths="$paths/user/flume/keyword/tweets/2015/$month/$day,"
			fi
		done	
	fi
done

paths=$paths | sed 's/\(.*\),/\1 /'
echo $paths > 'paths.txt'
