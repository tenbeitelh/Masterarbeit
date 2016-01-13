#!/bin/bash

paths=""

for year in 2015 2016
do  
	echo "Processing year: $year"
	hadoop fs -test -e /user/flume/keyword/tweets/$year/
	year_exists=$?
	if [[ $year_exists == 0 ]]; then
		for month in 01 02 03 04 05 06 07 08 09 10 11 12
		do 
			days=""
			echo "Processing month: $month"
			hadoop fs -test -e /user/flume/keyword/tweets/$year/$month
			month_exists=$?
			if [[ $month_exists == 0 ]]; then
				for day in 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31; 
				do
					echo "Processing day: $day"
					hadoop fs -test -e /user/flume/keyword/tweets/$year/$month/$day
					day_exists=$?
					#echo $month" "$day
					if [[ $day_exists == 0 ]]; then
						paths="$paths/user/flume/keyword/tweets/2015/$month/$day,"
					fi
				done
			fi
		done
	fi
done

rm -f paths.txt
touch paths.txt
echo ${paths::-1} > paths.txt
