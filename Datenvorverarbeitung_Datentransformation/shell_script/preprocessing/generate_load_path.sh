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
						days="$days$day,"
					fi
				done
				days=$days | sed 's/\(.*\),/\1 /'
				days="{$days}"
				paths="$paths/user/flume/keyword/tweets/$year/$month/$days,"
			fi
		done
	fi
done

paths=$paths | sed 's/\(.*\),/\1 /'
echo $paths > 'paths.txt'
