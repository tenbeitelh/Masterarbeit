#!/bin/bash

now="$(date +'%d/%m/%Y')"

if [[ $# < 3 ]]; 
	then
		echo 'Less then two parameters are given. The inputDir and outputDir parameter are necessary'
		exit
	else
		inputDir=$1
		outputDir=$2

		seqdir="$outputDir-seq-$now"

		mahout seqdirectory -c UTF-8 -i $inputDir -o $seqdir

		vecdir="$outputDir-vec-$now"

		mahout seq2sparse -i $seqdir -o $vecdir -ow -a de.hs.osnabrueck.tenbeitel.mahout.analyzer.MahoutGermanAnalyzer

		rm -r seqdir
fi

