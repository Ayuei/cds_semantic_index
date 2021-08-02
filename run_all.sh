#!/bin/sh

for i in $(seq 0 6);
do 
	touch parsed_ids.txt$i
	python es_index.py $i > logs/$i.log 2>&1 &
done
