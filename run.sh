#!/bin/sh

fp=$1
parsed_fp="parsed_ids_"$fp

touch $parsed_fp
python es_index.py $fp  > logs/$fp.log 2>&1 &