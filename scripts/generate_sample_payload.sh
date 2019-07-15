#!/bin/sh

if [ $# -ne 3 ]; then
    echo "Please, provide the path/to/the/sample/file.txt, the content delimeter and the number of messages"
    exit 1
fi



SAMPLE_FILE="$1"
mkdir -p "$(dirname -- "$1")"
rm -f $SAMPLE_FILE

DELIMETER="$2"
NB_MSG=$3
START=1

echo "Sample file name will be generated in: $SAMPLE_FILE, DELIMETER: $DELIMETER, NB MSG: $NB_MSG"

for i in $(eval echo "{$START..$NB_MSG}")
do
  wanted_size=$((900*1024))
  file_size=$(( ((wanted_size/12)+1)*12 ))
  read_size=$((file_size*3/4))

  echo "wanted=$wanted_size file=$file_size read=$read_size"

  dd if=/dev/urandom bs=$read_size count=1 | base64 > ${SAMPLE_FILE}_tmp

  truncate -s "$wanted_size" ${SAMPLE_FILE}_tmp

  cat ${SAMPLE_FILE}_tmp >> ${SAMPLE_FILE}_tmp_tmp
  echo -e "\n$DELIMETER" >> ${SAMPLE_FILE}_tmp_tmp
done

head -n -1  ${SAMPLE_FILE}_tmp_tmp > $SAMPLE_FILE

rm -f ${SAMPLE_FILE}_tmp_tmp
rm -f ${SAMPLE_FILE}_tmp

