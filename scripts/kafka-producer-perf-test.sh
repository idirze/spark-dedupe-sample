#!/bin/sh

SAMPLE_PAYLOAD_FILE="/root/A/B/C/sampleA.txt"
SAMPLE_PAYLOAD_DELIMETER="NEW__LINE"

sh generate_sample_payload.sh $SAMPLE_PAYLOAD_FILE  $SAMPLE_PAYLOAD_DELIMETER  100

sh /usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --producer-props \
                             bootstrap.servers=sandbox-hdp.hortonworks.com:6667  \
                             acks=1 \
                             batch.size=8196 \
                             buffer.memory=67108864 \
                            --num-records 2000 \
                            --topic input-perf6 \
                            --throughput 200  \
                            --payload-file $SAMPLE_PAYLOAD_FILE \
                            --payload-delimiter  $SAMPLE_PAYLOAD_DELIMETER

