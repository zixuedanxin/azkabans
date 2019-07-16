#!/bin/bash
# cat ${JOB_PROP_FILE} >> /root/azkaban.txt
echo $2
cat ${JOB_PROP_FILE}
# >> /home/kafka/$1.txt