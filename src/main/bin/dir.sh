#!/bin/bash

time=`date -d yesterday +%Y%m%d`

echo "data time is "${time}

baseDir=/spark/can/

year=${time:0:4}

month=${time:4:2}

day=${time:6:2}


canDirMonth=${baseDir}${year}"/"${month}

hadoop fs -test -e ${canDirMonth}

if [ $? -ne 0 ] ; then
  hadoop fs -mkdir ${canDirMonth}
fi

canDir=${baseDir}${year}"/"${month}"/"${day}

echo "canDir dir is "${canDir}

hadoop fs -test -e ${canDir}

if [ $? -ne 0 ] ; then
  hadoop fs -mkdir ${canDir}
fi

alarmDir=/spark/alarmStatus/day/${year}"/"${month}"/"${time}

hadoop fs -test -e ${alarmDir}
if [ $? -ne 0 ] ; then
  hadoop fs -mkdir ${alarmDir}
fi


