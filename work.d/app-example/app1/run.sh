#!/bin/bash

unalias -a
set -ex -o pipefail +o posix

st="$(date +'%Y-%m-%d %H:%M:%S')"

echo "arg=[$*]"
env
sleep $4
echo "wake up"

host="$(hostname -s)"
data="arg=[$*]"

et="$(date +'%Y-%m-%d %H:%M:%S')"

#
# drop table if exists app1_log;
# create table app1_log (id bigint unsigned not null auto_increment, starttime datetime not null, endtime datetime not null, host varchar(20) not null, client varchar(20) not null, worker_key varchar(20) not null, job varchar(64) null, data text, primary key(id));
#
sql="insert into app1_log(starttime, endtime, host, client, worker_key, job, data) values('${st}', '${et}', '${host}', '${BQ_CLIENT}', '${BQ_WORKER_KEY}', '${BQ_SESSION}', '${data}')"
echo "[[$sql]]"

MYSQL_PWD='batchq_pass' mysql -h 10.96.155.95 -u batchq_user -D batchq -e "${sql}"

exit 0
