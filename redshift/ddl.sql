create external schema spectrum
from data catalog database 'task3' region 'ap-northeast-1'
iam_role 'arn:aws:iam::026845558380:role/redshift-role'
create external database if not exists;

--  一時保管用
create external table spectrum.location_raw(
user_id char(36),
latitude double precision,
longitude double precision,
created_at integer
)
row format delimited fields terminated by ','
stored as textfile
location 's3://me32as8cme32as8c-task3-rawdata/unstored/';

