----- 来源表
create external table dim_base_source_full(
    id string,
    source_site string,
    source_url string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_base_source_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_base_source_full partition (dt)
select
    id,
    source_site,
    source_url,
    dt
from ods_base_source_full where dt='2022-02-24';

----- 用户表(拉链表)
drop table dim_user_info_zip;
create external table dim_user_info_zip(
    id string,
    login_name string,
    nick_name string,
    passwd string,
    real_name string,
    phone_num string,
    email string,
    user_level string,
    birthday string,
    gender string,
    create_time string,
    operate_time string,
    start_date string,
    end_date string,
    `status` string
)
    partitioned by (dt string)
    stored as ORC
    location '/warehouse/edu/dim_user_info_zip'
    TBLPROPERTIES ("orc.compress"="SNAPPY");


----- 首次加载
-----------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_info_zip partition (dt)
select
    data.id,
    data.login_name,
    data.nick_name,
    data.passwd,
    data.real_name,
    data.phone_num,
    data.email,
    data.user_level,
    data.birthday,
    data.gender,
    data.create_time,
    data.operate_time,
    date_format(nvl(data.operate_time, data.create_time), 'yyyy-MM-dd') start_date,
    '9999-99-99' end_date,
    data.status,
    '9999-99-99' dt
from ods_user_info_inc where dt='2022-02-24' and type='bootstrap-insert';

--------每次加载
-----------------------------
set hive.exec.dynamic.partition.mode=nonstrict;
with old as (
    select id,
           login_name,
           nick_name,
           passwd,
           real_name,
           phone_num,
           email,
           user_level,
           birthday,
           gender,
           create_time,
           operate_time,
           start_date,
           end_date,
           status
    from dim_user_info_zip where dt='9999-99-99'
), new as (
    select
        data.id,
        data.login_name,
        data.nick_name,
        data.passwd,
        data.real_name,
        data.phone_num,
        data.email,
        data.user_level,
        data.birthday,
        data.gender,
        data.create_time,
        data.operate_time,
        '2022-02-25' start_date,
        '9999-99-99' end_date,
        data.status
    from ods_user_info_inc where dt='2022-02-25' and type='update' and data.operate_time is not null
    union all
    select
        data.id,
        data.login_name,
        data.nick_name,
        data.passwd,
        data.real_name,
        data.phone_num,
        data.email,
        data.user_level,
        data.birthday,
        data.gender,
        data.create_time,
        data.operate_time,
        '2022-02-25' start_date,
        '9999-99-99' end_date,
        data.status
    from ods_user_info_inc where dt='2022-02-25' and type='insert'
)
insert overwrite table dim_user_info_zip partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       start_date,
       if(rn=1, '9999-99-99', date_sub('2022-02-25',1)) end_date,
       status,
       if(rn=1, '9999-99-99', date_sub('2022-02-25',1)) dt
from (
    select id,
           login_name,
           nick_name,
           passwd,
           real_name,
           phone_num,
           email,
           user_level,
           birthday,
           gender,
           create_time,
           operate_time,
           start_date,
           end_date,
           status,
           row_number() over (partition by id order by start_date desc ,operate_time desc ) rn
    from (
        select id,
                 login_name,
                 nick_name,
                 passwd,
                 real_name,
                 phone_num,
                 email,
                 user_level,
                 birthday,
                 gender,
                 create_time,
                 operate_time,
                 start_date,
                 end_date,
                 status
          from old
          union all
          select id,
                 login_name,
                 nick_name,
                 passwd,
                 real_name,
                 phone_num,
                 email,
                 user_level,
                 birthday,
                 gender,
                 create_time,
                 operate_time,
                 start_date,
                 end_date,
                 status
              from new
        ) t
    ) t2;






