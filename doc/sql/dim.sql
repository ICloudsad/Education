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


----- 课程维度表
drop table dim_course_full;
create external table dim_course_full(
    id string,
    course_name string,
    subject_id string,
    subject_name string,
    category_id string,
    category_name string,
    teacher string,
    chapter_num int,
    origin_price double,
    reduce_amount double,
    actual_price double,
    create_time string,
    update_time string,
    deleted string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_course_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");

---- 数据加载
-------------------------
with cou as (
    select id,
           course_name,
           subject_id,
           teacher,
           chapter_num,
           origin_price,
           reduce_amount,
           actual_price,
           create_time,
           update_time,
           deleted
    from ods_course_info_full where dt='2022-02-24'
), sub as (
    select id,
           subject_name,
           category_id
    from ods_base_subject_info_full where dt='2022-02-24'
), cat as (
    select id,
           category_name
    from ods_base_category_info_full where dt='2022-02-24'
)
insert overwrite table dim_course_full partition (dt='2022-02-24')
select
    cou.id,
    course_name,
    cou.subject_id,
    sub.subject_name,
    sub.category_id,
    cat.category_name,
    teacher,
    chapter_num,
    origin_price,
    reduce_amount,
    actual_price,
    create_time,
    update_time,
    deleted
from cou left join sub
on cou.subject_id = sub.id
left join cat
on sub.category_id = cat.id;



------ 省份维度表
create external table dim_province_full(
    id string,
    name string,
    region_id string,
    area_code string,
    iso_code string,
    iso_3166_2 string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_province_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


------ 数据加载
insert overwrite table dim_province_full partition (dt='2022-02-24')
select id,
       name,
       region_id,
       area_code,
       iso_code,
       iso_3166_2
from ods_base_province_full where dt='2022-02-24';


------- 章节维度表
drop table dim_chapter_full;
drop table dim_video_full;
drop table dim_paper_full;
create external table dim_chapter_full(
    id string,
    chapter_name string,
    is_free string,
    create_time string,
    update_time string,
    deleted string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_chapter_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


------- 数据加载
insert overwrite table dim_chapter_full partition (dt='2022-02-24')
select id,
       chapter_name,
       is_free,
       create_time,
       update_time,
       deleted
from ods_chapter_info_full where dt='2022-02-24';



------- 视频维度表
create external table dim_video_full(
    id string,
    video string,
    during_sec bigint,
    video_status string,
    video_size bigint,
    create_time string,
    update_time string,
    deleted string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_video_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


------- 数据加载
insert overwrite table dim_video_full partition (dt='2022-02-24')
select id,
       video_name,
       during_sec,
       video_status,
       video_size,
       create_time,
       update_time,
       deleted
from ods_video_info_full where dt='2022-02-24';


------ 试卷维度表
create external table dim_paper_full(
    id string,
    paper_title string,
    create_time string,
    update_time string,
    deleted string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_paper_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


------- 数据加载
insert overwrite table dim_paper_full partition (dt='2022-02-24')
select id,
       paper_title,
       create_time,
       update_time,
       deleted
from ods_test_paper_full where dt='2022-02-24';


------- 知识点维度表
create external table dim_knowledge_point_full(
    id string,
    point_txt string,
    point_level string,
    create_time string,
    update_time string,
    deleted string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_knowledge_point_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


------- 数据加载
insert overwrite table dim_knowledge_point_full partition (dt='2022-02-24')
select id,
       point_txt,
       point_level,
       create_time,
       update_time,
       deleted
from ods_knowledge_point_full where dt='2022-02-24';


-------- 问题维度表
create external table dim_question_full(
    id string,
    question_txt string,
    question_type string,
    question_option array<struct<id: string, option_txt: string, is_correct: string>>,
    create_time string,
    update_time string,
    deleted string
)
partitioned by (dt string)
stored as ORC
location '/warehouse/edu/dim_question_full'
TBLPROPERTIES ("orc.compress"="SNAPPY");


------ 数据加载
with ques as (
    select id,
           question_txt,
           question_type,
           create_time,
           update_time,
           deleted
    from ods_test_question_info_full where dt='2022-02-24'
), opt as (
    select id,
           option_txt,
           question_id,
           is_correct
    from ods_test_question_option_full where dt='2022-02-24'
)
insert overwrite table dim_question_full partition (dt='2022-02-24')
select
    ques.id,
    question_txt,
    question_type,
    collect_list(named_struct('id', opt.id, 'option_txt', opt.option_txt, 'is_correct', opt.is_correct)) question_option,
    create_time,
    update_time,
    deleted
from ques left join opt
on ques.id = opt.question_id
group by ques.id, question_txt, question_type, create_time, update_time, deleted;

