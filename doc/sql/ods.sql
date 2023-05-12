drop database edu cascade ;
------用户日志表
create database edu;
drop table if exists ods_log_inc;
create external table if not exists ods_log_inc(
    actions array<struct<action_id: string, item: string, item_type: string, ts: string>>,
    common struct<ar: string, ba: string, ch: string, is_new: string, md: string, mid: string, os: string, sc: string, sid: string, uid: string, vc: string>,
    displays array<struct<display_type: string, item: string, item_type: string, `order`: int, pos_id: int>>,
    page struct<during_time: bigint, item: string, item_type: string, last_page_id: string, page_id: string>,
    err struct<error_code: bigint, msg: string>,
    `start` struct<entry: string, first_open: int, loading_time: bigint, open_ad_id: string, open_ad_ms: bigint, open_ad_skip_ms: bigint>,
    appVideo struct<play_sec: bigint, postition_sec: bigint, video_id: string>,
    ts bigint
)comment '用户日志表'
partitioned by (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
stored as TEXTFILE
location '/warehouse/edu/ods_log_inc'
TBLPROPERTIES ("orc.compress"="GZIP");

load data inpath '/origin_data/edu/log/edu_log/2022-02-24' overwrite into table ods_log_inc partition(dt='2022-02-24');
load data inpath '/origin_data/edu/log/edu_log/2022-02-25' overwrite into table ods_log_inc partition(dt='2022-02-25');

msck repair table ods_log_inc;
----- 分类表
create external table if not exists ods_base_category_info_full(
    id string,
    category_name string,
    create_time string,
    update_time string,
    deleted string
)comment '分类表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as TEXTFILE
location '/warehouse/edu/ods_base_category_info_full'
TBLPROPERTIES ("orc.compress"="GZIP");

msck repair table ods_base_category_info_full;


load data inpath '/origin_data/edu/db/base_category_info_full/2022-02-24' overwrite into table ods_base_category_info_full partition (dt='2022-02-24');
load data inpath '/origin_data/edu/db/base_category_info_full/2022-02-25' overwrite into table ods_base_category_info_full partition (dt='2022-02-25');


----- 省份表
create external table if not exists ods_base_province_full(
    id string,
    name string,
    region_id string,
    area_code string,
    iso_code string,
    iso_3166_2 string
) comment '省份表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as TEXTFILE
location '/warehouse/edu/ods_base_province_full'
TBLPROPERTIES ("orc.compress"="GZIP");

msck repair table ods_base_province_full;

load data inpath '/origin_data/edu/db/base_province_full/2022-02-24' overwrite into table ods_base_province_full partition (dt='2022-02-24');
load data inpath '/origin_data/edu/db/base_province_full/2022-02-25' overwrite into table ods_base_province_full partition (dt='2022-02-25');


----- 来源表
create external table if not exists ods_base_source_full(
    id string,
    source_site string,
    source_url string
)comment '来源表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as TEXTFILE
location '/warehouse/edu/ods_base_source_full'
TBLPROPERTIES ("orc.compress"="GZIP");

msck repair table ods_base_source_full;

load data inpath '/origin_data/edu/db/base_source_full/2022-02-24' overwrite into table ods_base_source_full partition (dt='2022-02-24');
load data inpath '/origin_data/edu/db/base_source_full/2022-02-25' overwrite into table ods_base_source_full partition (dt='2022-02-25');


------ 科目表
drop table if exists ods_base_subject_info_full;
create external table if not exists ods_base_subject_info_full(
    id string,
    subject_name string,
    category_id string,
    create_time string,
    update_time string,
    deleted string
)comment '科目表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as TEXTFILE
location '/warehouse/edu/ods_base_subject_info_full'
TBLPROPERTIES ("orc.compress"="GZIP");

msck repair table ods_base_subject_info_full;

load data inpath '/origin_data/edu/db/base_subject_info_full/2022-02-24' overwrite into table ods_base_subject_info_full partition (dt='2022-02-24');
load data inpath '/origin_data/edu/db/base_subject_info_full/2022-02-25' overwrite into table ods_base_subject_info_full partition (dt='2022-02-25');

----- 加购表
drop table if exists ods_cart_info_inc;
create external table if not exists ods_cart_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, user_id: string, course_id: string, course_name: string, cart_price: double, img_url: string, session_id: string, create_time: string, update_time: string, deleted: string, sold: string>,
    old map<string, string>
)comment '加购表'
partitioned by (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
stored as TEXTFILE
location '/warehouse/edu/ods_cart_info_inc'
TBLPROPERTIES ("orc.compress"="GZIP");


----- 章节表
create external table if not exists ods_chapter_info_full(
    id string,
    chapter_name string,
    course_id string,
    video_id string,
    publisher_id string,
    is_free string,
    create_time string,
    update_time string,
    deleted string
) comment '章节表'
partitioned by (dt string)
row format delimited fields terminated by '\t'
stored as TEXTFILE
location '/warehouse/edu/ods_chapter_info_full'
TBLPROPERTIES ("orc.compress"="GZIP");


------ 章节评论表
create external table if not exists ods_comment_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, user_id: string, chapter_id: string, course_id: string, comment_txt: string, create_time: string, deleted:string>,
    old map<string, string>
)comment '章节评论表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_comment_info_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


----- 课程信息表
create external table if not exists ods_course_info_full(
    id string,
    course_name string,
    course_slogan string,
    course_cover_url string,
    subject_id string,
    teacher string,
    publisher_id string,
    chapter_num int,
    origin_price double,
    reduce_amount double,
    actual_price double,
    course_introduce string,
    create_time string,
    update_time string,
    deleted string
)comment '课程信息表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_course_info_full'
    TBLPROPERTIES ("orc.compress"="GZIP");



----- 收藏表
create external table if not exists ods_favor_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, course_id: string, create_time: string,update_time: string, deleted:string>,
    old map<string, string>
)comment '收藏表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_favor_info_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


----- 知识点表
create external table if not exists ods_knowledge_point_full(
    id string,
    point_txt string,
    point_level string,
    course_id string,
    chapter_id string,
    publisher_id string,
    create_time string,
    update_time string,
    deleted string
)comment '知识点表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_knowledge_point_full'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 订单明细表
create external table if not exists ods_order_detail_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, course_id: string, course_name: string, order_id: string, user_id: string, origin_amount: double, coupon_reduce: double, final_amount: double, session_id: string, create_time: string, update_time: string>,
    old map<string, string>
)comment '订单明细表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_order_detail_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


----- 订单表
create external table if not exists ods_order_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, user_id: string, origin_amount: double, coupon_reduce: double, final_amount: double, order_status: string, out_trade_no: string, trade_body: string, session_id: string, province_id: string, create_time: string, expire_time: string, update_time: string>,
    old map<string, string>
)comment '订单表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_order_info_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 支付表
create external table if not exists ods_payment_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, out_trade_no: string, order_id: string, alipay_trade_no: string, total_amount: double, trade_body: string, payment_type: string, payment_status: string, create_time: string, update_time: string, callback_content: string, callback_time: string>,
    old map<string, string>
)comment '支付表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_payment_info_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 课程评价表
create external table if not exists ods_review_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, user_id: string, course_id: string, review_txt: string, review_stars: string, create_time: string, deleted: string>,
    old map<string, string>
)comment '课程评价表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_review_info_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 测验表
create external table if not exists ods_test_exam_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, paper_id: string, user_id: string, score: double, duration_sec: int, create_time: string, submit_time: string, update_time: string, deleted: string>,
    old map<string, string>
)comment '测验表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_exam_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


------- 测验问题表
create external table if not exists ods_test_exam_question_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, exam_id: string, papaer_id: string, question_id: string, user_id: string, answer: string, is_correct: string, score: double, create_time: string, update_time: string, deleted: string>,
    old map<string, string>
)comment '测验问题表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_exam_question_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


----- 试卷表
drop table ods_test_paper_full;
create external table if not exists ods_test_paper_full(
    id string,
    paper_title string,
    course_id string,
    create_time string,
    update_time string,
    publisher_id string,
    deleted string
)comment '试卷表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_paper_full'
    TBLPROPERTIES ("orc.compress"="GZIP");

msck repair table ods_test_paper_full;


------ 试卷问题表
create external table if not exists ods_test_paper_question_full(
    id string,
    paper_id string,
    question_id string,
    score double,
    create_time string,
    deleted string,
    publisher_id string
)comment '试卷问题表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_paper_question_full'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 知识点问题表
create external table if not exists ods_test_point_question_full(
    id string,
    point_id string,
    question_id string,
    create_time string,
    publish_id string,
    deleted string
)comment '知识点问题表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_point_question_full'
    TBLPROPERTIES ("orc.compress"="GZIP");


------- 问题信息表
create external table if not exists ods_test_question_info_full(
    id string,
    question_txt string,
    chapter_id string,
    course_id string,
    question_type string,
    create_time string,
    update_time string,
    publisher_id string,
    deleted string
)comment '问题信息表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_question_info_full'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 问题选项表
create external table if not exists ods_test_question_option_full(
    id string,
    option_txt string,
    question_id string,
    is_correct string,
    create_time string,
    update_time string,
    deleted string
)comment '问题选项表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_test_question_option_full'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 用户章节进度表
create external table if not exists ods_user_chapter_process_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id: string, course_id: string, chapter_id: string, user_id: string, position_sec: int, create_time: string, update_time: string, deleted: string>,
    old map<string, string>
)comment '测验问题表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_user_chapter_process_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


------- 用户表
create external table if not exists ods_user_info_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id:string, login_name:string, nick_name:string, passwd:string, real_name:string, phone_num:string, email:string, head_img:string, user_level:string, birthday:string, gender:string, create_time:string, operate_time:string, status:string>,
    old map<string, string>
)comment '用户表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_user_info_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ 视频表
create external table if not exists ods_video_info_full(
    id string,
    video_name string,
    during_sec int,
    video_status string,
    video_size bigint,
    video_url string,
    video_source_id string,
    version_id string,
    chapter_id string,
    course_id string,
    publisher_id string,
    create_time string,
    update_time string,
    deleted string
)comment '视频表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as TEXTFILE
    location '/warehouse/edu/ods_video_info_full'
    TBLPROPERTIES ("orc.compress"="GZIP");


------ VIP变化表
create external table if not exists ods_vip_change_detail_inc(
    `database` string,
    `table` string,
    type string,
    ts bigint,
    xid string,
    xoffset string,
    `commit` string,
    data struct<id:string, user_id:string, from_vip:string, to_vip:string, create_time:string>,
    old map<string, string>
)comment 'VIP变化表'
    partitioned by (dt string)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    stored as TEXTFILE
    location '/warehouse/edu/ods_vip_change_detail_inc'
    TBLPROPERTIES ("orc.compress"="GZIP");


show tables like 'ods*';
