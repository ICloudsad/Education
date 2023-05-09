#! /bin/bash

if [ $# -lt 1 ]
then
  echo "参数个数必须大于1"
  exit
fi

[ "$2" ] && dateStr=$2 || dateStr=$(date -d '-1 day' +%F)

function import_data_db() {
  table_name=$1
  /opt/module/hive/bin/hive -e "use edu;load data inpath '/origin_data/edu/db/${table_name}/${dateStr}' overwrite into table ods_${table_name} partition (dt='${dateStr}')";
}

function import_data_log() {
  table_name=$1
  /opt/module/hive/bin/hive -e "use edu;load data inpath '/origin_data/edu/log/${table_name}/${dateStr}' overwrite into table ods_log_inc partition (dt='${dateStr}')";
}


case $1 in
"all")
  import_data_db base_category_info_full
  import_data_db base_province_full
  import_data_db base_source_full
  import_data_db base_subject_info_full
  import_data_db cart_info_inc
  import_data_db chapter_info_full
  import_data_db comment_info_inc
  import_data_db course_info_full
  import_data_db favor_info_inc
  import_data_db knowledge_point_full
  import_data_log edu_log
  import_data_db order_detail_inc
  import_data_db order_info_inc
  import_data_db payment_info_inc
  import_data_db review_info_inc
  import_data_db test_exam_inc
  import_data_db test_exam_question_inc
  import_data_db test_paper_full
  import_data_db test_paper_question_full
  import_data_db test_point_question_full
  import_data_db test_question_info_full
  import_data_db test_question_option_full
  import_data_db user_chapter_process_inc
  import_data_db user_info_inc
  import_data_db video_info_full
  import_data_db vip_change_detail_inc
  ;;
"base_category_info_full")
    import_data_db base_category_info_full
    ;;
"base_province_full")
    import_data_db base_province_full
    ;;
"base_source_full")
    import_data_db base_source_full
    ;;
"base_subject_info_full")
    import_data_db base_subject_info_full
    ;;
"cart_info_inc")
    import_data_db cart_info_inc
    ;;
"chapter_info_full")
    import_data_db chapter_info_full
    ;;
"comment_info_inc")
    import_data_db comment_info_inc
    ;;
"course_info_full")
    import_data_db course_info_full
    ;;
"favor_info_inc")
    import_data_db favor_info_inc
    ;;
"knowledge_point_full")
    import_data_db knowledge_point_full
    ;;
"log_inc")
    import_data_log log_inc
    ;;
"order_detail_inc")
    import_data_db order_detail_inc
    ;;
"order_info_inc")
    import_data_db order_info_inc
    ;;
"payment_info_inc")
    import_data_db payment_info_inc
    ;;
"review_info_inc")
    import_data_db review_info_inc
    ;;
"test_exam_inc")
    import_data_db test_exam_inc
    ;;
"test_exam_question_inc")
    import_data_db test_exam_question_inc
    ;;
"test_paper_full")
    import_data_db test_paper_full
    ;;
"test_paper_question_full")
    import_data_db test_paper_question_full
    ;;
"test_point_question_full")
    import_data_db test_point_question_full
    ;;
"test_question_info_full")
    import_data_db test_question_info_full
    ;;
"test_question_option_full")
    import_data_db test_question_option_ful
    ;;
"user_chapter_process_inc")
    import_data_db user_chapter_process_inc
    ;;
"user_info_inc")
    import_data_db user_info_inc
    ;;
"video_info_full")
    import_data_db video_info_full
    ;;
"vip_change_detail_inc")
    import_data_db vip_change_detail_inc
    ;;
*)
  echo "必须传入表名/all"
  exit
  ;;
esac

