#! /bin/bash

DATAX_HOME=/opt/module/datax

if [ $# -lt 1 ]
then
  echo "参数必须大于0"
  exit
fi

[ "$2" ] && do_date=$2 || do_date=$(date -d '-1 day' +%F)

function handle_targetdir() {
    hadoop fs -test -e $1
    if [ $? -eq 1 ]
    then
      echo "路径$1不存在, 正在创建..."
      hadoop fs -mkdir -p $1
    else
      echo "路径$1已存在"
      fs_count=$(hadoop fs -count $1)
      content_size=$(echo $fs_count | awk '{print $3}')
      if [ $content_size -eq 0 ]
      then
        echo "路径$1为空"
      else
        echo "路径$1不为空, 正在清空"
        hadoop fs -rm -r -f $1/*
      fi
    fi
}

function import_data() {
    datax_config=$1
    target_dir=$2
    handle_targetdir $target_dir
    python $DATAX_HOME/bin/datax.py -p"-Dtargetdir=$target_dir" $datax_config
}

case $1 in
"all")
import_data /opt/module/datax/job/import/edu.base_category_info.json /origin_data/edu/db/base_category_info_full/$do_date
import_data /opt/module/datax/job/import/edu.base_province.json /origin_data/edu/db/base_province_full/$do_date
import_data /opt/module/datax/job/import/edu.base_source.json /origin_data/edu/db/base_source_full/$do_date
import_data /opt/module/datax/job/import/edu.base_subject_info.json /origin_data/edu/db/base_subject_info_full/$do_date
import_data /opt/module/datax/job/import/edu.chapter_info.json /origin_data/edu/db/chapter_info_full/$do_date
import_data /opt/module/datax/job/import/edu.course_info.json /origin_data/edu/db/course_info_full/$do_date
import_data /opt/module/datax/job/import/edu.knowledge_point.json /origin_data/edu/db/knowledge_point_full/$do_date
import_data /opt/module/datax/job/import/edu.test_paper.json /origin_data/edu/db/test_paper_full/$do_date
import_data /opt/module/datax/job/import/edu.test_paper_question.json /origin_data/edu/db/test_paper_question_full/$do_date
import_data /opt/module/datax/job/import/edu.test_point_question.json /origin_data/edu/db/test_point_question_full/$do_date
import_data /opt/module/datax/job/import/edu.test_question_info.json /origin_data/edu/db/test_question_info_full/$do_date
import_data /opt/module/datax/job/import/edu.test_question_option.json /origin_data/edu/db/test_question_option_full/$do_date
import_data /opt/module/datax/job/import/edu.video_info.json /origin_data/edu/db/video_info_full/$do_date
  ;;
"base_category_info")
import_data /opt/module/datax/job/import/edu.base_category_info.json /origin_data/edu/db/base_category_info_full/$do_date
;;
"base_province")
import_data /opt/module/datax/job/import/edu.base_province.json /origin_data/edu/db/base_province_full/$do_date
;;
"base_source")
import_data /opt/module/datax/job/import/edu.base_source.json /origin_data/edu/db/base_source_full/$do_date
;;
"base_subject_info")
import_data /opt/module/datax/job/import/edu.base_subject_info.json /origin_data/edu/db/base_subject_info_full/$do_date
;;
"chapter_info")
import_data /opt/module/datax/job/import/edu.chapter_info.json /origin_data/edu/db/chapter_info_full/$do_date
;;
"course_info")
import_data /opt/module/datax/job/import/edu.course_info.json /origin_data/edu/db/course_info_full/$do_date
;;
"knowledge_point")
import_data /opt/module/datax/job/import/edu.knowledge_point.json /origin_data/edu/db/knowledge_point_full/$do_date
;;
"test_paper")
import_data /opt/module/datax/job/import/edu.test_paper.json /origin_data/edu/db/test_paper_full/$do_date
;;
"test_paper_question")
import_data /opt/module/datax/job/import/edu.test_paper_question.json /origin_data/edu/db/test_paper_question_full/$do_date
;;
"test_point_question")
import_data /opt/module/datax/job/import/edu.test_point_question.json /origin_data/edu/db/test_point_question_full/$do_date
;;
"test_question_info")
import_data /opt/module/datax/job/import/edu.test_question_info.json /origin_data/edu/db/test_question_info_full/$do_date
;;
"test_question_option")
import_data /opt/module/datax/job/import/edu.test_question_option.json /origin_data/edu/db/test_question_option_full/$do_date
;;
"video_info")
import_data /opt/module/datax/job/import/edu.video_info.json /origin_data/edu/db/video_info_full/$do_date
;;
esac
