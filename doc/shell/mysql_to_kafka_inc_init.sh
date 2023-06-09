#!/bin/bash

# 该脚本的作用是初始化所有的增量表，只需执行一次

MAXWELL_HOME=/opt/module/maxwell

import_data() {
    $MAXWELL_HOME/bin/maxwell-bootstrap --database edu --table $1 --config $MAXWELL_HOME/config.properties
}

case $1 in
"cart_info")
    import_data cart_info
;;
"comment_info")
    import_data comment_info
;;
"favor_info")
    import_data favor_info
;;
"order_detail")
    import_data order_detail
;;
"order_info")
    import_data order_info
;;
"payment_info")
    import_data payment_info
;;
"review_info")
    import_data review_info
;;
"test_exam")
    import_data test_exam
;;
"test_exam_question")
    import_data test_exam_question
;;
"user_chapter_process")
    import_data user_chapter_process
;;
"user_info")
    import_data user_info
;;
"vip_change_detail")
    import_data vip_change_detail
;;
"all")
  import_data cart_info
  import_data comment_info
  import_data favor_info
  import_data order_detail
  import_data order_info
  import_data payment_info
  import_data review_info
  import_data test_exam
  import_data test_exam_question
  import_data user_chapter_process
  import_data user_info
  import_data vip_change_detail
;;
esac