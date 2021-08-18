from util.hiveconnector import HiveConnector, MysqlConnector;

def inser_homework_question_duration(hive_conn, mysql_conn, grade_no, class_no):
     # 查询 Module, Unit, QuestionId, pen_name, count(distinct stroke_num)
    hql = "SELECT Module, Unit, QuestionId, pen_name, count(distinct stroke_num), min(id) as sid FROM test_point_yw_%d%d GROUP BY Module, Unit, QuestionId, pen_name order by Module, Unit, QuestionId, pen_name, sid" %(grade_no, class_no)
    result = hive_conn.execte(hql)
    # insert data queried from step1 into table
    for row in result:
        sql = "INSERT INTO `homework_question_duration_%d%d` VALUES (NULL, NULL, NULL, '%s', '语文', '%s', '%s', '%s', NULL, NULL, %d, NULL, NULL, NULL)" %(grade_no, class_no, row[3], row[0], row[1], row[2], row[4])
        mysql_conn.insert(sql)
    mysql_conn.commit()

def update_homework_question_duration_writinrg_duration(hive_conn, mysql_conn, grade_no, class_no):
    # 计算每道题的笔划书写总时长(ms)：笔落在纸上的时间
    hql = "SELECT r.Module, r.Unit, r.questionId, r.pen_name, SUM(r.spend_time) FROM (SELECT Module, Unit, questionId, pen_name, (max(ts)-min(ts)) as spend_time FROM test_point_yw_%d%d GROUP BY Module, Unit, questionId, pen_name, stroke_num ORDER BY Module, Unit, questionId, pen_name, stroke_num) as r GROUP BY r.Module, r.Unit, r.questionId, r.pen_name ORDER BY r.Module, r.Unit, r.questionId, r.pen_name" %(grade_no, class_no)
    result = hive_conn.execte(hql)
    # insert data queried from step1 into table
    for row in result:
        sql = "UPDATE `homework_question_duration_%d%d` SET `writing_duration` = %f WHERE module = '%s' AND unit = '%s' AND question_id='%s' AND pen_name = '%s'" %(grade_no, class_no, row[4], row[0], row[1], row[2], row[3])
        mysql_conn.update(sql)
        mysql_conn.commit()

def update_homework_question_duration_break_duration(hive_conn, mysql_conn, grade_no, class_no):
     # 计算每道题中，笔划停顿总时长(ms)：笔划与笔划之间停顿的总时长
    hql = "with r as (SELECT Module, Unit, questionId, pen_name, stroke_num, max(ts) as max_ts, min(ts) as min_ts FROM test_point_yw_%d%d GROUP BY Module, Unit, questionId, pen_name, stroke_num ORDER BY Module, Unit, questionId, pen_name, stroke_num) SELECT Module, Unit, questionId, pen_name, SUM(if(pre_break_duration is NULL, 0, pre_break_duration)) FROM (SELECT r.Module as Module, r.Unit as Unit, r.questionId as questionId, r.pen_name as pen_name, (r.min_ts - (SELECT t.max_ts from r as t WHERE t.stroke_num = r.stroke_num-1 and t.Module = r.Module and t.Unit = r.Unit and t.questionId = r.questionId and t.pen_name = r.pen_name)) as pre_break_duration FROM r) s GROUP BY Module, Unit, questionId, pen_name" %(grade_no, class_no)
    result = hive_conn.execte(hql)
    # insert data queried from step1 into table
    for row in result:
        sql = "UPDATE `homework_question_duration_%d%d` SET `break_duration` = %f WHERE module = '%s' AND unit = '%s' AND question_id='%s' AND pen_name = '%s'" %(grade_no, class_no, row[4], row[0], row[1], row[2], row[3])
        mysql_conn.update(sql)
        mysql_conn.commit()

def update_homework_question_duration_start_end_time(hive_conn, mysql_conn, grade_no, class_no):
    hql = "SELECT Module, Unit, questionId, pen_name, max(create_time) as end_time, min(create_time) as start_time FROM test_point_yw_%d%d GROUP BY Module, Unit, questionId, pen_name ORDER BY Module, Unit, questionId, pen_name" %(grade_no, class_no)
    result = hive_conn.execte(hql)
    for row in result:
        sql = "UPDATE `homework_question_duration_%d%d` SET `end_time` = '%s', `start_time` = '%s' WHERE `module` = '%s' AND `unit` = '%s' AND `question_id`='%s' AND `pen_name` = '%s'" %(grade_no, class_no, row[4], row[5], row[0], row[1], row[2], row[3])
        mysql_conn.update(sql)
        mysql_conn.commit()

def insert_homework_stroke_duration(hive_conn, mysql_conn, grade_no, class_no):
    hql = "SELECT Module, Unit, questionId, pen_name, stroke_num, min(ts), max(ts), max(ts)-min(ts),  min(point_psr), max(point_psr), avg(point_psr), stddev(point_psr) FROM test_point_yw_%d%d GROUP BY Module, Unit, questionId, pen_name, stroke_num" %(grade_no, class_no)
    result = hive_conn.execte(hql)
    for row in result:
        sql = "INSERT INTO `homework_stroke_duration_%d%d` VALUES (NULL, '%s',  '%s', '%s', '%s', '语文', NULL, NULL, %d, %d, %d, %d, %f, %f, %f, %f, NULL)" %(grade_no, class_no, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11])
        mysql_conn.insert(sql)
    mysql_conn.commit()


if __name__ == "__main__":
    hive_conn = HiveConnector("192.168.2.131")
    hive_conn.connect("nbys")
    mysql_conn = MysqlConnector("192.168.2.1", 3306, "root", "123456", "nbys_bi")
    grade_no = 3
    class_no = 6
    try:
        print("homework_question_duration表初始化...")
        inser_homework_question_duration(hive_conn, mysql_conn, grade_no, class_no)
        print("插入homework_question_duration表成功")
        update_homework_question_duration_writinrg_duration(hive_conn, mysql_conn, grade_no, class_no)
        print("homework_question_duration表 writinrg_duration 更新成功")
        update_homework_question_duration_break_duration(hive_conn, mysql_conn, grade_no, class_no)
        print("homework_question_duration表 break_duration 更新成功")
        update_homework_question_duration_start_end_time(hive_conn, mysql_conn, grade_no, class_no)
        print("homework_question_duration表 start/end time 更新成功")
        insert_homework_stroke_duration(hive_conn, mysql_conn, grade_no, class_no)
        print("插入homework_stroke_duration表成功")
        # hql = "SELECT Module, Unit, questionId, pen_name, stroke_num, min(ts), max(ts), max(ts)-min(ts),  min(point_psr), max(point_psr), avg(point_psr), stddev(point_psr) FROM test_point_yw GROUP BY Module, Unit, questionId, pen_name, stroke_num"
        # result = hive_conn.execte(hql)
        # print(len(result))
        # for row in result:
        #    sql = "INSERT INTO homework_stroke_duration VALUES (NULL, '%s',  '%s', '%s', '%s', '语文', NULL, NULL, %d, %d, %d, %d, %f, %f, %f, %f, NULL)" %(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11])
        #    mysql_conn.update(sql)
        #    mysql_conn.commit()
    except Exception as e:
        print(e)
    finally:
        hive_conn.close()
        mysql_conn.close()