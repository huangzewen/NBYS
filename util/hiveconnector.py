from impala.dbapi import connect
import pymysql

class HiveConnector(object):
    def __init__(self, host="127.0.0.1", port=10000):
        self._host = host
        self._port = port
        self._conn = None
        self._cursor = None

    def connect(self, db_name):
        self._conn = connect(host=self._host, port=self._port, database=db_name, auth_mechanism='PLAIN')
        self._cursor = self._conn.cursor()
    
    def test(self):
         self._cursor.execute("SHOW DATABASES")
         return self._cursor.fetchall()

    def execte(self, sql_str):
        self._cursor.execute(sql_str)
        return self._cursor.fetchall()

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
        if self._conn is not None:
            self._conn.close()

class MysqlConnector(object):
    def __init__(self, host, port, user, password, db_name): 
        self._con = pymysql.connect(host=host, port=port, user=user, password=password, db=db_name, 
                                   cursorclass=pymysql.cursors.DictCursor)
        self._cur = self._con.cursor()

    def insert(self, sql_str):
        self._cur.execute(sql_str)
        
    def update(self, sql_str):
        self._cur.execute(sql_str)

    def delete(self, sql_str):
        self._cur.execute(sql_str)

    def commit(self):
        self._con.commit()
        
    def close(self):
       if self._cur:
           self._cur.close()
       if self._con:
           self._cur.close()

if __name__ == "__main__":
    hive_conn = HiveConnector("192.168.2.131")
    hive_conn.connect("nbys")
    mysql_conn = MysqlConnector("192.168.2.1", 3306, "root", "123456", "nuopen")
    try:
        # 查询 Module, Unit, QuestionId, pen_name, count(distinct stroke_num)
        hql = "SELECT Module, Unit, QuestionId, pen_name, count(distinct stroke_num), min(id) as sid FROM test_point_yw " + \
                "GROUP BY Module, Unit, QuestionId, pen_name order by Module, Unit, QuestionId, pen_name, sid"
        result = hive_conn.execte(hql)
        # insert data queried from step1 into table
        for row in result:
           sql = "INSERT INTO `homework_question_duration` VALUES (NULL, NULL, NULL, '%s', '语文', '%s', '%s', '%s', NULL, NULL, %d, NULL, NULL)" %(row[3], row[0], row[1], row[2], row[4])
           print(sql)
           mysql_conn.insert(sql)
        mysql_conn.commit()

        # 计算每道题的笔划书写总时长(ms)：笔落在纸上的时间
        hql = "SELECT r.Module, r.Unit, r.questionId, r.pen_name, SUM(r.spend_time) FROM (SELECT Module, Unit, questionId, pen_name, (max(ts)-min(ts)) as spend_time FROM test_point_yw GROUP BY Module, Unit, questionId, pen_name, stroke_num ORDER BY Module, Unit, questionId, pen_name, stroke_num) as r GROUP BY r.Module, r.Unit, r.questionId, r.pen_name ORDER BY r.Module, r.Unit, r.questionId, r.pen_name"
        result = hive_conn.execte(hql)
        # insert data queried from step1 into table
        for row in result:
            sql = "UPDATE `homework_question_duration` SET `writing_duration` = %f WHERE module = '%s' AND unit = '%s' AND question_id='%s' AND pen_name = '%s'" %(row[4], row[0], row[1], row[2], row[3])
            mysql_conn.update(sql)
            mysql_conn.commit()

        # 计算每道题中，笔划停顿总时长(ms)：笔划与笔划之间停顿的总时长
        hql = "with r as (SELECT Module, Unit, questionId, pen_name, stroke_num, max(ts) as max_ts, min(ts) as min_ts FROM test_point_yw GROUP BY Module, Unit, questionId, pen_name, stroke_num ORDER BY Module, Unit, questionId, pen_name, stroke_num) SELECT Module, Unit, questionId, pen_name, SUM(if(pre_break_duration is NULL, 0, pre_break_duration)) FROM (SELECT r.Module as Module, r.Unit as Unit, r.questionId as questionId, r.pen_name as pen_name, (r.min_ts - (SELECT t.max_ts from r as t WHERE t.stroke_num = r.stroke_num-1 and t.Module = r.Module and t.Unit = r.Unit and t.questionId = r.questionId and t.pen_name = r.pen_name)) as pre_break_duration FROM r) s GROUP BY Module, Unit, questionId, pen_name"
        result = hive_conn.execte(hql)
        # insert data queried from step1 into table
        for row in result:
            sql = "UPDATE `homework_question_duration` SET `break_duration` = %f WHERE module = '%s' AND unit = '%s' AND question_id='%s' AND pen_name = '%s'" %(row[4], row[0], row[1], row[2], row[3])
            mysql_conn.update(sql)
            mysql_conn.commit()

        hql = "SELECT Module, Unit, questionId, pen_name, max(create_time) as end_time, min(create_time) as start_time FROM test_point_yw GROUP BY Module, Unit, questionId, pen_name ORDER BY Module, Unit, questionId, pen_name"
        result = hive_conn.execte(hql)
        for row in result:
            sql = "UPDATE `homework_question_duration` SET `end_time` = '%s', `start_time` = '%s' WHERE `module` = '%s' AND `unit` = '%s' AND `question_id`='%s' AND `pen_name` = '%s'" %(row[4], row[5], row[0], row[1], row[2], row[3])
            mysql_conn.update(sql)
            mysql_conn.commit()

    except Exception as e:
        print(e)
    finally:
       hive_conn.close()
       mysql_conn.close()
