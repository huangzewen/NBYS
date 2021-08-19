from util.hiveconnector import MysqlConnector

class DataCleaner(object):
    def __init__(self, db_conn):
        self._db_conn = db_conn
        # self._table_name = table_name

    def rmrow_empty_field(self, table_name, field_name, *empty_value):
        condition = "("
        for i in range(len(empty_value)):
            condition += "'%s', "
        condition += "NULL)"
        sql = "DELETE FROM %s WHERE `%s`  IN " + condition
        sql %= ((table_name, field_name) + empty_value)
        self._db_conn.delete(sql)
        self._db_conn.commit()

    # 替换非法的break_duration： 1）<0 2) > 10*60*1000
    def replace_break_duration(self, table_name):
        sql = "UPDATE `%s` SET break_duration = (1000*TIMESTAMPDIFF(SECOND, `start_time`, `end_time`) - `writing_duration` + 1000) WHERE  break_duration > 10*60*1000 OR break_duration < 0" %table_name
        self._db_conn.update(sql)
        self._db_conn.commit()

    def rmrow_homeworktime_less_0(self, table_name):
        sql = "DELETE FROM %s WHERE TIMESTAMPDIFF(SECOND, `start_time`, `end_time`) < 0" %table_name
        self._db_conn.delete(sql)
        self._db_conn.commit()

if __name__ == "__main__":
    mysql_conn = MysqlConnector("192.168.2.1", 3306, "root", "123456", "nbys_bi")
    grade_no = 3
    class_no = 8
    question_duration_table_name =  'homework_question_duration_%d%d' %(grade_no, class_no)
    stroke_duration_table_name =  'homework_stroke_duration_%d%d' %(grade_no, class_no)
    # question_duration_table_name = 'homework_question_duration_sum_3'
    # stroke_duration_table_name = 'homework_stroke_duration_sum_3'
    cleaner = DataCleaner(mysql_conn)

    # break_duration < 0 或 异常大（>10*60*1000）
    cleaner.replace_break_duration(question_duration_table_name)

    # 删除module为空的行
    cleaner.rmrow_empty_field(question_duration_table_name, 'module', 'None', 'none')
    cleaner.rmrow_empty_field(stroke_duration_table_name, 'module', 'None', 'none')

    mysql_conn.close()

        