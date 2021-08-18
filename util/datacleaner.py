from util.hiveconnector import MysqlConnector

class DataCleaner(object):
    def __init__(self, db_conn, table_name):
        self._db_conn = db_conn
        self._table_name = table_name

    def rmrow_empty_field(self, field_name, *empty_value):
        condition = "("
        for i in range(len(empty_value)):
            condition += "'%s', "
        condition += "NULL)"
        sql = "DELETE FROM %s WHERE `%s`  IN " + condition
        sql %= ((self._table_name, field_name) + empty_value)
        self._db_conn.delete(sql)

    # 替换非法的break_duration： 1）<0 2) > 10*60*1000
    def replace_break_duration(self):
        sql = "UPDATE `%s` SET break_duration = (1000*TIMESTAMPDIFF(SECOND, `start_time`, `end_time`) - `writing_duration` + 1000) WHERE  break_duration > 10*60*1000 OR break_duration < 0" %self._table_name
        self._db_conn.update(sql)

if __name__ == "__main__":
    mysql_conn = MysqlConnector("192.168.2.1", 3306, "root", "123456", "nbys_bi")
    cleaner = DataCleaner(mysql_conn, 'homework_question_duration_36')
    cleaner.replace_break_duration()
    # cleaner = DataCleaner(mysql_conn, 'homework_question_duration_36')
    # cleaner.rmrow_empty_field('module', 'None', 'none')
    mysql_conn.commit()

        