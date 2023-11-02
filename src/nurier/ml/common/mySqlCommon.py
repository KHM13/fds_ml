import pymysql
from src.nurier.ml.common.CommonProperties import CommonProperties as prop


class mysqlCommon:
    __result = []

    def excute_query(self, mlmodel_id: str):
        conn = pymysql.connect(host=prop().get_MYSQL_host(),
                               port=int(prop().get_MYSQL_port()),
                               user=prop().get_MYSQL_user(),
                               password=prop().get_MYSQL_pass(),
                               database=prop().get_MYSQL_database(),
                               charset='utf8')
        cursor = conn.cursor()
        sql = "SELECT * FROM preprocess_process where mlmodel_id =" + mlmodel_id
        cursor.execute(sql)
        result = cursor.fetchall()
        for data in result:
            self.__result.append(data)
        conn.commit()
        conn.close()

    def get_result(self):
        return self.__result
