import pymysql



class mysqlCommon:
    __result = []

    def excute_query(self, a: str):
        # TO_DO 프로퍼티에서 가져오기
        conn = pymysql.connect(host='localhost', user='root', password='password', charset='utf8')
        cursor = conn.cursor()
        sql = "SELECT * FROM TEST WHERE TESTTT = %s"
        cursor.execute(sql, a)
        result = cursor.fetchall()
        for data in result:
            self.__result.append(data)
        conn.commit()
        conn.close()

    def get_result(self):
        return self.__result
