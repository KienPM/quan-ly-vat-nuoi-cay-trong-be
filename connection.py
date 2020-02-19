import pymysql
import traceback


# con = pymysql.connect("localhost", "root","toor","csdl", autocommit=True)
class Connection:
    def __init__(self, ip, user, password, db_name):
        self.con = pymysql.connect(ip, user, password, db_name, autocommit=True, charset='utf8')
        self.dict_conn = pymysql.connect(ip, user, password, db_name, autocommit=True, charset='utf8',
                                         cursorclass=pymysql.cursors.DictCursor)
        self.db_name = db_name

    def phongban(self, action, data=None):
        try:
            tmp_data = {
                "id_phong_ban": None,
                "ten_phong_ban": None,
                "dia_chi": None,
                "email": None
            }
            response = None
            sql = None
            if action == "POST":
                data["id_phong_ban"] = None if data.get("id_phong_ban") is None else data.get("id_phong_ban")
                sql = "insert into {}.phong_ban (id_phong_ban,ten_phong_ban,dia_chi,email) values  (%s, %s, %s,%s)".format(
                    self.db_name)
                result = self.excute_sql(sql,
                                         (data["id_phong_ban"], data["ten_phong_ban"], data["dia_chi"], data["email"]))
                data["id_phong_ban"] = self.excute_sql(
                    "select max(id_phong_ban) from {}.phong_ban".format(self.db_name))
                response = data
            elif action == "DELETE":
                sql = "Delete from {}.phong_ban  where id_phong_ban=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, (data["id_phong_ban"]))
            elif action == "PUT":
                sql = "update {}.phong_ban set ten_phong_ban=%s, dia_chi=%s , email=%s where id_phong_ban=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, (
                    data["ten_phong_ban"],
                    data["dia_chi"],
                    data["email"],
                    data["id_phong_ban"])
                                         )
                response = data
            elif action == "GET":
                response = list()
                sql = "Select id_phong_ban,ten_phong_ban,dia_chi,email from {}.phong_ban".format(self.db_name)
                result = self.excute_sql(sql)
                for item in result:
                    data = {
                        "id_phong_ban": item[0],
                        "ten_phong_ban": item[1],
                        "dia_chi": item[2],
                        "email": item[3]
                    }
                    response.append(data)
            self.con.commit()
        except:
            self.con.rollback()

        return response

    def nhanvien(self, action, data=None):
        # data  = {
        #     "id_nhan_vien": item[0],
        #     "ho": item[1],
        #     "ten_dem":item[2],
        #     "ten":item[3],
        #     "dia_chi": item[4],
        #     "ngay_sinh": item[5],
        #     "email": item[6],
        #     "gioi_tinh": item[7],
        #     "sdt": list()
        # }
        try:
            tmp_data = {
                "id_phong_ban": None,
                "ten_phong_ban": None,
                "dia_chi": None,
                "email": None
            }
            response = None
            sql = None
            if action == "POST":
                data["id_nhan_vien"] = None if data.get("id_nhan_vien") is None else data.get("id_nhan_vien")
                sql = "insert into {}.nhan_vien (id_nhan_vien,ho,ten_dem,ten,dia_chi,ngay_sinh,email,gioi_tinh,id_phong_ban)  values  (%s, %s, %s,%s,%s, %s, %s,%s, %s)".format(
                    self.db_name)
                result = self.excute_sql(sql, [
                    data["id_nhan_vien"],
                    data["ho"],
                    data["ten_dem"],
                    data["ten"],
                    data["dia_chi"],
                    data["ngay_sinh"],
                    data["email"],
                    data["gioi_tinh"],
                    data["id_phong_ban"],
                ]
                                         )
                data["id_nhan_vien"] = \
                    self.excute_sql("select max(id_nhan_vien) from {}.nhan_vien".format(self.db_name))[0][0]
                for sdt in data['sdt']:
                    self.excute_sql(
                        "insert into {}.nhan_vien_sdt  (sdt,id_nhan_vien) values (%s, %s)".format(self.db_name),
                        [sdt, data["id_nhan_vien"]])
                response = data
            elif action == "DELETE":
                sql = "Delete from {}.phong_ban  where id_phong_ban=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, (data["id_phong_ban"]))
            elif action == "PUT":
                sql = "update {}.phong_ban set ten_phong_ban=%s, dia_chi=%s , email=%s where id_phong_ban=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, (
                    data["ten_phong_ban"],
                    data["dia_chi"],
                    data["email"],
                    data["id_phong_ban"])
                                         )
                response = data
            elif action == "GET":
                response = list()
                sql = "Select id_nhan_vien,ho,ten_dem,ten,dia_chi,ngay_sinh,email,gioi_tinh,id_phong_ban from {}.nhan_vien".format(
                    self.db_name)
                result = self.excute_sql(sql)
                for item in result:
                    try:
                        data = {
                            "id_nhan_vien": item[0],
                            "ho": item[1],
                            "ten_dem": item[2],
                            "ten": item[3],
                            "dia_chi": item[4],
                            "ngay_sinh": item[5],
                            "email": item[6],
                            "gioi_tinh": item[7],
                            "sdt": list()
                        }
                        id_pb = item[8]
                        sql = "Select id_phong_ban,ten_phong_ban,dia_chi,email from {}.phong_ban where id_phong_ban =%s".format(
                            self.db_name)
                        pb = self.excute_sql(sql, id_pb)
                        for item in pb:
                            data["phong_ban"] = {
                                "id_phong_ban": item[0],
                                "ten_phong_ban": item[1],
                                "dia_chi": item[2],
                                "email": item[3]
                            }
                        sql = "Select sdt from {}.nhan_vien_sdt where id_nhan_vien=%s".format(self.db_name)
                        sdts = self.excute_sql(sql, data["id_nhan_vien"])
                        for sdt in sdts:
                            data['sdt'].append(sdt[0])
                        response.append(data)
                    except Exception as e:
                        print(e)
                        continue
            self.con.commit()
        except:
            print(traceback.format_exc())
            self.con.rollback()
        return response

    def vat_nuoi(self, action, data=None):
        try:
            response = None
            if action == "POST":
                sql = "insert into {}.hang_hoa (ten,kieu_hang_hoa,gia,don_vi_tinh) values  (%s, %s, %s,%s)".format(
                    self.db_name)
                cursor = self.execute_get_cursor(sql, (
                    data["ten"],
                    data["kieu_hang_hoa"],
                    data["gia"],
                    data["don_vi_tinh"]
                ))

                data["id_hang_hoa_vat_nuoi"] = cursor.lastrowid
                sql = "INSERT INTO vat_nuoi(id_hang_hoa_vat_nuoi,dia_ly,hinh_thai_ngoai_hinh,huong_sx,muc_do_hoan_thien) " \
                      "VALUES (%s,%s,%s,%s,%s)"
                self.execute_get_cursor(sql, (
                    data["id_hang_hoa_vat_nuoi"],
                    data["dia_ly"],
                    data["hinh_thai_ngoai_hinh"],
                    data["huong_sx"],
                    data["muc_do_hoan_thien"]
                ))
                response = data
            elif action == "DELETE":
                sql = "Delete from vat_nuoi where id_hang_hoa_vat_nuoi=%s"
                self.excute_sql(sql, (data["id_hang_hoa_vat_nuoi"]))
                sql = "Delete from hang_hoa where id_hang_hoa=%s"
                self.excute_sql(sql, (data["id_hang_hoa_vat_nuoi"]))
            elif action == "PUT":
                sql = "UPDATE hang_hoa SET ten=%s,gia=%s,don_vi_tinh=%s " \
                      "WHERE id_hang_hoa=%s"
                self.excute_sql(sql, (
                    data["ten"],
                    data["gia"],
                    data["don_vi_tinh"],
                    data["id_hang_hoa_vat_nuoi"]
                ))

                sql = "UPDATE vat_nuoi SET dia_ly=%s,hinh_thai_ngoai_hinh=%s,huong_sx=%s,muc_do_hoan_thien=%s " \
                      "WHERE id_hang_hoa_vat_nuoi=%s"
                self.excute_sql(sql, (
                    data["dia_ly"],
                    data["hinh_thai_ngoai_hinh"],
                    data["huong_sx"],
                    data["muc_do_hoan_thien"],
                    data["id_hang_hoa_vat_nuoi"]
                ))

                response = data
            elif action == "GET":
                sql = "SELECT id_hang_hoa_vat_nuoi,ten,kieu_hang_hoa,gia,don_vi_tinh,dia_ly,hinh_thai_ngoai_hinh,huong_sx,muc_do_hoan_thien " \
                      "FROM hang_hoa,vat_nuoi " \
                      "WHERE hang_hoa.id_hang_hoa = vat_nuoi.id_hang_hoa_vat_nuoi"
                cursor = self.execute_get_cursor(sql)
                response = cursor.fetchall()
            self.con.commit()
            return response
        except Exception as e:
            self.con.rollback()
            raise e

    def cay_trong(self, action, data=None):
        try:
            response = None
            if action == "POST":
                sql = "insert into {}.hang_hoa (ten,kieu_hang_hoa,gia,don_vi_tinh) values  (%s, %s, %s,%s)".format(
                    self.db_name)
                cursor = self.execute_get_cursor(sql, (
                    data["ten"],
                    data["kieu_hang_hoa"],
                    data["gia"],
                    data["don_vi_tinh"]
                ))

                data["id_hang_hoa_cay_trong"] = cursor.lastrowid
                sql = "INSERT INTO cay_trong(id_hang_hoa_cay_trong,nguon_goc,phuong_phap_lai_tao,dd_di_truyen) " \
                      "VALUES(%s,%s,%s,%s)"
                self.execute_get_cursor(sql, (
                    data["id_hang_hoa_cay_trong"],
                    data["nguon_goc"],
                    data["phuong_phap_lai_tao"],
                    data["dd_di_truyen"]
                ))
                response = data
            elif action == "DELETE":
                sql = "Delete from cay_trong where id_hang_hoa_cay_trong=%s"
                self.excute_sql(sql, (data["id_hang_hoa_cay_trong"]))
                sql = "Delete from hang_hoa where id_hang_hoa=%s"
                self.excute_sql(sql, (data["id_hang_hoa_cay_trong"]))
            elif action == "PUT":
                sql = "UPDATE hang_hoa SET ten=%s,gia=%s,don_vi_tinh=%s " \
                      "WHERE id_hang_hoa=%s"
                self.excute_sql(sql, (
                    data["ten"],
                    data["gia"],
                    data["don_vi_tinh"],
                    data["id_hang_hoa_cay_trong"]
                ))

                sql = "UPDATE cay_trong SET nguon_goc=%s,phuong_phap_lai_tao=%s,dd_di_truyen=%s " \
                      "WHERE id_hang_hoa_cay_trong=%s"
                self.excute_sql(sql, (
                    data["nguon_goc"],
                    data["phuong_phap_lai_tao"],
                    data["dd_di_truyen"],
                    data["id_hang_hoa_cay_trong"]
                ))

                response = data
            elif action == "GET":
                sql = "SELECT id_hang_hoa_cay_trong,ten,kieu_hang_hoa,gia,don_vi_tinh,nguon_goc,phuong_phap_lai_tao,dd_di_truyen " \
                      "FROM hang_hoa,cay_trong " \
                      "WHERE hang_hoa.id_hang_hoa = cay_trong.id_hang_hoa_cay_trong"
                cursor = self.execute_get_cursor(sql)
                response = cursor.fetchall()
            self.con.commit()
            return response
        except Exception as e:
            self.con.rollback()
            raise e

    def excute_sql(self, sql, value=None):
        cur = self.con.cursor()
        if value:
            cur.execute(sql, value)
        else:
            cur.execute(sql)
        result = cur.fetchall()
        return result

    def execute_get_cursor(self, sql, args=()):
        cursor = self.dict_conn.cursor()
        cursor.execute(sql, args)
        return cursor
