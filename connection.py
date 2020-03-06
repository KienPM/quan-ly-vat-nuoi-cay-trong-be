import pymysql
import traceback
import copy


# con = pymysql.connect("localhost", "root","toor","csdl", autocommit=True)
class Connection:
    def __init__(self, ip, user, password, db_name):
        self.dict_conn = pymysql.connect(ip, user, password, db_name, autocommit=True, charset='utf8',
                                         cursorclass=pymysql.cursors.DictCursor)
        self.con = pymysql.connect(ip, user, password, db_name, autocommit=True, charset='utf8')
        self.db_name = db_name

    def get_sql_nv(self, action, data=None):
        try:
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
                    pbs = self.get_phongban_id_phong_ban(id_pb)
                    for item in pbs:
                        data["phong_ban"] = {
                            "id_phong_ban": item[0],
                            "ten_phong_ban": item[1],
                            "dia_chi": item[2],
                            "email": item[3]
                        }

                    sql = "Select sdt from {}.nhan_vien_sdt where id_nhan_vien=%s".format(self.db_name)
                    sdts = self.get_sdt_idnv(data["id_nhan_vien"])  # self.excute_sql(sql, data["id_nhan_vien"])
                    for sdt in sdts:
                        data['sdt'].append(sdt[0])
                    response.append(data)
                except:
                    continue
        except:
            return list()

    def delete_sql_nv(self, action, data=None):

        sql = "Delete from {}.nhan_vien  where id_nhan_vien=%s".format(
            self.db_name,
        )
        result = self.excute_sql(sql, (data["id_nhan_vien"]))

    def get_phongban_id_phong_ban(self, id_phong_ban):
        data = {}
        sql = "Select id_phong_ban,ten_phong_ban,dia_chi,email from {}.phong_ban where id_phong_ban =%s".format(
            self.db_name)
        pb = self.excute_sql(sql, id_phong_ban)
        for item in pb:
            data = {
                "id_phong_ban": item[0],
                "ten_phong_ban": item[1],
                "dia_chi": item[2],
                "email": item[3]
            }
        return data

    def get_sdt_idnv(self, data=None):
        sql = "Select sdt from {}.nhan_vien_sdt where id_nhan_vien=%s".format(self.db_name)
        sdts = self.excute_sql(sql, data["id_nhan_vien"])
        list_sdt = []
        for sdt in sdts:
            list_sdt.append(sdt[0])
        return list_sdt

    def delete_sdt(self, data=None):
        sql = "delete from {}.nhan_vien_sdt where id_nhan_vien=%s and sdt=%s ".format(self.db_name)
        sdts = self.excute_sql(sql, (data["id_nhan_vien"], data['sdt']))
        return data['sdt']

    def get_max_id_nv(self):
        return self.excute_sql("select max(id_nhan_vien) from {}.nhan_vien".format(self.db_name))[0][0]

    def add_sdt(self, data=None):
        data["id_nhan_vien"] = self.get_max_id_nv()
        for sdt in data['sdt']:
            self.excute_sql("insert into {}.nhan_vien_sdt  (sdt,id_nhan_vien) values (%s, %s)".format(self.db_name),
                            [sdt, data["id_nhan_vien"]])
        return data

    # _-----------------------
    def get_ban_hang(self, data=None):
        response = list()
        sql = "Select id_ban_hang,id_nhan_vien,id_khach_hang,ngay_ban from {}.ban_hang".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_ban_hang": item[0],
                "id_nhan_vien": item[1],
                "id_khach_hang": item[2],
                "ngay_ban": item[3],
            }
            response.append(data)
        return response

    def get_ban_hang_by_id(self, data=None):
        response = list()
        sql = "Select id_ban_hang,id_nhan_vien,id_khach_hang,ngay_ban from {}.ban_hang where id_ban_hang=%s".format(
            self.db_name)
        result = self.excute_sql(sql, data["id_ban_hang"])
        for item in result:
            data = {
                "id_ban_hang": item[0],
                "id_nhan_vien": item[1],
                "id_khach_hang": item[2],
                "ngay_ban": item[3],
            }
            response.append(data)
        return response

    # ```````````

    def add_ban_hang(self, data=None):
        sql = "insert into {}.ban_hang (id_ban_hang,id_nhan_vien,id_khach_hang,ngay_ban)  values  (%s, %s, %s,%s)".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_ban_hang"),
            data["id_nhan_vien"],
            data["id_khach_hang"],
            data["ngay_ban"]
        ]
                                 )

    def edit_ban_hang(self, data=None):
        sql = "update {}.ban_hang set id_nhan_vien=%s,id_khach_hang=%s,ngay_ban=%s  where id_ban_hang=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data["id_nhan_vien"],
            data["id_khach_hang"],
            data["ngay_ban"],
            data.get("id_ban_hang")
        ]
                                 )

    def delete_ban_hang(self, data=None):
        sql = "delete from  {}.ban_hang where id_ban_hang=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_ban_hang")
        ]
                                 )

    def get_hang_hoa(self, data=None):
        response = list()
        sql = "Select id_ban_hang,id_nhan_vien,id_khach_hang,ngay_ban from {}.ban_hang".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_ban_hang": item[0],
                "id_nhan_vien": item[1],
                "id_khach_hang": item[2],
                "ngay_ban": item[3],
            }
            response.append(data)
        return response

    def get_hang_hoa_by_nhanvien(self, data=None):
        response = list()
        sql = "Select id_ban_hang,id_nhan_vien,id_khach_hang,ngay_ban from {}.ban_hang where id_nhan_vien = %s".format(
            self.db_name)
        result = self.excute_sql(sql, data['id_nhan_vien'])
        for item in result:
            data = {
                "id_ban_hang": item[0],
                "id_nhan_vien": item[1],
                "id_khach_hang": item[2],
                "ngay_ban": item[3],
            }
            response.append(data)
        return response

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    def add_chi_tiet_ban_hang(self, data=None):
        sql = "insert into {}.chi_tiet_ban_hang (id_ban_hang,id_hang_hoa,id_kho,so_luong, gia)  values  (%s, %s, %s,%s, %s)".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_ban_hang"),
            data["id_hang_hoa"],
            data["id_kho"],
            data["so_luong"],
            data["gia"]
        ]
                                 )
        return data

    def edit_chi_tiet_ban_hang(self, data=None):
        sql = "update {}.chi_tiet_ban_hang set so_luong=%s,gia=%s  where id_ban_hang=%s and id_hang_hoa=%s and id_kho=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data["so_luong"],
            data["gia"],
            data.get("id_ban_hang"),
            data["id_hang_hoa"],
            data["id_kho"],
        ]
                                 )

    def delete_chi_tiet_ban_hang(self, data=None):
        sql = "delete from  {}.chi_tiet_ban_hang where id_ban_hang=%s and id_hang_hoa=%s and id_kho=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_ban_hang"),
            data["id_hang_hoa"],
            data["id_kho"]
        ]
                                 )

    def delete_chi_tiet_ban_hang_id_ban_hang(self, data=None):
        sql = "delete from  {}.chi_tiet_ban_hang where id_ban_hang=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_ban_hang"),
        ]
                                 )

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    def add_hang_hoa(self, data=None):
        sql = "insert into {}.hang_hoa (id_hang_hoa,ten,kieu_hang_hoa,gia,don_vi_tinh)  values  (%s, %s, %s,%s,%s)".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_hang_hoa"),
            data["ten"],
            data["kieu_hang_hoa"],
            data["gia"],
            data["don_vi_tinh"]
        ]
                                 )
        return data

    def edit_chi_tiet_ban_hang(self, data=None):
        sql = "update {}.chi_tiet_ban_hang set so_luong=%s,gia=%s  where id_ban_hang=%s and id_hang_hoa=%s and id_kho=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data["so_luong"],
            data["gia"],
            data.get("id_ban_hang"),
            data["id_hang_hoa"],
            data["id_kho"],
        ]
                                 )

    def delete_chi_tiet_ban_hang(self, data=None):
        sql = "delete from  {}.chi_tiet_ban_hang where id_ban_hang=%s and id_hang_hoa=%s and id_kho=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_ban_hang"),
            data["id_hang_hoa"],
            data["id_kho"]
        ]
                                 )

    def get_hang_hoa(self, data=None):
        response = list()
        sql = "Select id_hang_hoa,ten,kieu_hang_hoa,gia,don_vi_tinh from {}.hang_hoa".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_hang_hoa": item[0],
                "ten": item[1],
                "kieu_hang_hoa": item[2],
                "gia": item[3],
                "don_vi_tinh": item[4]
            }
            response.append(data)
        return response

    def get_chi_tiet_ban_hang(self, data=None):
        response = list()
        sql = "Select id_ban_hang, id_hang_hoa, id_kho, so_luong, gia from {}.chi_tiet_ban_hang".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_ban_hang": item[0],
                "id_hang_hoa": item[1],
                "id_kho": item[2],
                "so_luong": item[3],
                "gia": item[4]
            }
            response.append(data)
        return response

    def get_hang_hoa_by_id(self, data=None):
        response = list()
        sql = "Select id_hang_hoa,ten,kieu_hang_hoa,gia,don_vi_tinh from {}.hang_hoa where id_hang_hoa =%s".format(
            self.db_name)
        result = self.excute_sql(sql, [data["id_hang_hoa"]])
        for item in result:
            data = {
                "id_hang_hoa": item[0],
                "ten": item[1],
                "kieu_hang_hoa": item[2],
                "gia": item[3],
                "don_vi_tinh": item[4]
            }
            response.append(data)
        return response

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    def get_max_id_kh(self):
        return self.excute_sql("select max(id_khach_hang) from {}.khach_hang".format(self.db_name))[0][0]

    def add_khach_hang(self, data=None):
        sql = "insert into {}.khach_hang (id_khach_hang,sdt,ten,dia_chi,ngay_sinh,email,gioi_tinh)  values  (%s, %s, %s,%s, %s, %s, %s)".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_khach_hang"),
            data["sdt"],
            data["ten"],
            data["dia_chi"],
            data["ngay_sinh"],
            data["email"],
            data["gioi_tinh"],
        ]
                                 )
        return data

    def edit_khach_hang(self, data=None):
        sql = "update {}.khach_hang  set sdt=%s,ten=%s,dia_chi=%s,ngay_sinh=%s,email=%s,gioi_tinh=%s  where id_khach_hang=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data["sdt"],
            data["ten"],
            data["dia_chi"],
            data["ngay_sinh"],
            data["email"],
            data["gioi_tinh"],
            data.get("id_khach_hang")
        ]
                                 )
        return data

    def delete_khach_hang(self, data=None):
        sql = "delete from  {}.khach_hang where id_khach_hang=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_khach_hang")
        ]
                                 )

    def get_khach_hang(self, data=None):
        response = list()
        sql = "Select id_khach_hang,sdt,ten,dia_chi,ngay_sinh,email,gioi_tinh from {}.khach_hang".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_khach_hang": item[0],
                "sdt": item[1],
                "ten": item[2],
                "dia_chi": item[3],
                "ngay_sinh": item[4],
                "email": item[5],
                "gioi_tinh": item[6],
            }
            response.append(data)
        return response

    def get_khach_hang_by_id(self, data=None):
        response = list()
        sql = "Select id_khach_hang,sdt,ten,dia_chi,ngay_sinh,email,gioi_tinh from {}.khach_hang where id_khach_hang= %s".format(
            self.db_name)
        result = self.excute_sql(sql, data["id_khach_hang"])
        for item in result:
            data = {
                "id_khach_hang": item[0],
                "sdt": item[1],
                "ten": item[2],
                "dia_chi": item[3],
                "ngay_sinh": item[4],
                "email": item[5],
                "gioi_tinh": item[6]
            }
            response.append(data)
        return response

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    def get_max_id_kho(self):
        return self.excute_sql("select max(id_kho) from {}.kho".format(self.db_name))[0][0]

    def get_kho(self, data=None):
        response = list()
        sql = "Select id_kho,ten,dien_tich from {}.kho".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_kho": item[0],
                "ten": item[1],
                "dien_tich": item[2],
            }
            response.append(data)
        return response

    def get_kho_by_id(self, data=None):
        response = list()
        sql = "Select id_kho,ten,dien_tich from {}.kho where id_kho =%s".format(self.db_name)
        result = self.excute_sql(sql, data['id_kho'])
        for item in result:
            data = {
                "id_kho": item[0],
                "ten": item[1],
                "dien_tich": item[2],
            }
            response.append(data)
        return response

    def add_kho(self, data=None):
        sql = "insert into {}.kho (id_kho,ten,dien_tich)  values  (%s, %s, %s)".format(self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_kho"),
            data["ten"],
            data["dien_tich"]
        ]
                                 )
        return data

    def edit_kho(self, data=None):
        sql = "update {}.kho  set ten=%s,dien_tich=%s where id_kho=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data["ten"],
            data["dien_tich"],
            data.get("id_kho")
        ]
                                 )
        return data

    def delete_kho(self, data=None):
        sql = "delete from  {}.kho where id_kho=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_kho")
        ]
                                 )

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    def add_khu_vuc(self, id_kho, data):
        sql = "INSERT INTO khu_vuc(id_kho, ten_khu_vuc, mo_ta) VALUES(%s,%s,%s)"
        cursor = self.execute_get_cursor(sql, (
            id_kho, data['ten_khu_vuc'], data['mo_ta']
        ))
        return data

    def list_khu_vuc(self, id_kho):
        sql = "SELECT * FROM kho WHERE id_kho=%s"
        cursor = self.execute_get_cursor(sql, (id_kho,))
        chi_tiet_kho = cursor.fetchone()
        sql = "SELECT * FROM khu_vuc WHERE id_kho=%s"
        cursor = self.execute_get_cursor(sql, (id_kho,))
        chi_tiet_kho["khu_vuc"] = cursor.fetchall()
        return chi_tiet_kho

    def update_khu_vuc(self, id_kho, data):
        sql = "UPDATE khu_vuc SET mo_ta=%s WHERE id_kho=%s AND ten_khu_vuc=%s"
        self.execute_get_cursor(sql, (
            data['mo_ta'], id_kho, data['ten_khu_vuc']
        ))
        return data

    def delete_khu_vuc(self, id_kho, data):
        sql = "DELETE FROM khu_vuc WHERE id_kho=%s AND ten_khu_vuc=%s"
        self.execute_get_cursor(sql, (
            id_kho, data['ten_khu_vuc']
        ))

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    # id_hang_hoa, ten, kieu_hang_hoa, gia, don_vi_tinh
    def add_hang_hoa(self, data=None):
        sql = "insert into {}.hang_hoa (id_hang_hoa, ten, kieu_hang_hoa, gia, don_vi_tinh)  values  (%s, %s, %s, %s, %s)".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_hang_hoa"),
            data["ten"],
            data["kieu_hang_hoa"],
            data["gia"],
            data["don_vi_tinh"]
        ]
                                 )
        return data

    def edit_hang_hoa(self, data=None):
        sql = "update {}.hang_hoa  set ten=%s,kieu_hang_hoa=%s,gia=%s,don_vi_tinh=%s where id_hang_hoa=%s".format(
            self.db_name)
        result = self.excute_sql(sql, [
            data["ten"],
            data["kieu_hang_hoa"],
            data["gia"],
            data["don_vi_tinh"],
            data.get("id_hang_hoa")
        ]
                                 )
        return data

    def delete_hang_hoa(self, data=None):
        sql = "delete from  {}.hang_hoa where id_hang_hoa=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data["id_hang_hoa"]
        ]
                                 )

    def get_id_hang_ton(self, data=None):
        response = list()
        sql = "Select id_kho, id_hang_hoa, so_luong from {}.luu_kho where id_kho =%s".format(self.db_name)
        result = self.excute_sql(sql, [data["id_kho"]])
        for item in result:
            data = {
                "id_kho": item[0],
                "id_hang_hoa": item[1], "so_luong": item[2],
            }
            response.append(data)
        return response

    def get_ids_hang_ton(self, data=None):
        response = list()
        sql = "Select id_kho, id_hang_hoa, so_luong from {}.luu_kho".format(self.db_name)
        result = self.excute_sql(sql)
        for item in result:
            data = {
                "id_kho": item[0],
                "id_hang_hoa": item[1],
                "so_luong": item[2],
            }
            response.append(data)
        return response

    # def get_khu_vuc_by_id(self, data=None):
    #     response = list()
    #     sql = "Select id_kho,ten_khu_vuc,mo_ta from {}.khu_vuc where id_kho =%s and ten_khu_vuc =%s".format(self.db_name)
    #     result = self.excute_sql(sql, [data['id_kho'], data['ten_khu_vuc']])
    #     for item in result:
    #         data  = {
    #             "id_kho": item[0],
    #             "ten_khu_vuc": item[1],
    #             "mo_ta": item[2],
    #         }
    #         response.append(data)
    #     return response
    def get_nv_by_id(self, data=None):
        sql = "Select id_nhan_vien,ho,ten_dem,ten,dia_chi,ngay_sinh,email,gioi_tinh,id_phong_ban from {}.nhan_vien where id_nhan_vien=%s ".format(
            self.db_name)
        result = self.excute_sql(sql, data['id_nhan_vien'])
        response = []
        for item in result:
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
            response.append(data)
        return response

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------
    def add_luu_kho(self, data=None):
        sql = "insert into {}.luu_kho (id_kho,id_hang_hoa,so_luong)  values  (%s, %s, %s)".format(self.db_name)
        result = self.excute_sql(sql, [
            data.get("id_kho"),
            data.get("id_hang_hoa"),
            data["so_luong"]
        ]
                                 )
        return data

    def edit_luu_kho(self, data=None):
        sql = "update {}.luu_kho  set so_luong=%s where id_kho=%s and id_hang_hoa=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data["so_luong"],
            data["id_kho"],
            data["id_hang_hoa"]
        ]
                                 )
        return data

    def delete_luu_kho(self, data=None):
        sql = "delete from  {}.luu_kho where id_kho=%s and id_hang_hoa=%s".format(self.db_name)
        result = self.excute_sql(sql, [
            data["id_kho"],
            data["id_hang_hoa"]
        ]
                                 )

    # --------------------------------------------------------#--------------------------------------------------------#--------------------------------------------------------

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
                sql = "Select id_phong_ban,ten_phong_ban,dia_chi,email from {}.phong_ban where id_phong_ban =%s".format(
                    self.db_name)
                pb = self.excute_sql(sql, data["id_phong_ban"])
                for item in pb:
                    data["phong_ban"] = {
                        "id_phong_ban": item[0],
                        "ten_phong_ban": item[1],
                        "dia_chi": item[2],
                        "email": item[3]
                    }
                sql = "Select sdt from {}.nhan_vien_sdt where id_nhan_vien=%s".format(self.db_name)
                sdts = self.excute_sql(sql, data["id_nhan_vien"])
                response = data
            elif action == "DELETE":
                sql = "Delete from {}.nhan_vien_sdt  where id_nhan_vien=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, (data["id_nhan_vien"]))

                for result in self.get_hang_hoa_by_nhanvien(data):
                    self.delete_chi_tiet_ban_hang_id_ban_hang(result)
                    self.delete_ban_hang(result)
                sql = "Delete from {}.nhan_vien  where id_nhan_vien=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, (data["id_nhan_vien"]))
            elif action == "PUT":
                sql = "update {}.nhan_vien set ho=%s,ten_dem=%s,ten=%s,dia_chi=%s,ngay_sinh=%s,email=%s,gioi_tinh=%s,id_phong_ban=%s  where id_nhan_vien=%s".format(
                    self.db_name,
                )
                result = self.excute_sql(sql, [
                    data["ho"],
                    data["ten_dem"],
                    data["ten"],
                    data["dia_chi"],
                    data["ngay_sinh"],
                    data["email"],
                    data["gioi_tinh"],
                    data["id_phong_ban"],
                    data["id_nhan_vien"],
                ]
                                         )
                sql = "Select id_phong_ban,ten_phong_ban,dia_chi,email from {}.phong_ban where id_phong_ban =%s".format(
                    self.db_name)
                pb = self.excute_sql(sql, data["id_phong_ban"])
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

    def khachhang(self, action, data=None):
        response = None
        if action == "GET":
            response = self.get_khach_hang()
        elif action == "POST":
            self.add_khach_hang(data)
            data["id_khach_hang"] = self.get_max_id_kh()
            response = data
        elif action == "PUT":
            self.edit_khach_hang(data)
            response = self.get_khach_hang_by_id(data)[0]
        elif action == "DELETE":
            response = self.delete_khach_hang(data)
        return response

    def kho(self, action, data=None):
        response = None
        if action == "GET":
            response = self.get_kho()
        elif action == "POST":
            self.add_kho(data)
            data["id_kho"] = self.get_max_id_kho() if not data.get("id_kho") else data["id_kho"]
            response = data
        elif action == "PUT":
            self.edit_kho(data)
            response = self.get_kho_by_id(data)[0]
        elif action == "DELETE":
            self.delete_kho(data)
        return response

    def hang_hoa(self, action, data=None):
        response = None
        if action == "GET":
            response = self.get_hang_hoa(data)
        elif action == "POST":
            self.add_hang_hoa(data)
            # data["id_kho"]= self.get_max_id_kho() if not data.get("id_kho") else data["id_kho"]
            response = data
        elif action == "PUT":
            self.edit_hang_hoa(data)
            response = self.get_hang_hoa_by_id(data)[0]
        elif action == "DELETE":
            response = self.delete_hang_hoa(data)
        return response

    def ban_hang(self, action, data=None):
        response = None
        if action == "GET":
            response = []
            datas = self.get_ban_hang(data)
            for item in datas:
                data = {
                    "id_ban_hang": item["id_ban_hang"],
                    "ngay_ban": item["ngay_ban"],
                    "nhan_vien": self.get_khach_hang_by_id(item)[0],
                    "khach_hang": self.get_nv_by_id(item)[0],
                }
                response.append(data)
        elif action == "POST":
            self.add_ban_hang(data)
            # data["id_kho"]= self.get_max_id_kho() if not data.get("id_kho") else data["id_kho"]
            response = data
        elif action == "PUT":
            self.edit_ban_hang(data)
            item = self.get_ban_hang_by_id(data)[0]
            response = {
                "id_ban_hang": item["id_ban_hang"],
                "ngay_ban": item["ngay_ban"],
                "nhan_vien": self.get_khach_hang_by_id(item)[0],
                "khach_hang": self.get_nv_by_id(item)[0],
            }

        elif action == "DELETE":
            response = self.ban_hang(data)
        return response

    def hang_hoa_kho(self, action, data=None):
        response = None
        if action == "GET":
            datas = self.get_id_hang_ton(data)
            response = []
            for data in datas:
                response.append(self.get_hang_hoa_by_id(data)[0])
        return response

    def chi_tiet_ban_hang(self, action, data=None):
        response = None
        if action == "GET":
            datas = self.get_chi_tiet_ban_hang(data)
            response = []
            for data in datas:
                item = {
                    "hang_hoa": self.get_hang_hoa_by_id(data)[0],
                    "kho": self.get_kho_by_id(data)[0],
                    "so_luong": data["so_luong"],
                    "gia": data["gia"],
                }
                response.append(item)
        return response

    ####

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

    ####
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

    def thongkedoanhthu(self, data=None):
        chi_tiet_ban_hangs = self.get_chi_tiet_ban_hang()
        datas = self.get_ban_hang()
        dict_doanh_thu = dict()
        for data in datas:
            for chi_tiet_ban_hang in chi_tiet_ban_hangs:
                if data["id_ban_hang"] == chi_tiet_ban_hang["id_ban_hang"]:
                    dict_doanh_thu[data['ngay_ban']] = dict_doanh_thu[data['ngay_ban']] + chi_tiet_ban_hang[
                        "gia"] if dict_doanh_thu.get(data['ngay_ban']) else chi_tiet_ban_hang["gia"]
        # dict_doanh_thu = sorted(datas , key=lambda k: k['ngay_ban'])

        # list_thang_co_doanh_thu = [doanh_thu.month  for doanh_thu in list(dict_doanh_thu.keys())]
        response = []
        month = 0
        for ngay_ban in sorted(dict_doanh_thu):
            month += 1
            print(ngay_ban.month)
            while month < ngay_ban.month:
                response.append(0)
                month += 1
            response.append(dict_doanh_thu.get(ngay_ban))
        return response

    def thongkedoanhthu_hanghoa(self, data=None):
        chi_tiet_ban_hangs = self.get_chi_tiet_ban_hang()
        datas = self.get_ban_hang()
        dict_doanh_thu = dict()
        for data in datas:
            for chi_tiet_ban_hang in chi_tiet_ban_hangs:
                if data["id_ban_hang"] == chi_tiet_ban_hang["id_ban_hang"]:
                    hang_hoa = self.get_hang_hoa_by_id(chi_tiet_ban_hang)[0]
                    if dict_doanh_thu.get(chi_tiet_ban_hang['id_hang_hoa']):
                        dict_doanh_thu[chi_tiet_ban_hang['id_hang_hoa']]["doanh_thu"] += chi_tiet_ban_hang["gia"]
                    else:
                        dict_doanh_thu[chi_tiet_ban_hang['id_hang_hoa']] = copy.deepcopy(hang_hoa)
                        dict_doanh_thu[chi_tiet_ban_hang['id_hang_hoa']].update({"doanh_thu": chi_tiet_ban_hang["gia"]})
        return list(dict_doanh_thu.values())

    def thongkedoanhthu_nv(self, data=None):
        chi_tiet_ban_hangs = self.get_chi_tiet_ban_hang()
        datas = self.get_ban_hang()
        dict_doanh_thu = dict()
        for data in datas:
            for chi_tiet_ban_hang in chi_tiet_ban_hangs:
                if data["id_ban_hang"] == chi_tiet_ban_hang["id_ban_hang"]:
                    nhan_vien = self.get_nv_by_id(data)[0]
                    if dict_doanh_thu.get(nhan_vien['id_nhan_vien']):
                        dict_doanh_thu[nhan_vien['id_nhan_vien']]["doanh_thu"] += chi_tiet_ban_hang["gia"]
                    else:
                        dict_doanh_thu[nhan_vien['id_nhan_vien']] = copy.deepcopy(nhan_vien)
                        dict_doanh_thu[nhan_vien['id_nhan_vien']].update({"doanh_thu": chi_tiet_ban_hang["gia"]})
        return list(dict_doanh_thu.values())

    def thongkehangton(self, data=None):
        hang_tons = self.get_ids_hang_ton()
        dict_hang_tons = dict()
        for hang_ton in hang_tons:
            if dict_hang_tons.get(hang_ton["id_hang_hoa"]):
                dict_hang_tons[hang_ton["id_hang_hoa"]].update(
                    {"so_luong": dict_hang_tons[hang_ton["id_hang_hoa"]]["so_luong"] + hang_ton['so_luong']})
            else:
                dict_hang_tons[hang_ton["id_hang_hoa"]] = copy.deepcopy(hang_ton)
                dict_hang_tons[hang_ton["id_hang_hoa"]].update(self.get_hang_hoa_by_id(hang_ton)[0])
        return list(dict_hang_tons.values())

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
