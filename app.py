""" Create by Ken at 2020 Feb 11 """
# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from flask import request
from flask_cors import CORS
from connection import Connection
import json
import traceback

app = Flask(__name__)
CORS(app)

con = Connection("localhost", "root", "abc13579", "QuanLyCayTrongVatNuoi")


####### Phong ban
@app.route("/phong-ban", methods=["GET"])
def phongban():
    return jsonify(con.phongban(action="GET"))


@app.route("/phong-ban", methods=["POST"])
def phongbanPOST():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.phongban(action=request.method, data=r_dict))


@app.route("/phong-ban", methods=["PUT"])
def phongbanPUT():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.phongban(action=request.method, data=r_dict))


@app.route("/phong-ban", methods=["DELETE"])
def phongbanDELETE():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.phongban(action=request.method, data=r_dict))


### nhan viên
@app.route("/nhan-vien", methods=["GET"])
def nv():
    return jsonify(con.nhanvien(action=request.method))


@app.route("/nhan-vien", methods=["POST"])
def nvPOST():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.nhanvien(action=request.method, data=r_dict))


@app.route("/nhan-vien", methods=["PUT"])
def nvPUT():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.nhanvien(action=request.method, data=r_dict))


@app.route("/nhan-vien", methods=["DELETE"])
def nvDELETE():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.nhanvien(action=request.method, data=r_dict))


# vat_nuoi
@app.route("/hang-hoa-vat-nuoi", methods=["GET"])
def get_all_vat_nuoi():
    try:
        return jsonify(con.vat_nuoi(action="GET"))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/hang-hoa-vat-nuoi", methods=["POST"])
def add_vat_nuoi():
    try:
        content = request.data
        r_dict = json.loads(content.decode('utf-8'))
        return jsonify(con.vat_nuoi(action=request.method, data=r_dict))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/hang-hoa-vat-nuoi", methods=["PUT"])
def update_vat_nuoi():
    try:
        content = request.data
        r_dict = json.loads(content.decode('utf-8'))
        return jsonify(con.vat_nuoi(action=request.method, data=r_dict))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/hang-hoa-vat-nuoi", methods=["DELETE"])
def delete_vat_nuoi():
    try:
        content = request.data
        r_dict = json.loads(content.decode('utf-8'))
        return jsonify(con.vat_nuoi(action=request.method, data=r_dict))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


# cay_trong
@app.route("/hang-hoa-cay-trong", methods=["GET"])
def get_all_cay_trong():
    try:
        return jsonify(con.cay_trong(action="GET"))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/hang-hoa-cay-trong", methods=["POST"])
def add_cay_trong():
    try:
        content = request.data
        r_dict = json.loads(content.decode('utf-8'))
        return jsonify(con.cay_trong(action=request.method, data=r_dict))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/hang-hoa-cay-trong", methods=["PUT"])
def update_cay_trong():
    try:
        content = request.data
        r_dict = json.loads(content.decode('utf-8'))
        return jsonify(con.cay_trong(action=request.method, data=r_dict))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/hang-hoa-cay-trong", methods=["DELETE"])
def delete_cay_trong():
    try:
        content = request.data
        r_dict = json.loads(content.decode('utf-8'))
        return jsonify(con.cay_trong(action=request.method, data=r_dict))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/khach-hang", methods=["GET"])
def GETkh():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.khachhang(action=request.method, data=None))


@app.route("/khach-hang", methods=["POST"])
def POSTkh():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.khachhang(action=request.method, data=r_dict))


@app.route("/khach-hang", methods=["PUT"])
def PUTkh():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.khachhang(action=request.method, data=r_dict))


@app.route("/khach-hang", methods=["DELETE"])
def DELETEkh():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.khachhang(action=request.method, data=r_dict))


@app.route("/kho", methods=["GET"])
def GETkho():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.kho(action=request.method, data=None))


@app.route("/kho", methods=["POST"])
def POSTkho():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.kho(action=request.method, data=r_dict))


@app.route("/kho", methods=["PUT"])
def PUTkho():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.kho(action=request.method, data=r_dict))


@app.route("/kho", methods=["DELETE"])
def DELETEkho():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.kho(action=request.method, data=r_dict))


@app.route("/kho/<int:id_kho>/khu-vuc", methods=["GET"])
def list_khu_vuc(id_kho):
    try:
        ds_khu_vuc = con.list_khu_vuc(id_kho)
        return jsonify(ds_khu_vuc)
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/kho/<int:id_kho>/khu-vuc", methods=["POST"])
def add_khu_vuc(id_kho):
    try:
        content = request.data
        data = json.loads(content.decode('utf-8'))
        return jsonify(con.add_khu_vuc(id_kho, data))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/kho/<int:id_kho>/khu-vuc", methods=["PUT"])
def update_khu_vuc(id_kho):
    try:
        content = request.data
        data = json.loads(content.decode('utf-8'))
        return jsonify(con.update_khu_vuc(id_kho, data))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


@app.route("/kho/<int:id_kho>/khu-vuc", methods=["DELETE"])
def delete_khu_vuc(id_kho):
    try:
        content = request.data
        data = json.loads(content.decode('utf-8'))
        return jsonify(con.delete_khu_vuc(id_kho, data))
    except:
        print(traceback.format_exc())
        return 'Internal server error', 500


###
@app.route("/hang-hoa", methods=["GET"])
def GEThang_hoa():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.hang_hoa(action=request.method, data=None))


@app.route("/hang-hoa", methods=["POST"])
def POSThang_hoa():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.hang_hoa(action=request.method, data=r_dict))


@app.route("/hang-hoa", methods=["PUT"])
def PUThang_hoa():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.hang_hoa(action=request.method, data=r_dict))


@app.route("/hang-hoa", methods=["DELETE"])
def DELETEkhang_hoa():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.hang_hoa(action=request.method, data=r_dict))


@app.route("/hang-hoa", methods=["DELETE"])
def DELETEkhan1g_hoa():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.hang_hoa(action=request.method, data=r_dict))


@app.route("/hang-hoa-trong-kho", methods=["GET"])
def hhtk():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.hang_hoa_kho(action=request.method, data=r_dict))


@app.route("/ban-hang", methods=["POST", "PUT", "DELETE", "GET"])
def ban_hang0():
    content = request.data
    r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.ban_hang(action=request.method, data=r_dict))


@app.route("/chi-tiet-ban-hang", methods=["GET"])
def ban1_hang0():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.chi_tiet_ban_hang(action=request.method, data=None))


@app.route("/thong-ke-tong-doanh-thu", methods=["GET"])
def ban1_3hang0():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.thongkedoanhthu())


@app.route("/thong-ke-theo-hang-hoa", methods=["GET"])
def ban1_h13ang0():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.thongkedoanhthu_hanghoa())


@app.route("/thong-ke-theo-nhan-vien", methods=["GET"])
def ban151_hang0():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.thongkedoanhthu_nv())


@app.route("/thong-ke-hang-ton", methods=["GET"])
def ban1_h31232ang0():
    content = request.data
    # r_dict = json.loads(content.decode('utf-8'))
    return jsonify(con.thongkehangton())


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8686)
