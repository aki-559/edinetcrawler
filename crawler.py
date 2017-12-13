import urllib.request as req
import urllib.parse as parse
import zipfile
import sys
import os
from time import sleep
from tqdm import tqdm
import shutil
import subprocess
import pandas as pd
import ssl

context = ssl._create_unverified_context()

def upload_all(code):
    subprocess.run("aws s3 cp --quiet {} s3://edinet-crawler/{} --recursive".format(code, code), shell=True)

def connect(url):
    opt_dict = {"User-Agent":"Mozilla/5.0"}

    r = req.Request(url, headers=opt_dict)
    res = req.urlopen(r, context = context)

    if res.getcode() == 200:
        return res.read()
    else:
        return ""

def download_file(code, title, pdf, xbrl):
    ##証券コードが存在しないならばダウンロードは行わない
    if code == "": return -1

    sleep(1)

    if not os.path.exists("./" + code):
        os.mkdir("./" + code)

    dir_path = "./" + code + "/" + title

    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    #xbrlファイルのパス
    xbrl_path = dir_path + "/" + "tmp.zip"
    #xbrlファイルのパス
    pdf_path = dir_path + "/" + title + ".pdf"

    ##ファイルのダウンロード
    with open(xbrl_path, "wb") as f:
        res = connect(xbrl)
        f.write(res)

    with open(pdf_path, "wb") as f:
        res = connect(pdf)
        f.write(res)

    ##zipファイルの展開
    with zipfile.ZipFile(xbrl_path, "r") as input_file:
        input_file.extractall(path=dir_path)

    ##zipファイルの削除
    os.remove(xbrl_path)

    ##ファイルの移動
    transloc(dir_path)

    ##ファイルのアップロード
    upload_all(code)

    #フォルダの削除
    shutil.rmtree("./" + code)

    return 0

def transloc(dir_path):
    for item in os.listdir(dir_path):
        p1 = dir_path + "/" + item + "/XBRL/PublicDoc"

        if os.path.exists(p1):
            if not os.path.exists(dir_path + "/xbrl"):
                os.mkdir(dir_path + "/xbrl")
            if not os.path.exists(dir_path + "/html"):
                os.mkdir(dir_path + "/html")

            for r in os.listdir(p1):
                if os.path.isfile(p1+"/"+r):
                    e = r.split(".")[1]
                    if e == "xbrl" or e == "xsd" or e == "xml":
                        shutil.copyfile(p1+"/"+r, dir_path + "/xbrl/" + r)
                    elif e=="html" or e=="htm":
                        shutil.copyfile(p1+"/"+r, dir_path+ "/html/" +r)

            shutil.rmtree(dir_path + "/" + item)

        elif os.path.isdir(dir_path + "/" + item):
            os.rename(dir_path + "/" + item, dir_path + "/xbrl")
