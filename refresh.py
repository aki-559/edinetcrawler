##東証のサイトから銘柄の一覧を取得する
import pandas as pd
import crawler
import dailyrefresh as db
import os.path
import zipfile
from bs4 import BeautifulSoup
import subprocess
from datetime import datetime

def scode(string):
    return string[:4]

def make_blank(string):
    if string == "-":
        return ""
    elif type(string) == str and " " in string:
        return string.replace(" ", "")
    else:
        return string

def companies():
    url = "http://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
    root = "http://www.jpx.co.jp"

    res = crawler.connect(url)
    soup = BeautifulSoup(res, "lxml")
    link = soup.table.td.a["href"]
    res = crawler.connect(root + link)

    if res != "":
        with open(os.path.expanduser("~/c_info/data_j.xls"), "wb") as f:
            f.write(res)

        market = {"出資証券":"0", "市場第一部（内国株）":"1", "市場第二部（内国株）":"2", "マザーズ（内国株）":"3", \
        "JASDAQ(スタンダード・内国株）":"4", "ETF・ETN":"5", "REIT・ベンチャーファンド・カントリーファンド・インフラファンド": "6"}

        df = pd.read_excel(os.path.expanduser("~/c_info/data_j.xls"))
        df["市場・商品区分"] = df["市場・商品区分"].map(market)
        df = df.loc[:, ["コード", "銘柄名","市場・商品区分", "33業種区分","17業種区分",]].applymap(make_blank)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        timestamps = pd.Series([timestamp]*len(df))
        df["created_at"] = timestamps
        df["updated_at"] = timestamps
        df.to_csv(os.path.expanduser("~/c_info/companies.csv"), index=False, header=False, encoding="utf-8")

    else:
        print("更新に失敗しました。")

def e_code_list():
    root = "https://disclosure.edinet-fsa.go.jp"
    top = "/E01EW/download?1512538021191"
    uji_bean = "ee.bean.W1E62071.EEW1E62071Bean"
    uji_verb = "W1E62071EdinetCodeDownload"
    SESSIONKEY = top.split("?")[1]
    file_info = "lgKbn=2&dflg=0&iflg=0&dispKbn=1"
    _id = "W1E62071"
    components = [top, "uji.bean="+uji_bean, "uji.verb="+uji_verb,"TID="+_id, "PID"+_id, "SESSIONKEY="+SESSIONKEY, file_info]
    url = root + "&".join(components)

    res = crawler.connect(url)

    with open(os.path.expanduser("~/c_info/EdinetcodeDlInfo.zip"), "wb") as f:
        f.write(res)

    with zipfile.ZipFile(os.path.expanduser("~/c_info/EdinetcodeDlInfo.zip"), "r") as input_file:
        input_file.extractall(path=os.path.expanduser("~/c_info"))

    os.remove(os.path.expanduser("~/c_info/EdinetcodeDlInfo.zip"))

def code_pair():
    df = pd.read_csv(os.path.expanduser("~/c_info/EdinetcodeDlInfo.csv"), encoding="cp932", skiprows=1)
    df = df.loc[:, ["ＥＤＩＮＥＴコード", "証券コード"]].dropna()
    df["証券コード"] = df["証券コード"].astype(str).apply(scode)
    df.to_csv(os.path.expanduser("~/c_info/code_pair.csv"), index=False, header=False)

def to_db():
    dbname = "investment_dashboard_staging"
    hostname = "sparx-stg.cqt5q7qi5yif.ap-northeast-1.rds.amazonaws.com"
    username = "investment_dashboard"

    commands = [b"delete from code_pair;\n", b"\copy code_pair from ~/c_info/code_pair.csv with csv;\n", b"delete from companies;\n",\
    b"\copy companies(security_code, name, market, industry_33, industry_17, created_at, updated_at) from ~/c_info/companies.csv with csv;\n", b"\q"]

    work = subprocess.Popen("psql -h {} -U {} -d {}".format(hostname, username, dbname), shell=True, stdin=subprocess.PIPE)

    for command in commands:
        work.stdin.write(command)

    output = work.communicate()[0]
    print(output)

if __name__ == "__main__":
    companies()
    e_code_list()
    code_pair()
    to_db()
