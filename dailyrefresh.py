##一日一回のペースでs3データベースの更新を行います。

import crawler
from bs4 import BeautifulSoup
import urllib.request as req
from datetime import datetime
from datetime import date
import re
import sqlalchemy
import pandas as pd
import os.path
from tqdm import tqdm
import sys

def today():
    today = date.today()

    return str(today.year), str(today.month), str(today.day)

def mkurl(year, month, day):
    url = "https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.verb=W1E63021CXP002002DSPSch&uji.bean=ee.bean.parent.EECommonSearchBean&PID=W1E63021&TID=W1E63021&SESSIONKEY=1511933626399&lgKbn=2&pkbn=0&skbn=1&dskb=&askb=&dflg=0&iflg=null&preId=1&xbr=on&sec=&scc=&snm=&spf1=1&spf2=1&iec=&icc=&inm=&spf3=1&fdc=&fnm=&spf4=1&spf5=2&otd=120&cal=1&era=H&yer=&mon=&cal2=2&psr=2&yer2=&mon2=&day2=&yer3=&mon3=&day3=&row=100&idx=0&str=&kbn=1&flg=&syoruiKanriNo="

    #検索する日付の設定
    url = url.replace("yer2=", "yer2=" + year).replace("yer3=", "yer3=" + year)
    url = url.replace("mon2=", "mon2=" + month).replace("mon3=", "mon3=" + month)
    url = url.replace("day2=", "day2=" + day).replace("day3=", "day3=" + day)

    return url

def rm_escape(string):
    return string.replace("\t","").replace("\n","").replace("\r","")

def get_components(url):
    root = "https://disclosure.edinet-fsa.go.jp"
    pdfs = []
    xbrls = []
    titles = []
    codes = []

    res = crawler.connect(url)
    soup = BeautifulSoup(res, "lxml")

    for row in soup.find_all("tr"):
        for cell in row.find_all("td", class_="table_border_1 table_cellpadding_1 "):
            if cell.find("a", attrs={"onclick": re.compile("^return clickDocNameForNotPaper")}) != None:
                #titleの格納
                title = str(cell.a.string)
                title = rm_escape(title)

                if not re.match("有価*", title):
                    break

                titles.append(title)

            elif cell.find("img", attrs = {"alt": "PDF"}) != None:
            #pdfのリンクを格納
                pdf = root + cell.a["href"]
                pdfs.append(pdf)

            elif cell.find("img", attrs = {"alt": "XBRL"}) != None:
            #xbrlのリンクを格納
                xbrl = cell.a["onclick"]
                components = xbrl.split("\'")
                top = components[7]
                uji_bean = components[3]
                uji_verb = components[1]
                SESSIONKEY = top.split("?")[1]
                file_info = components[5]
                components = [top, "uji.bean="+uji_bean, "uji.verb="+uji_verb, "SESSIONKEY="+SESSIONKEY, file_info]
                xbrl = root + "&".join(components)

                xbrls.append(xbrl)

            elif cell.div != None:
            #edinetコードの格納
                content = str(cell.div.string)
                content = rm_escape(content)

                if re.match("^E", content):
                    code = content
                    code = e_to_s(code)
                    codes.append(code)

                elif cell.div.br != None:
                    content = str(cell.div)
                    content = rm_escape(content)
                    code = content.split(">")[1].replace("/<br/", "")
                    code = e_to_s(code)
                    codes.append(code)

    if len(pdfs) == len(xbrls) and len(titles) == len(codes) and len(pdfs) == len(titles):
        return codes, titles, pdfs, xbrls
    else:
        print(len(codes), len(titles), len(pdfs), len(xbrls))
        return [], [], [], []

def alter_table(query):
    engine = sqlalchemy.create_engine("postgresql://investment_dashboard:hogehoge@sparx-stg.cqt5q7qi5yif.ap-northeast-1.rds.amazonaws.com:5432/investment_dashboard_staging")

    conn = engine.connect()
    result = conn.execute(query)

    return result

def e_to_s(e_code):
    query = "select c.security_code from companies as c inner join code_pair as p on c.security_code = p.security_code where p.edinet_code = '{}';".format(e_code)

    result = alter_table(query)
    count = 0

    for row in result:
        s_code = str(row).replace("(", "").replace(")", "").replace(",", "")
        count += 1;

    if count == 1:
        return s_code
    else:
        return ""

def reports(security_code, target_date, title, path):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    query = "insert into reports(security_code, target_date, title, path, created_at, updated_at) values({0},'{1}','{2}','{3}','{4}','{4}');".format(security_code, target_date, title, path, timestamp)
    alter_table(query)

def get_n(url):
    res = crawler.connect(url)
    soup = BeautifulSoup(res, "lxml")
    kekka = soup.find("p", attrs={"class":"pageLink"}).string
    kekka = rm_escape(str(kekka))
    num = int(kekka.split("件")[0])

    return num

if __name__ == "__main__":
    argv = sys.argv
    argc = len(argv)

    if argc == 4:
        y, m, d = argv[1:4]
    else:
        y, m, d = today()

    target_date = "{}-{}-{}".format(y, m, d)
    url = mkurl(y, m, d)

    for i in range(get_n(url) // 100 + 1):
        url = url.replace("idx=0","idx="+str(i*100))
        codes, titles, pdfs, xbrls = get_components(url)
        for code, title, pdf, xbrl in zip(tqdm(codes), titles, pdfs, xbrls):
            res = crawler.download_file(code, title, pdf, xbrl)

            if res == 0:
                reports(code, target_date, title, "./{0}/{1}/{1}.pdf".format(code, title))
