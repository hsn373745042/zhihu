import requests
import random
import re
import time
from bs4 import BeautifulSoup
import threading
import mysql.connector

# 创建数据库、数据表
def my_data():
    # 连接数据库主机
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'admin'
    )
    mycursor = mydb.cursor()
    # 执行命令创建zhihu_db数据库
    mycursor.execute('create database zhihu_db')
    mydb = mysql.connector.connect(
        host = 'localhost',
        user = 'root',
        password = 'admin',
        database = 'zhihu_db'
    )
    mycursor = mydb.cursor()
    # 执行命令：创建名为urls的数据表,字段为url
    mycursor.execute('create table urls (url VARCHAR(255)) character set utf8')
    # 执行命令：创建名为sites的数据表，字段为topic,title和url
    mycursor.execute('create table sites (topic VARCHAR(255),title VARCHAR(255),url VARCHAR(255)) character set utf8')
    # 执行命令：给数据库zhihu_db里的数据表sites添加名为id的主键
    mycursor.execute('alter table sites add column id int auto_increment primary key')

# 定义存储url中id的函数
def add_id(url):
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='zhihu_db'
    )
    mycursor = mydb.cursor()
    sql = 'insert into urls (url) value (%s)'
    val = (url,)
    mycursor.execute(sql,val)
    mydb.commit()

# 创建ua池
def ua_pond():
    #ua = UserAgent(verify_ssl=False)
    #return ua.random    # 大量低版本浏览器无法上知乎
    ua = ['Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36',
          'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36',
          'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36',
          'Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36',
          'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
          'Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36',
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36',
          'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36'
          ]
    return random.sample(ua,1)[0]

# 爬取所有父话题的名字和url中的id
def f_crawler():
    basic_url = 'https://www.zhihu.com/topics'
    header = {'User-Agent':ua_pond()}
    res = requests.get(basic_url,headers=header)
    soup = BeautifulSoup(res.text,'html.parser')
    f_topic = soup.find_all(class_='zm-topic-cat-item')
    # 33类父话题
    ff = []
    for f in f_topic:
        f_id = f['data-id']
        f_name = f.text
        #构造父话题url
        url = basic_url+'#'+f_name
        ff.append((url,f_id))
    return ff

# 多进程爬取各类话题下的子话题url中的id，并去重
class my_threading(threading.Thread):
    def __init__(self,threadID,name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        print('开始线程:%s' % self.name)
        s_crawler(self.name)
        print('结束线程:%s' % self.name)

def s_crawler(threadName):
    global ff,sid
    while len(ff):
        try:
            f = ff.pop(0)
            print("%s 开始爬子话题 %s %s" % (threadName,f[1],time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())))
            headers = {'User-Agent': ua_pond()}
            res1 = requests.get(f[0],headers=headers)
            soup1 = BeautifulSoup(res1.text, 'html.parser')
            s_topic = soup1.find(class_='zh-general-list clearfix').find_all(class_='blk')
            # 子话题
            for s in s_topic:
                s_id = re.findall('.*?(\d.*\d)',s.find('a')['href'])
                sid.append(s_id[0])
            # 爬取更多子话题
            headers1 = {
                'origin': 'https://www.zhihu.com',
                'referer': 'https://www.zhihu.com/topics',
                'user-agent': ua_pond()
                }
            data = {
                'method': 'next',
                'params': '{"topic_id":'+f[1]+',"offset":0,"hash_id":""}'
                }
            res2 = requests.post('https://www.zhihu.com/node/TopicsPlazzaListV2',headers=headers1,data=data)
            soup2 = res2.json()
            m = 0
            for s in soup2['msg']:
                s_id = re.findall('href="/topic/(\d.*\d)">',s)
                sid.append(s_id[0])
                m += 1
            # 如果还有子话题，继续爬取；否则，停止。
            while len(str(soup1.find(class_='zg-btn-white zu-button-more'))) != 0:
                data = {
                'method': 'next',
                'params': '{"topic_id":'+f[1]+',"offset":'+str(m)+',"hash_id":""}'
                }
                res2 = requests.post('https://www.zhihu.com/node/TopicsPlazzaListV2',headers=headers1,data=data)
                if res2.json()['msg'] == []:
                    break
                else:
                    for s in res2.json()['msg']:
                        s_id = re.findall('href="/topic/(\d.*\d)">', s)
                        sid.append(s_id[0])
                        m += 1
        except IndexError:
            pass

# 多进程爬取每个子话题里的父、子话题id，并去重
class my_threadings(threading.Thread):
    def __init__(self,threadID,name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        print('开始线程:%s' % self.name)
        t_crawler(self.name)
        print('结束线程:%s' % self.name)

def t_crawler(threadName):
    global list_id,list_ids
    while len(list_id):
        try:
            i = list_id.pop(0)
            print("%s 开始爬子话题 %s %s" % (threadName,str(i),time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            url = 'https://www.zhihu.com/api/v3/topics/'+str(i)+'/children'
            headers = {'User-Agent': ua_pond()}
            r = requests.get(url,headers=headers)
            soup = r.json()
            for i in soup['data']:
                d = i['id']
                list_ids.append(d)
        except IndexError:
            pass

if __name__ == '__main__':
    my_data()
    print('数据库已创建！')
    print('开始爬取33类父话题')

    ff = f_crawler()
    sid = []

    print('开始多进程爬取各类话题下的子话题url中的id，并去重。')
    # 创建新线程
    thread0 = my_threading(0, '香菜')
    thread1 = my_threading(1, '爱衣')
    thread2 = my_threading(2, '真礼')
    thread3 = my_threading(3, '麻沙美')
    thread4 = my_threading(4, '好美')
    thread5 = my_threading(5, '界人')
    thread6 = my_threading(6, '武人')
    thread7 = my_threading(7, '绿川光')
    thread8 = my_threading(8, '福山润')
    thread9 = my_threading(9, '真野')

    # 开启新线程
    start = time.time()

    thread0.start()
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()
    thread9.start()

    # 等待至线程中止
    thread0.join()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    thread8.join()
    thread9.join()
    print('退出主线程')

    less_id = set(sid)
    print('第一波去重完毕！总耗时%s秒' % (time.time()-start))

    # 把set类型的数据转换成list类型
    list_id = list(less_id)  # 用来读取，读一个删一个，防止各线程重复爬取
    li = list(less_id) # 保存之前的id
    list_ids = []  # 用来添加新话题id，最后去重

    print('开始多进程爬取每个子话题里的父、子话题id，并去重。')
    # 创建新线程
    thread0 = my_threadings(0, '香菜')
    thread1 = my_threadings(1, '爱衣')
    thread2 = my_threadings(2, '真礼')
    thread3 = my_threadings(3, '麻沙美')
    thread4 = my_threadings(4, '好美')
    thread5 = my_threadings(5, '界人')
    thread6 = my_threadings(6, '武人')
    thread7 = my_threadings(7, '绿川光')
    thread8 = my_threadings(8, '福山润')
    thread9 = my_threadings(9, '真野')

    # 开启新线程
    start1 = time.time()

    thread0.start()
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()
    thread7.start()
    thread8.start()
    thread9.start()

    # 等待至线程中止
    thread0.join()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()
    thread6.join()
    thread7.join()
    thread8.join()
    thread9.join()
    print('退出主线程')

    more_id = set(li + list_ids)
    print('第二波去重完毕！总耗时%s秒' % (time.time() - start1))
    print('开始储存数据')
    start2 = time.time()
    for i in more_id:
        add_id(i)
    print('数据存储完毕！总耗时%s秒' % (time.time() - start2))