import random
import time
from multiprocessing import Process,Queue
import mysql.connector
import requests
from bs4 import BeautifulSoup
import re
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

def my_proxy():
    proxies = []
    url = 'http://webapi.http.zhimacangku.com/getip?num=10&type=2&pro=0&city=0&yys=100026&port=11&time=4&ts=1&ys=0&cs=0&lb=1&sb=0&pb=4&mr=2&regions=110000,130000,140000,320000,330000,340000,410000,420000,530000,610000'
    res = requests.get(url)
    soup = res.json()['data']
    for i in soup:
        proxies.append('%s:%s' % (i['ip'],i['port']))
    return proxies

# 创建ip代理池
def proxy_pond(n):
    proxies = []
    ip = random.choice(proxies[int(n)])
    proxy = {'http':'http://'+ip,'https':'https://'+ip}
    return proxy

def read_id():
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='zhihu_db'
    )
    mycursor = mydb.cursor()
    mycursor.execute('select * from urls')
    myresult = mycursor.fetchone()
    return myresult

def del_id(l):
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='zhihu_db'
    )
    mycursor = mydb.cursor()
    sql = 'delete from urls where url = %s'
    mycursor.execute(sql,l)
    mydb.commit()

# 定义存储数据函数
def add_data(topic,title,url):
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='admin',
        database='zhihu_db'
    )
    mycursor = mydb.cursor()
    sql = 'insert into sites (topic,title,url) value (%s,%s,%s)'
    val = (topic,title,url)
    mycursor.execute(sql,val)
    # 数据表内容有更新，必须使用到该语句
    mydb.commit()

# 定义生产者
def producer(i,q):
    for x in range(i):
        id = read_id()
        del_id(id)
        # 构造所有话题的url
        url = 'https://www.zhihu.com/api/v4/topics/'+id[0]+'/feeds/top_activity?include=data%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Danswer%29%5D.target.content%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Danswer%29%5D.target.is_normal%2Ccomment_count%2Cvoteup_count%2Ccontent%2Crelevant_info%2Cexcerpt.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Darticle%29%5D.target.content%2Cvoteup_count%2Ccomment_count%2Cvoting%2Cauthor.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dtopic_sticky_module%29%5D.target.data%5B%3F%28target.type%3Dpeople%29%5D.target.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F%28target.type%3Danswer%29%5D.target.author.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Darticle%29%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Cauthor.badge%5B%3F%28type%3Dbest_answerer%29%5D.topics%3Bdata%5B%3F%28target.type%3Dquestion%29%5D.target.annotation_detail%2Ccomment_count%3B&limit=5'
        l = 'https://www.zhihu.com/topic/'+id[0]+'/hot'
        q.put((url,l))
        print('生产者生产了产品 %s' % (id[0]))

# 定义消费者
def consumer(q,n):
    while True:
        rq = q.get()
        # 队列为空结束循环
        if rq is None:
            break
        else:
            headers = {'user-agent': ua_pond()}
            # 提取话题名称
            rl = requests.get(rq[1],headers=headers,proxies=proxy_pond(n),timeout=60)
            rl.encoding = 'utf-8'
            res = BeautifulSoup(rl.text,'html.parser')
            if res.find(class_='TopicMetaCard-title') != None:
                topic = res.find(class_='TopicMetaCard-title').text
            elif res.find(class_='TopicCard-titleText') != None:
                topic = res.find(class_='TopicCard-titleText').text
            # 找不到话题的比较奇怪，直接跳过找下一个话题
            else:
                continue
            print("%s号 开始爬话题 %s %s" % (n,topic,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            # 提取话题里的问题
            r = requests.get(rq[0],headers=headers,proxies=proxy_pond(n),timeout=60)
            soup = r.json()
            for i in soup['data']:
                # 专题的title存放路径不一样
                try:
                    title = i['target']['question']['title']
                    question = 'question/' + re.findall('s/.*?(\d.*\d)', i['target']['question']['url'])[0]
                    answer = '/answer/' + re.findall('s/.*?(\d.*\d)', i['target']['url'])[0]
                    # 构造回答的url
                    url = 'https://www.zhihu.com/' + question + answer
                    # 将标题和url存储到数据库
                    try:
                        add_data(topic,title,url)
                    # 会遇到带有mysql无法识别的字符的title，只能跳过
                    except:
                        pass
                # 专题的title存放路径不一样
                except:
                    title = i['target']['title']
                    # 会遇到带有mysql无法识别的字符的title，只能跳过
                    try:
                        id = i['target']['id']
                        url = 'https://zhuanlan.zhihu.com/p/' + str(id)
                        add_data(topic, title, url)
                    except:
                        pass
            # 部分页面没有soup['paging']
            try:
                pa = soup['paging']['is_end']
                while pa == False:
                    # 如果请求过快，等5秒再请求
                    try:
                        url = soup['paging']['next']
                        res = requests.get(url,headers=headers,proxies=proxy_pond(n),timeout=60)
                        soup = res.json()
                        pa = soup['paging']['is_end']
                        for i in soup['data']:
                            # 专题的title路径不同
                            try:
                                title = i['target']['question']['title']
                                question = 'question/' + re.findall('s/.*?(\d.*\d)', i['target']['question']['url'])[0]
                                answer = '/answer/' + re.findall('s/.*?(\d.*\d)', i['target']['url'])[0]
                                # 构造回答的url
                                url = 'https://www.zhihu.com/' + question + answer
                                # 将标题和url存储到数据库
                                try:
                                    add_data(topic,title,url)
                                # 会遇到带有mysql无法识别的字符的title，只能跳过
                                except:
                                    pass
                            # 专题的title路径不同
                            except:
                                title = i['target']['title']
                                try:
                                    id = i['target']['id']
                                    url = 'https://zhuanlan.zhihu.com/p/' + str(id)
                                    add_data(topic, title, url)
                                # 会遇到带有mysql无法识别的字符的title，只能跳过
                                except:
                                    pass
                    except:
                        time.sleep(5)
                        continue
            # 遇到没有问题的话题直接跳过
            except:
                pass
        print("%s号 结束爬话题 %s %s" % (n, topic, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

if __name__ == '__main__':

    start = time.time()
    # 创建队列
    q = Queue()
    # 创建4个进程
    my_process = {}
    for i in range(5):
        my_process[str(i)] = Process(target=consumer,args=(q,str(i)))
    # 开启4个进程
    for i in range(5):
        my_process[str(i)].start()
    # 调用生产者
    producer(5,q)
    # 将4个None放入队列，迫使阻塞的进程结束
    for i in range(5):
        q.put(None)
    # 主线程等待子进程结束
    for i in range(5):
        my_process[str(i)].join()

    print('爬取任务结束！',time.time()-start)