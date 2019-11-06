# -*- coding: utf-8 -*-

# @Author: yooongchun
# @File: ProxyIP.py
# @Time: 2018/6/19
# @Contact: yooongchun@foxmail.com
# @blog: https://blog.csdn.net/zyc121561
# @Description: 抓取代理IP

import requests
import re
import random
import time
from bs4 import BeautifulSoup
import threading
from multiprocessing import Process
from database import IP_Pool
from UA import FakeUserAgent
import logging
import model
from macpath import split
import datetime
import json

class ProxyItem(object):
    def __init__(self, channel, url):
        self.channel = channel
        self.url = url

class Crawl(object):
    '''抓取网页内容获取IP地址保存到数据库中'''

    def __init__(self,
                 retry_times=10,
                 proxy_flag=False,
                 proxy_database=None,
                 proxy_table=None,
                 database_name="ALL_IP.db"):
        self.__URLs = self.__URL()  # 访问的URL列表
        self.__ALL_IP_TABLE_NAME = "all_ip_table"  # 数据库表名称
        self.__DATABASE_NAME = database_name  # 数据库名称
        self.__RETRY_TIMES = retry_times  # 数据库访问重试次数
        self.__PROXIES_FLAG = proxy_flag  # 是否使用代理访问
        self.__PROXY_DATABASE = proxy_database  # 代理访问的IP数据库
        self.__PROXY_TABLE = proxy_table  # 代理访问的IP数据库表

   
    def __URL(self):
        '''
        返回URL列表
        URL获取地址：
        1.西刺网站
        2.快代理网站
        3.66代理网站
        4.89代理网站
        5.秘密代理网站
        6.data5u网站
        7.免费代理IP网站
        8.云代理网站
        9.全网代理网站
        '''
        URL = []
        url_xici = [
            ProxyItem('xici', "http://www.xicidaili.com/nn/%d" % (index + 1))
            for index in range(3275)
        ]
        url_kuaidaili = [
            ProxyItem('kuaidaili', "https://www.kuaidaili.com/free/inha/%d" % (index + 1))
            for index in range(3127)
        ]
        url_66 = [
            ProxyItem('66', "http://www.66ip.cn/%d.html" % (index + 1)) 
            for index in range(1288)
        ]
        url_89 = [
            ProxyItem('89',"http://www.89ip.cn/index_%d.html" % (index + 1))
            for index in range(31)
        ]
        # url_mimi = [
        #     ProxyItem('mimi',"http://www.mimiip.com/gngao/%d" % (index + 1))
        #     for index in range(683)
        # ]
        url_data5u = [
            ProxyItem('data5u', 'http://www.data5u.com')
        ]
        url_yqie = [
            ProxyItem('yqie', 'http://ip.yqie.com/ipproxy.htm')
        ]
        url_yundaili = [
            ProxyItem('yundaili', 'http://www.ip3366.net/?stype=1&page=%d' % (index + 1))
            for index in range(10)
        ]
        url_quanwangdaili = [
            ProxyItem('quanwangdaili', 'http://www.goubanjia.com/')
        ]

        URL = url_xici + url_kuaidaili + url_66 + url_89  \
            + url_data5u + url_yqie + url_yundaili + url_quanwangdaili
        random.shuffle(URL)  # 随机打乱
        return URL

    def __proxies(self):
        '''构造代理IP,需要提供代理IP保存的数据库名称和表名'''
        item = model.ProxyIp.select().where(model.ProxyIp.status == 1).order_by('random()').limit(1).get()
        if item:
            IP = str(item.ip) + ":" + str(item.port)
            return {"http": str(item.source_protocol) + "://" + IP}
        else:
            return False

    def __crawl(self, url, headers, proxies=False, re_conn_times=3):
        '''爬取url'''
        for cnt in range(re_conn_times):
            try:
                if not proxies:
                    response = requests.get(
                        url=url, headers=headers, timeout=5)
                else:
                    response = requests.get(
                        url=url, headers=headers, proxies=proxies, timeout=5)
                break
            except Exception:
                response = None
                continue
        if response is None:
            logging.error(u"ProxyIP-Crawl:请求url出错：%s" % url)
            return None
        try:
            html = response.content.decode("utf-8")
            return html
        except Exception:
            logging.error(u"ProxyIP-Crawl:HTML UTF-8解码失败，尝试GBK解码...")
        try:
            html = response.content.decode("gbk")
            return html
        except Exception:
            logging.error(u"ProxyIP-Crawl:HTML解码出错，跳过！")
            return None

    def __parse(self, channel, html):
        '''解析HTML获取IP地址'''
        if html is None:
            return
        all_ip = []
        soup = BeautifulSoup(html, "lxml")
    
        if channel == 'xici' or channel == 'kuaidaili' or channel == '66' or channel == '89' or channel == 'yqie' or channel == 'yundaili' :
            tds = soup.find_all("td")
            for index, td in enumerate(tds):
                logging.debug(u"ProxyIP-Crawl:页面处理进度：{}/{}".format(index + 1, len(tds)))

                fields = {
                    'xici': {
                        'ip' : 0,
                        'port': 1,
                        'address': 2,
                        'source_type': 3,
                        'source_protocol': 4
                    },
                    'kuaidaili': {
                        'ip' : 0,
                        'port': 1,
                        'address': 4,
                        'source_type': 2,
                        'source_protocol': 3
                    },
                    '66' : {
                        'ip' : 0,
                        'port': 1,
                        'address': 2,
                        'source_type': 3
                    },
                    '89':  {
                        'ip' : 0,
                        'port': 1,
                        'address': 2,
                    },
                    'yqie':  {
                        'ip' : 0,
                        'port': 1,
                        'address': 2,
                        'source_type': 3,
                        'source_protocol': 4
                    },
                    'yundaili': {
                        'ip' : 0,
                        'port': 1,
                        'address': 5,
                        'source_type': 2,
                        'source_protocol': 3
                    },
                }
                if re.match(r"^\d+\.\d+\.\d+\.\d+$", re.sub(r"\s+|\n+|\t+", "", td.text)):
                    item = {}
                    for key in fields[channel]:
                        pos = fields[channel][key]
                        item.update({key: re.sub(r"\s+|\n+|\t+", "", tds[index + pos].text)})
                    item.update({u'channel': channel})
                    all_ip.append(item)
                else:
                    logging.debug(u"不匹配的项！")
        elif channel == 'data5u':
            wlist = soup.find("div", {'class': 'wlist'})
            if wlist:
                tds = wlist.find_all('ul')
                for index, td in enumerate(tds):
                    logging.debug(u"ProxyIP-Crawl:页面处理进度：{}/{}".format(index + 1, len(tds)))
                    if re.match(r"^\d+\.\d+\.\d+\.\d+$",
                                re.sub(r"\s+|\n+|\t+", "", td.text)):
                        item = {}
                        item.update({u'ip': re.sub(r"\s+|\n+|\t+", "", td.text)})
                        item.update({u'port': re.sub(r"\s+|\n+|\t+", "", tds[index + 1].text)})
                        item.update({u'address': re.sub(r"\s+|\n+|\t+", "", tds[index + 5].text)})
                        item.update({u'source_type': re.sub(r"\s+|\n+|\t+", "", tds[index + 2].text)})
                        item.update({u'source_protocol': re.sub(r"\s+|\n+|\t+", "", tds[index + 3].text)})
                        item.update({u'country': re.sub(r"\s+|\n+|\t+", "", tds[index + 4].text)})
                        item.update({u'channel': channel})
                        all_ip.append(item)
                    else:
                        logging.debug(u"不匹配的项！")
        elif channel == 'quanwangdaili':
            tds = soup.find('table').find_all("td")
            for index, td in enumerate(tds):

                logging.debug(u"ProxyIP-Crawl:页面处理进度：{}/{}".format(index + 1, len(tds)))
                if re.match(r"^(\d+\.+){3,}(\d+\.*){1,}\:(\d+)",
                            re.sub(r"\s+|\n+|\t+", "", td.text)):
                    item = {}
                    ip = ''
                    for span in td.find_all(['span', 'div']):
                        if not span.has_attr('class'):
                            ip = ip + str(span.get_text())

                    if not re.match(r"^\d+\.\d+\.\d+\.\d+$",  ip):
                        continue

                    port = td.find('span', {'class': 'port'})
                    portClass = port.get('class')
                    if len(portClass) <2:
                        continue

                    realEncrypt = portClass[1]
                    encryptIndex = []
                    for i in range(len(realEncrypt)):
                        encryptIndex.append(str('ABCDEFGHIZ'.index(realEncrypt[i])))
                    realPort = int(''.join(encryptIndex)) >> 0x3
                    logging.info(realPort)

                    item.update({u'ip': re.sub(r"\s+|\n+|\t+", "", ip)})
                    item.update({u'port': re.sub(r"\s+|\n+|\t+", "", str(realPort))})
                    item.update({u'address': re.sub(r"\s+|\n+|\t+", "", tds[index + 3].text)})
                    item.update({u'source_type': re.sub(r"\s+|\n+|\t+", "", tds[index + 1].text)})
                    item.update({u'source_protocol': re.sub(r"\s+|\n+|\t+", "",  str.upper(tds[index + 2].get_text().strip().encode('utf-8')))})
                    item.update({u'channel': channel})
                    all_ip.append(item)
                else:
                    logging.debug(u"不匹配的项！")
        return all_ip

    def crawl(self, proxyItem, headers, proxies):
        '''抓取IP并保存'''
        html = self.__crawl(proxyItem.url, headers, proxies=proxies)
        if html is None:
            return
        ipList = self.__parse(proxyItem.channel, html)
        if ipList is None or len(ipList) < 1:
            return
        
        for ip in ipList:
            #logging.info(ip)
            try:
                model.ProxyIp.insert(**ip).upsert().execute()
            except:
                logging.error("crawl save error: " + proxyItem.url)
                logging.error(ip)
        # if len(ipList) == 1:
        #     model.ProxyIp.insert(**ipList).upsert().execute()
        # else:
        #     with model.database.atomic():
        #         model.ProxyIp.insert_many(ipList).upsert().execute()

    def multiple_crawl(self, thread_num=10, sleep_time=15 * 60):
        '''多线程抓取'''
        count = 0
        while True:
            count += 1
            logging.info(u"ProxyIP-Crawl:开始第{}轮抓取,当前url数：{}".format(count, len(self.__URLs)))
            cnt = 0
            st = time.time()
            while cnt < len(self.__URLs):
                pool = []
                if self.__PROXIES_FLAG:
                    proxies = self.__proxies()
                else:
                    proxies = False
                for i in range(thread_num):
                    if cnt >= len(self.__URLs):
                        break
                    url = self.__URLs[cnt]
                    headers = FakeUserAgent().random_headers()
                    th = threading.Thread(
                        target=self.crawl, args=(url, headers, proxies))
                    th.setDaemon(True)
                    pool.append(th)
                    logging.info(
                        u"ProxyIP-Crawl:抓取URL：{}\t 进度：{}/{}\t{:.2f}%".format(
                            url.url, cnt + 1, len(self.__URLs),
                            (cnt + 1) / float(len(self.__URLs)) * 100))
                    th.start()
                    time.sleep(2 * random.random())  # 随机休眠，均值：0.5秒
                    cnt += 1
                for th in pool:
                    th.join()
            ed = time.time()
            logging.info(
                u"ProxyIP-Crawl:第{}轮抓取完成,耗时：{:.2f}秒". format(count, ed - st))
            st = time.time()
            while time.time() - st < sleep_time:
                logging.info(
                    u"ProxyIP-Crawl:休眠等待：{:.2f}秒".format(sleep_time -
                                                         time.time() + st))
                time.sleep(5)

    def run(self):
        self.multiple_crawl(3, 60 * 60 * 3)


class Validation(object):
    '''校验IP有效性'''

    def __init__(self):
        self.__URL = "http://106.14.179.179:23005/get"
        self.__RETRY_TIMES = 10  # 数据库访问重试次数

    def __check_ip_anonumous(self, ip):
        '''检验IP是否高匿名'''
        IP = ip.getProxyIP()
        logging.info(u"ProxyIP-Validation:校验IP是否高匿：{}".format(IP))
        if "高匿" in str(ip.type):
            return True
        else:
            logging.debug(u"ProxyIP-Validation:非高匿IP：{}".format(IP))
            return False

    def __check_ip_validation(self, ip, metadata = {}, moduleName = 'ProxyIP-Validation', is_filter = False):
        '''校验IP地址有效性'''
        try:
            IP = ip.getProxyIP()
        except Exception:
            logging.info(u"{}:IP地址格式不正确!".format(moduleName))
            return False
        
        if is_filter:
            re_conn_time = 2
        else:
            re_conn_time = 5

        logging.info(u"{}:校验IP地址有效性：{}".format(moduleName, IP))
        proxies = ip.getProxies()
        headers = FakeUserAgent().random_headers()
        for i in range(re_conn_time):
            try:
                response = requests.get(
                    url=self.__URL,
                    proxies=proxies,
                    headers=headers,
                    timeout=5)

                try:
                    if response.status_code != 200:
                        response = None
                        continue

                    content = json.loads(response.content)
                    if content['headers'].has_key('Cdn-Src-Ip'):
                        metadata.update({u'type': '普通'}) #还不知道怎么判断透明和普通匿名代理
                    else:
                        metadata.update({u'type': '高匿'})

                    metadata.update({u'speed': str(int(response.elapsed.total_seconds() * 1000))})

                except Exception, e1:
                    logging.info(e1)
                    continue
                break
            except Exception, ex:
                logging.info(ex)
                response = None
                continue
        if response is None:
            logging.error(u"{}:请求校验IP的网络错误！".format(moduleName))
            return False
        return True

    def __validation(self, ip, is_filter, moduleName):
        '''校验有效IP池中的IP有效性，无效则删除'''
        now =  time.strftime('%Y-%m-%d %H:%M:%S')
        metadata = {}
        if not self.__check_ip_validation(ip, metadata, moduleName, is_filter):
            if is_filter:
                ip.status = 2
            else:
                if ip.fail < 50:
                    ip.status = 3
                else:
                    ip.status = 2

            ip.validdate = now
            ip.die_time = now
            ip.fail = ip.fail + 1
            ip.updated_at = now
            ip.save()
        else:
            for k in metadata:
                setattr(ip, k, metadata[k])
           
            ip.status = 1
            ip.validdate = now
            ip.die_time = None
            if ip.alive_first == None:
                ip.alive_first = now
            ip.alive_near = now
            ip.fail = 0
            ip.updated_at = now
            ip.save()

    def multiple_validation(self, is_filter = False, thread_num=10, sleep=15 * 60):
        '''定时校验有效IP池里的IP，无效的删除'''
        logging.info("is_filter:" + str(is_filter))
        count = 0
        while True:
            if is_filter:
                moduleName = 'ProxyIP-Filter'
                IPs = model.ProxyIp.select().where(model.ProxyIp.status == 0)
            else:
                moduleName = 'ProxyIP-Validation'
                IPs = model.ProxyIp.select().where(model.ProxyIp.status == 1 or (model.ProxyIp.status == 3 and model.ProxyIp.fail < 50))
                
            count += 1
            logging.info(u"{}:第{}次校验，IP数：{}".format(
                moduleName, count, len(IPs)))
            cnt = 0
            while cnt < len(IPs):
                pool = []
                for i in range(thread_num):
                    if cnt >= len(IPs):
                        break
                    logging.info(
                        u"{}:校验进度：{}/{}\t{:.2f}%".format(
                            moduleName,cnt, len(IPs), cnt / float(len(IPs)) * 100))
                    th = threading.Thread(
                        target=self.__validation, args=(IPs[cnt], is_filter, moduleName))
                    th.setDaemon(True)
                    th.start()
                    time.sleep(2 * random.random())
                    pool.append(th)
                    cnt += 1
                for th in pool:
                    th.join()
            ips = model.ProxyIp.select().where(model.ProxyIp.status == 1)
            logging.info(u"{}:完成第{}次校验，当前有效IP数：{}".format(
                moduleName, count, len(ips)))
            logging.info(u"{}:进入休眠：{} 秒".format(moduleName, sleep))
            time.sleep(sleep)

    def run(self):
        '''启动程序'''
        p1 = Process(target=self.multiple_validation, kwargs={'is_filter': True})
        p2 = Process(target=self.multiple_validation, kwargs={'is_filter': False, 'sleep': 60})
        p1.start()
        p2.start()
        p1.join()
        p2.join()


def main():
    # 初始化
    crawl = Crawl()
    validation = Validation()
  
    p2 = Process(target=validation.run)
    p1 = Process(target=crawl.run)

    p2.start()
    p1.start()
    p2.join()
    p1.join()


if __name__ == "__main__":
    # Crawl().run()
    # Validation().run()
    main()
