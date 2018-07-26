#coding:utf-8
import requests
from lxml import etree
import json
import time
from queue import Queue
import threading


class Qiushi(object):
    def __init__(self):
        self.url = 'https://www.qiushibaike.com/8hr/page/{}/'
        self.url_list = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'
        }
        self.file = open('qiushi2.json', 'w')
        # 构建队列
        self.url_queue = Queue()
        self.res_queue = Queue()
        self.data_queue = Queue()

    def generate_url_list(self):
        # self.url_list = [self.url.format(i) for i in range(1, 14)]

        print('正在生成url')
        for i in range(1, 14):
            url = self.url.format(i)
            self.url_queue.put(url)

    def get_data(self):
        while True:
            url = self.url_queue.get()
            print('正在获取{}对应的响应'.format(url))
            res = requests.get(url, headers=self.headers)
            if res.status_code == 200:
                self.res_queue.put(res.content)
            else:
                self.url_queue.put(url)
            self.url_queue.task_done()

    def parse_data(self):
        while True:
            print('正在解析数据')
            data = self.res_queue.get()
            # 创建element对象
            html = etree.HTML(data)

            # 获取所有段子的节点列表
            node_list = html.xpath('//*[contains(@id,"qiushi_tag_")]')
            # print(len(node_list))

            data_list = []
            # 遍历节点列表
            for node in node_list:
                temp = {}
                try:
                    temp['name'] = node.xpath('./div[1]/a[2]/h2/text()')[0].strip()
                    temp['link'] = 'https://www.qiushibaike.com/' + node.xpath('./div[1]/a[2]/@href')[0]
                    temp['age'] = node.xpath('./div[1]/div/text()')[0]
                    temp['gender'] = node.xpath('./div[1]/div/@class')[0].split(' ')[-1].split('I')[0]
                except:
                    temp['name'] = '匿名用户'
                    temp['link'] = None
                    temp['age'] = None
                    temp['gender'] = None
                temp['content'] = node.xpath('./a[1]/div/span/text()')[0].strip()
                data_list.append(temp)

            self.data_queue.put(data_list)
            self.res_queue.task_done()

    def save_data(self):
        while True:
            data_list = self.data_queue.get()
            print('正在保存数据')
            for data in data_list:
                str_data = json.dumps(data, ensure_ascii=False) + ',\n'
                self.file.write(str_data)
            self.data_queue.task_done()

    def __del__(self):
        self.file.close()

    def run(self):
        # url
        # 请求头
        # self.generate_url_list()
        #
        # for url in self.url_list:
        #
        #     # 发送请求，获取响应
        #     data = self.get_data(url)
        #
        #     # 解析数据
        #     data_list = self.parse_data(data)
        #
        #     # 保存
        #     self.save_data(data_list)

        thread_list = []

        # 创建生成url的线程
        t_generate_url = threading.Thread(target=self.generate_url_list)
        thread_list.append(t_generate_url)

        # 创建获取发送请求的线程
        for i in range(3):
            t = threading.Thread(target=self.get_data)
            thread_list.append(t)

        # 创建解析相应的线程
        for i in range(3):
            t = threading.Thread(target=self.parse_data)
            thread_list.append(t)

        # 创建一个保存数据的线程
        t_save_data = threading.Thread(target=self.save_data)
        thread_list.append(t_save_data)

        for t in thread_list:
            # 将子线程设置为守护线程，守护线程跟随主线程的退出而退出，因此子线程可以写成死循环
            t.setDaemon(True)
            t.start()

        for q in [self.url_queue, self.data_queue, self.res_queue]:
            # 让主线程等待所有队列操作完毕之后再退出(主线程关注队列而非子线程是否操作完毕)
            q.join()

        time.sleep(1)




if __name__ == '__main__':
    qiushi = Qiushi()
    qiushi.run()