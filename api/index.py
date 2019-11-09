# -*- coding: utf-8 -*-
# !/usr/bin/env python
import sys
from werkzeug.wrappers import Response
from flask import Flask, jsonify, request
from peewee import fn

sys.path.append('../')

from config import settings
import model
import json
from util.log import Log

logging = Log()


app = Flask(__name__)


class JsonResponse(Response):

    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (dict, list)):
            response = jsonify(response)

        return super(JsonResponse, cls).force_type(response, environ)


app.response_class = JsonResponse

api_list = {
    'get': u'get an usable proxy',
    # 'refresh': u'refresh proxy pool',
    'get_all': u'get all proxy from proxy pool',
    'delete?proxy=127.0.0.1:8080': u'delete an unable proxy',
    'get_status': u'proxy statistics'
}


@app.route('/')
def index():
    return api_list


@app.route('/get/')
def get():
    speed = request.args.get('speed')
    query =  model.ProxyIp.select().where(model.ProxyIp.status == 1)
    if speed: 
        speed = int(speed)
        query = query.where(model.ProxyIp.speed < speed)
    
    proxy = query.order_by(fn.Rand()).limit(1).first()
        # if ip:
        #     return ip.getProxies()
        # else:
        #     return False
    if proxy: 
        return proxy.getProxies()
        
    return { "code" : 1, "msg": 'no proxy!'}


@app.route('/refresh/')
def refresh():
    # TODO refresh会有守护程序定时执行，由api直接调用性能较差，暂不使用
    # ProxyManager().refresh()
    pass
    return 'success'


@app.route('/get_all/')
def getAll():
    proxies = model.ProxyIp.select().where(model.ProxyIp.status == 1)
    l = []
    for item in proxies:
        l.append(item)
    return l


# @app.route('/delete/', methods=['GET'])
# def delete():
#     proxy = request.args.get('proxy')
#     ProxyManager().delete(proxy)
#     return 'success'


def run():
    app.run(host=settings.API_HOST, port=settings.API_PORT)


if __name__ == '__main__':
    run()
