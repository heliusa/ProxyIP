# -*- coding: utf-8 -*-
import  re

def toHttpUrl(url):
    return re.sub("^(http:\/\/|https:\/\/|\/\/)", 'http://', url)
    

def toHttpsUrl(url):
    return re.sub("^(http:\/\/|https:\/\/|\/\/)(.*)", 'https://', url)

def toUrl(url):
    return re.sub("^(http:\/\/|https:\/\/|\/\/)(.*)", '//', url)