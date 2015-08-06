#!/usr/bin/env python
# -*- coding:utf-8 -*-

import lxml
import leancloud
import requests
import time
from config import *
from bs4 import BeautifulSoup
from leancloud import Object, Query

leancloud.init(TECH2IPO_APP_ID, master_key=TECH2IPO_APP_MASTER_KEY)
PostShort = Object.extend('PostShort')


def get_html(url):
    return requests.get(url).text


def get_id_title_list(html):
    soup = BeautifulSoup(html, 'lxml')
    li_tag_list = soup.find_all('li')
    res = []
    for i in li_tag_list:
        res.append(dict(post_ID=i.a.get('href'), short_title=i.a.get_text()))
    return res


def exist_file(post_ID):
    query = Query(PostShort)
    query.equal_to('post_ID', int(post_ID))
    try:
        obj = query.first()
        print 'finded', post_ID
        return True
    except:
        print 'not find'
        return False


def upload(id_title_list):
    for each in id_title_list:
        print each
        if not exist_file(each.get('post_ID')):
            time.sleep(1)
            post_short = PostShort()
            post_short.set('post_ID', int(each.get('post_ID', 0)))
            post_short.set('short_title', each.get('short_title', '').strip())
            post_short.save()


def main():
    url = 'http://tech2ipo.com/ucWeb'
    html = get_html(url)
    l = get_id_title_list(html)
    upload(l)


if __name__ == '__main__':
    main()
