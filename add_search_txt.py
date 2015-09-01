#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
用来生成Search类的数据，把Post的数据转换成txt格式存储到Search表。
"""

from bs4 import BeautifulSoup
import leancloud
from leancloud import Object, Query, User
import traceback
import config
import re
import time
import HTMLParser
import cgi


leancloud.init(config.CONFIG.APP_ID, master_key=config.CONFIG.APP_MASTER_KEY)


class Db(Object):
    def __getattr__(self, name):
        if name in self._field:
            return self.get(name)
        else:
            return getattr(self._field, name)

    def __setattr__(self, key, value):
        if key in self._field:
            self.set(key, value)
        else:
            self.__dict__[key] = value


class Post(Db):
    _field = (
        'author',
        'title',
        'kind',
        'createdAt',
        'updatedAt',
        'html',
        'brief',
        'ID',
    )


class SiteTagPost(Db):
    _field = (
        'tag_list',
        'site',
        'post',
    )


class Search(Db):
    _field = (
        'kind',
        'post_id',
        'author',
        'post',
        'title',
        'txt',
    )

"""
remove tag
soup = BeautifulSoup('<script>a</script>baba<script>b</script>')
[s.extract() for s in soup('script')]
"""


def remove_img_tags(data):
    p = re.compile(r'<img.*?/>')
    return p.sub('', data)


def html2txt(html):
    if not html:
        return ''
    soup = BeautifulSoup(html)
    text = soup.get_text()
    text = remove_img_tags(text)
    text = cgi.escape(text)
    return text


def create_and_save(post_id, kind, post, ID, author, txt, title=''):
    post_text = Search()
    post_text.set('post_id', post_id)
    post_text.set('kind', kind)
    post_text.set('post', post)
    # post_text.set('ID', ID)    # remove ID field
    post_text.set('author', author)
    post_text.set('title', title)
    post_text.set('txt', txt)
    print 'saving', ID
    post_text.save()


def is_exist(post_id):
    query = Query(Search)
    query.equal_to('post_id', post_id)
    try:
        obj = query.first()
        print 'post_id exist', post_id
        return True
    except:    # not exist
        return False


def solve(res_list):
    for each in res_list:
        print each.get('ID'), each.get('kind')
        kind = each.get('kind')
        post_id = each.id

        if is_exist(post_id):
            print 'skip, exist post_id', post_id

        if kind == 10:    # html
            try:
                ID = each.get('ID')
                post_id = each.id
                query = Query(Post)
                p = query.get(post_id)
                title = each.get('title')
                brief = each.get('brief')
                html_text = html2txt(each.get('html'))
                txt = brief + html_text

                author = each.get('author')
                if not author:
                    owner_id = each.get('owner').id
                    query = Query(User)
                    owner = query.get(owner_id)
                    author = owner.get('username')

                create_and_save(post_id, kind, p, ID, author, txt, title)
            except:
                print traceback.print_exc()
                return

        else:     # txt
            try:
                pid = each.get('post').id
                query = Query(Post)
                p = query.get(pid)
                #title = p.title
                ID = p.get('ID')
                owner_id = each.get('owner').id
                query = Query(User)
                owner = query.get(owner_id)
                author = owner.get('username')
                txt = each.get('txt')
                create_and_save(post_id, kind, each.get('post'), ID, author, txt)
            except:
                print traceback.print_exc()
                return


skip_num = 0
LIMIT_NUM = 500


def get_all_res_list(call_back):
    global skip_num
    query = Query(Post)
    #query.descending('ID')
    query.descending('updatedAt')
    query.skip(skip_num*LIMIT_NUM)
    query.limit(LIMIT_NUM)
    res = query.find()

    call_back(res)

    skip_num += 1
    if len(res) >= LIMIT_NUM:
        time.sleep(1)
        get_all_res_list(call_back)

def print_list(l):
    for i in l:
        print i.get('ID')


def test():
   get_all_res_list(print_list)

def main():
    get_all_res_list(solve)

if __name__ == '__main__':
    main()
    #test()
    print 'finish'
