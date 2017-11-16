#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import orm
from models import User, Blog, Comment
def test():
    yield from orm.create_pool(user='root', password='yzm123456', database='awesome')

    u = User(name = 'Test', email='holaus@sina.com', passwd='123456789', image='about:blank')
    yield from u.save()
    for x in test():
        pass
