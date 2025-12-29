#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jijunye
@file: 11.py
@time: 2025/10/30 11:29
@description:
"""
import requests

question = "量子通信概念"
url = 'http://research-industry.slb.ttfund/search'
data = {
    'question': question
}
response = requests.post(url, json=data)
content = response.json()
