#!/usr/bin/env python
#-*- coding: utf-8 -*-

import commands
import requests
import json
import pandas as pd
from Mail_html import send_mail
from texttable import Texttable

ResourceManagerMap = {}
#yarn集群信息
ResourceManagerMap['rm1'] = "resourcemanager16017"
ResourceManagerMap['rm2'] = "resourcemanager16018"
ResourceManagerUrl = "resourcemanager16017"
queue_per = 10
try:
    for name, url in ResourceManagerMap.items():
        result = commands.getstatusoutput('yarn rmadmin -getServiceState %s' % name)
        if result[1] == "active": ResourceManagerUrl = url
except Exception:
    pass

APPLICATION_MASTER = "http://%s:8088/ws/v1/cluster/apps?state=RUNNING" % ResourceManagerUrl
r = requests.get(APPLICATION_MASTER)
html = r.text
str_html = html.encode("utf-8")
#HTML转换为json格式
json_dic = json.loads(str_html)
app = json_dic['apps']['app']
#json格式转化为dataframe格式
df = pd.DataFrame(app)
#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#选取要打印的字段
df_que_per = df[['queueUsagePercentage']]
df_new = df[['id','queue','queueUsagePercentage']]
#统计 ”% of Queue“ 大于 queue_per 的任务
df_que_per2 = df_que_per.loc[df_que_per['queueUsagePercentage'] >= queue_per]
df_per_que_message = df_new.loc[df_new['queueUsagePercentage'] >= queue_per]
tb = Texttable()
#设置对齐方式
tb.set_cols_align(['l','r','r'])
#设置每列的数据类型
tb.set_cols_dtype(['t','i','f'])
#这句是添加标题
tb.header(df_per_que_message.columns.get_values())
#这句是添加数据行。默认会将数据行的第一行作为标题
tb.add_rows(df_per_que_message.values,header=False)
#绘制表格
tb_draw = tb.draw()
#将dataframe格式转化成HTML格式
html_text = df_per_que_message.to_html(index=False,justify='center',border=5)
#将表格内部转换成实线
# html_text2 = html_text.replace('class', 'cellspacing=\"0\" class')
#表格上添加标题
title = "The percent of Queue is over %s ,please check !" % queue_per
html_text3 = html_text.replace('class="dataframe">',
                                'class="dataframe"><caption>{}</caption>'.format(title)
                                )
#将表格内容居中
html_text4 = html_text3.replace('<td>', '<td style="height:40px;" align="center" valian="middle">')
#获取”% of Queue“ 大于queue_per的任务的值
que_per2_list = df_que_per2['queueUsagePercentage'].values.T.tolist()
for que in que_per2_list:
    if que > queue_per:
        arrReceiver = ['xxx@xxx.com']
        send_mail("New Job valaue is too large in %s" % ResourceManagerUrl, html_text4, arrReceiver)
        break
