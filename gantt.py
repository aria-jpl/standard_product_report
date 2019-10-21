#!/usr/bin/env python

'''
Generates a gantt chart from given information
'''
from builtins import range
from builtins import object
from dateutil import parser
import datetime as dt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import matplotlib.dates
from matplotlib.dates import DAILY,WEEKLY,MONTHLY, DateFormatter, rrulewrapper, RRuleLocator 
import numpy as np


class gantt_chart(object):
    def __init__(self):
        self.objects = [] #contains list of tuples: (startime, endtime, title, color)
    
    def add(self, starttime, endtime, uid, color='orange'):
        self.objects.append([starttime, endtime, uid, color])

    def build_gantt(self, filename, title):
        '''builds the chart from self.objects'''
        num = len(self.objects)
        pos = np.arange(0.5,num*0.5+0.5,0.5)
        fig_height = 5 + num * 0.25
        fig = plt.figure(figsize=(20,fig_height))
        ax = fig.add_subplot(111)
        ylabels = []
        i = 0
        for obj in self.objects:
            starttime, endtime, uid, color = obj
            start = matplotlib.dates.date2num(starttime)
            end = matplotlib.dates.date2num(endtime)
            ylabels.append(uid)
            ax.barh((i*0.5)+0.5, end - start, left=start, height=0.3, align='center', edgecolor='darkorange', color=color, alpha = 0.8)
            i+=1
        locsy, labelsy = plt.yticks(pos, ylabels)
        plt.setp(labelsy, fontsize = 14)
        ax.set_ylim(ymin = -0.1, ymax = num*0.5+0.5)
        ax.grid(color = 'g', linestyle = ':')
        ax.xaxis_date()
        #rule = rrulewrapper(WEEKLY, interval=1)
        rule = rrulewrapper(WEEKLY, interval=1)
        loc = RRuleLocator(rule)
        formatter = DateFormatter("%d-%b")
        ax.xaxis.set_major_locator(loc)
        ax.xaxis.set_major_formatter(formatter)
        labelsx = ax.get_xticklabels()
        plt.setp(labelsx, rotation=30, fontsize=10)
        font = font_manager.FontProperties(size='small')
        ax.legend(loc=1,prop=font)
        ax.invert_yaxis()
        fig.autofmt_xdate()
        plt.title(title)
        plt.savefig(filename)

if __name__ == '__main__':
    filename = 'test.png'
    title = 'Test Gantt Chart'
    dt_reg = 'August {}, 2018'
    print('building test gantt chart and saving to: {}'.format(filename))
    gn = gantt_chart()
    for i in range(10, 20, 2): 
        startday = parser.parse(dt_reg.format(i))
        endday = parser.parse(dt_reg.format(i+2))
        uid = 'Day {}'.format(i)
        print('adding {} to {}'.format(startday, endday))
        gn.add(startday, endday, uid)
    gn.build_gantt(filename, title)
