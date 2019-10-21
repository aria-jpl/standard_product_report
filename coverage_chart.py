#!/usr/bin/env python

'''
Generates a lat band chart from given information
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
import random

class coverage_chart(object):
    def __init__(self):
        self.objects = [] #contains list of tuples: (startime, endtime, title, color)
    
    def add(self, starttime, endtime, minlat, maxlat, uid, color='orange'):
        self.objects.append([starttime, endtime, minlat, maxlat, uid, color])

    def build(self, filename, title):
        '''builds the chart from self.objects'''
        #determine the min and max latitude across all objects
        overall_minlat = min([x[2] for x in self.objects])
        overall_maxlat = max([x[3] for x in self.objects])
        print('overall minmax: {} {}'.format(overall_minlat, overall_maxlat))
        lat_height = overall_maxlat - overall_minlat
        height_multiplier = 2.0
        num = len(self.objects)
        #pos = np.arange(0.5, lat_height, 0.5)
        fig_height = height_multiplier * lat_height
        fig = plt.figure(figsize=(20, fig_height))
        ax = fig.add_subplot(111)
        #ylabels = [float('%.4g' % x) for x in np.arange(overall_minlat, overall_maxlat, 0.5)]
        #ylabels = ylabels.extend(float('%.4g' % overall_maxlat))
        ylabels = np.linspace(overall_minlat, overall_maxlat, num=int(lat_height * height_multiplier), endpoint=True)
        ylabels = [float('%.4g' % x) for x in ylabels]
        pos = [float(x) - overall_minlat for x in ylabels]
        for obj in self.objects:
            starttime, endtime, minlat, maxlat, uid, color = obj
            start = matplotlib.dates.date2num(starttime)
            end = matplotlib.dates.date2num(endtime)
            element_height =  (maxlat - minlat)
            yposition = (minlat - overall_minlat)
            ax.barh(yposition, end - start, left=start, height=element_height, align='edge', edgecolor='darkorange', color=color, alpha = 0.5)
        locsy, labelsy = plt.yticks(pos, ylabels)
        plt.setp(labelsy, fontsize = 14)
        #plt.gca().invert_yaxis()
        ax.set_ylim(ymin = 0.0, ymax = lat_height)
        ax.grid(color = 'g', linestyle = ':')
        ax.xaxis_date()
        #rule = rrulewrapper(WEEKLY, interval=1)
        rule = rrulewrapper(MONTHLY, interval=1)
        loc = RRuleLocator(rule)
        formatter = DateFormatter("%Y-%m-%d")
        ax.xaxis.set_major_locator(loc)
        ax.xaxis.set_major_formatter(formatter)
        labelsx = ax.get_xticklabels()
        plt.setp(labelsx, rotation=30, fontsize=10)
        font = font_manager.FontProperties(size='small')
        ax.legend(loc=1,prop=font)
        #ax.invert_yaxis()
        fig.autofmt_xdate()
        plt.title(title)
        plt.savefig(filename)

if __name__ == '__main__':
    filename = 'test.png'
    title = 'Test Lat Chart'
    dt_reg = 'August {}, 2018'
    print('building test gantt chart and saving to: {}'.format(filename))
    gn = coverage_chart()
    for i in range(10, 20, 1):
        rand = random.random()
        minlat = 10.3 + rand
        maxlat = 12.4  + rand
        startday = parser.parse(dt_reg.format(i))
        endday = parser.parse(dt_reg.format(i+2))
        uid = 'Day {}'.format(i)
        print('adding {} to {}'.format(startday, endday))
        gn.add(startday, endday, minlat, maxlat, uid, 'gray')
    for i in range(10, 20, 1):
        rand = random.random()
        minlat = 12.4 + rand
        maxlat = 14.3  + rand
        startday = parser.parse(dt_reg.format(i))
        endday = parser.parse(dt_reg.format(i+2))
        uid = 'Day {}'.format(i)
        print('adding {} to {}'.format(startday, endday))
        gn.add(startday, endday, minlat, maxlat, uid, 'gray')


    gn.build(filename, title)
