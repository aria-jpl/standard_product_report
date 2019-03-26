#!/usr/bin/env python

'''
Generates a report for standard products covering the input AOI
'''
import re
import json
import requests
from datetime import datetime
import dateutil.parser
#import gantt
import coverage_chart
import excel
from hysds.celery import app


def main():
    '''
    Determines the proper AOI, queries for relevant products & builds the report.
    '''
    ctx = load_context()
    aoi_id = ctx.get('aoi_id', False)
    aoi_index = ctx.get('aoi_index', False)
    if aoi_id is False or aoi_index is False:
        raise Exception('invalid inputs of aoi_id: {}, aoi_index: {}'.format(aoi_id, aoi_index))
    aoi = get_aoi(aoi_id, aoi_index)
    acqs = sort_by_track(get_objects('acq', aoi))
    slcs = sort_by_track(get_objects('slc', aoi))
    acq_lists = sort_by_track(get_objects('acq-list', aoi))
    ifg_cfgs = sort_by_track(get_objects('ifg-cfg', aoi))
    ifgs = sort_by_track(get_objects('ifg', aoi))
    audit_trail = sort_by_track(get_objects('audit_trail', aoi))

    product_name = 'AOI-ops-report-{}'.format(aoi_id)
    print_results(acqs, slcs, acq_lists, ifg_cfgs, ifgs)
    excel.generate(aoi, acqs, slcs, acq_lists, ifg_cfgs, ifgs, audit_trail)
    
    #attempt to plot a coverage chart by track
    try:
        gen_coverage_plot(ifgs, aoi, 'ifgs')
    except:
        print('failed to generate coverage plot for ifgs')

    try:
        gen_coverage_plot(acq_lists, aoi, 'acq-lists')
    except:
        print('failed to generate coverage plot for acquisition lists')

    #test plot ifgs in a gant chart by track
    #try:
    #    plot_obj(ifgs, aoi, 'ifgs')
    #except:
    #    print('Failed to generate ifg plot')
    #try:
    #    plot_obj(acq_lists, aoi, 'acq_lists')
    #except:
    #    print('Failed to generate acq-list plot')

def print_results(acqs, slcs, acq_lists, ifg_cfgs, ifgs):
    print_object('Acquisitions', acqs)
    print_object('SLCs', slcs)
    print_object('Acquisition-Lists', acq_lists)
    print_object('IFG-CFGs', ifg_cfgs)
    print_object('IFGs', ifgs)

def print_object(name, obj_dct):
    '''prints the count of objects by track'''
    keys = obj_dct.keys()
    print('-----------------------------------------\nResults for: {}'.format(name))
    for track in keys:
        print('Track {} count: {}'.format(track, len(obj_dct.get(track, []))))

def parse_start_end_times(obj):
    '''attempt to parse start end times from file id'''
    reg = '([1-2][0-9]{7})'
    try:
        result = re.findall(reg, obj.get('_id', ''))
        start = int(result[0])
        end = int(result[1])
        if end < start:
            start, end = end, start
        end = dateutil.parser.parse(str(end)[0:4] + '-' + str(end)[4:6] + '-' + str(end)[6:8])
        start = dateutil.parser.parse(str(start)[0:4] + '-' + str(start)[4:6] + '-' + str(start)[6:8])
        return start, end
    except:
        obj_s = obj.get('_source', {})
        st =  dateutil.parser.parse(obj_s.get('starttime', False)).strftime('%Y-%m-%D')
        et =  dateutil.parser.parse(obj_s.get('endtime', False)).strftime('%Y-%m-%D')
        return st, et

def parse_start_time(obj):
    '''gets start time'''
    #obj_s = obj.get('_source', {})
    #st = obj_s.get('starttime', False)
    #if not st is False:
    #return dateutil.parser.parse(st)
    #else:
    return str(parse_start_end_times(obj)[0])

def sort_by_frame(obj_list):
    '''
    Goes through the objects in the result list, and places them in a dict where key is frame
    '''
    sorted_dict = {}
    for result in obj_list:
        frame = result.get('_source', {}).get('metadata', {}).get('frame_id')
        if frame in sorted_dict.keys():
            sorted_dict.get(frame, []).append(result)
        else:
            sorted_dict[frame] = [result]
    return sorted_dict

def plot_obj(es_obj_dict, aoi, product_name):
    aoi_name = aoi.get('_id', 'AOI_err')
    gantt_reg = '{}_{}_track_{}_chart'
    col = get_color()
    for track in es_obj_dict.keys():
        es_obj_list = es_obj_dict.get(track, [])
        title = 'Coverage Report for {} over {}, Track {}'.format(product_name, aoi_name, track)
        gantt_filename = gantt_reg.format(aoi_name, product_name, track)
        chart = gantt.gantt_chart()
        #sort by frame
        es_frame_dict = sort_by_frame(es_obj_list)
        for frame in sorted(es_frame_dict.keys()):
            es_frame_list = es_frame_dict.get(frame, [])
            #print('found {} ifgs for frame {}'.format(len(es_frame_list), frame))
            es_frame_list = sorted(es_frame_list, key=lambda x: parse_start_time(x))
            color = col.next()
            for obj in es_frame_list:
                uid = obj.get('_id')
                obj_name = 'F:{}, S:{}'.format(frame, obj.get('_source', {}).get('starttime', '')[0:10])
                try:
                    startdt, enddt = parse_start_end_times(obj) # attempt to parse from the id dt
                except:
                    startdt = dateutil.parser.parse(obj.get('_source', {}).get('starttime', False))
                    enddt = dateutil.parser.parse(obj.get('_source', {}).get('endtime', False))
                chart.add(startdt, enddt, obj_name, color=color)
        chart.build_gantt(gantt_filename + '.png', title)

def gen_coverage_plot(es_obj_dict, aoi, product_name):
    aoi_name = aoi.get('_id', 'AOI_err')
    fn_reg = '{}_{}_track_{}_coverage-plot'
    color = 'gray'
    for track in es_obj_dict.keys():
        es_obj_list = es_obj_dict.get(track, [])
        title = 'Coverage Plot for {} over {}, Track {}'.format(product_name, aoi_name, track)
        plot_filename = fn_reg.format(aoi_name, product_name, track)
        chart = coverage_chart.coverage_chart()
        #sort by frame
        es_frame_dict = sort_by_frame(es_obj_list)
        for frame in sorted(es_frame_dict.keys()):
            es_frame_list = es_frame_dict.get(frame, [])
            #print('found {} ifgs for frame {}'.format(len(es_frame_list), frame))
            es_frame_list = sorted(es_frame_list, key=lambda x: parse_start_time(x))
            #color = col.next()
            for obj in es_frame_list:
                uid = obj.get('_id')
                obj_name = 'F:{}, S:{}'.format(frame, obj.get('_source', {}).get('starttime', '')[0:10])
                location = obj.get('_source', {}).get('location', {}).get('coordinates', False)[0]
                lat_list = [x[1] for x in location]
                minlat = min(lat_list)
                maxlat = max(lat_list)
                try:
                    startdt, enddt = parse_start_end_times(obj) # attempt to parse from the id dt
                except:
                    startdt = dateutil.parser.parse(obj.get('_source', {}).get('starttime', False))
                    enddt = dateutil.parser.parse(obj.get('_source', {}).get('endtime', False))
                chart.add(startdt, enddt, minlat, maxlat, obj_name, color=color)
        chart.build(plot_filename + '.png', title)

def get_color():
    while True:
        yield 'green'
        yield 'orange'

def get_aoi(aoi_id, aoi_index):
    '''
    retrieves the AOI from ES
    '''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, aoi_index)
    es_query = {"query":{"bool":{"must":[{"term":{"id.raw":aoi_id}}]}}}
    result = query_es(grq_url, es_query)
    if len(result) < 1:
        raise Exception('Found no results for AOI: {}'.format(aoi_id))
    return result[0]

def sort_by_track(es_result_list):
    '''
    Goes through the objects in the result list, and places them in an dict where key is track
    '''
    print('found {} results'.format(len(es_result_list)))
    sorted_dict = {}
    for result in es_result_list:
        track = get_track(result)
        if track in sorted_dict.keys():
            sorted_dict.get(track, []).append(result)
        else:
            sorted_dict[track] = [result]
    return sorted_dict

def get_track(es_obj):
    '''returns the track from the elasticsearch object'''
    es_ds = es_obj.get('_source', {})
    #iterate through ds
    track_met_options = ['track_number', 'track', 'trackNumber', 'track_Number']
    for tkey in track_met_options:
        track = es_ds.get(tkey, False)
        if track:
            return track
    #if that doesn't work try metadata
    es_met = es_ds.get('metadata', {})
    for tkey in track_met_options:
        track = es_met.get(tkey, False)
        if track:
            return track
    raise Exception('unable to find track for: {}'.format(es_obj.get('_id', '')))

def get_objects(object_type, aoi):
    '''returns all objects of the object type ['ifg, acq-list, 'ifg-blacklist'] that intersect both
    temporally and spatially with the aoi'''
    #determine index
    idx_dct = {'audit_trail': 'grq_*_s1-gunw-acqlist-audit_trail', 'ifg':'grq_*_s1-gunw', 'acq-list':'grq_*_s1-gunw-acq-list', 'ifg-cfg': 'grq_*_s1-gunw-ifg-cfg', 'ifg-blacklist':'grq_*_blacklist', 'slc': 'grq_*_s1-iw_slc', 'acq': 'grq_*_acquisition-s1-iw_slc'}
    idx = idx_dct.get(object_type)
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},"filter":{"bool":{"must":[{"range":{"endtime":{"from":starttime}}},{"range":{"starttime":{"to":endtime}}}]}}}},"from":0,"size":1000}
    results = query_es(grq_url, grq_query)
    return results

def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    # make sure the fields from & size are in the es_query
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 10
        es_query['size'] = iterator_size
    if 'from' in es_query.keys():
        from_position = es_query['from']
    else:
        from_position = 0
        es_query['from'] = from_position
    #run the query and iterate until all the results have been returned
    print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    #print('status code: {}'.format(response.status_code))
    #print('response text: {}'.format(response.text))
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        #print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))
    return results_list
    
def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')


if __name__ == '__main__':
    main()

