#!/usr/bin/env python

'''
Contains functions for writing Excel files for the Standard Product Report
'''
import re
import pickle
import hashlib
from openpyxl import Workbook
import dateutil.parser

def generate(aoi, acqs, slcs, acq_lists, ifg_cfgs, ifgs):
    '''ingests the various products and stages them by track for generating worksheets'''
    # unique tracks based on acquisition list
    unique_tracks = acq_lists.keys()
    for track in unique_tracks:
        print('generating workbook for track {}'.format(track))
        generate_track(track, aoi, acqs.get(track, []), slcs.get(track, []), acq_lists.get(track, []), ifg_cfgs.get(track, []), ifgs.get(track, []))

def generate_track(track, aoi, acqs, slcs, acq_lists, ifg_cfgs, ifgs):
    '''generates excel sheet for given track, inputs are lists'''
    #stage products
    filename = '{}_T{}.xlsx'.format(aoi.get('_id', 'AOI'), track)
    acq_dct = convert_to_dict(acqs)
    slc_dct = convert_to_dict(slcs)
    acq_list_dct = convert_to_hash_dict(acq_lists)
    ifg_cfg_dct = convert_to_hash_dict(ifg_cfgs)
    ifg_dct = convert_to_hash_dict(ifgs)
    #generate the acquisition sheet
    wb = Workbook()
    ws1 = wb.create_sheet("Products", 0)
    all_missing_slcs = [] # list by starttime
    titlerow = ['Acquisition List ID', 'SLCs Localized?', 'IFG-CFG generated?', 'IFG generated?', 'Missing SLCS?']
    ws1.append(titlerow)
    for hkey in acq_list_dct.keys():
        obj = acq_list_dct.get(hkey)
        acqid = obj.get('_id')
        local_slcs = is_covered(obj, slc_dct) #True/False if SLCs are localized
        missing_acqs_str = ''
        if not local_slcs:
            missing_acq_list = get_missing(obj, slc_dct, acq_dct) # list of starttime keys
            missing_acqs = [acq_dct.get(x, {}).get('_id', 'UNKNOWN') for x in missing_acq_list]
            all_missing_slcs.extend(missing_acqs)
            missing_acqs_str = ' '.join(missing_acqs)
        row = [acqid, is_covered(obj, slc_dct), in_dict(hkey, ifg_cfg_dct), in_dict(hkey, ifg_dct), missing_acqs_str]
        ws1.append(row)
    #generate missing slc list
    ws2 = wb.create_sheet("unlocalized SLCs")
    all_missing_slcs = sorted(list(set(all_missing_slcs)))
    title_row = ['Missing SLC acq id', 'Start Time', 'End Time']
    ws2.append(title_row)
    for st in all_missing_slcs:
        acq = acq_dct.get(st, {})
        acq_id = acq.get('_id', 'UNKNOWN')
        starttime = acq.get('_source', {}).get('starttime', '-')
        endtime = acq.get('_source', {}).get('endtime', '-')
        row = [acq_id, starttime, endtime]
        ws2.append(row)
    wb.save(filename)

def in_dict(hsh, dct):
    '''returns true if the hash input is a key in the input dict'''
    rslt = dct.get(hsh, False)
    if rslt is False:
        return False
    return True

def build_ms_hash(obj):
    '''builds a hash from the starttimes of all the objects in the master & slave lists'''
    master = [parse_from_fn(x) for x in obj.get('_source', {}).get('metadata', {}).get('master_scenes', [])] 
    slave = [parse_from_fn(x) for x in obj.get('_source', {}).get('metadata', {}).get('slave_scenes', [])]
    master = pickle.dumps(sorted(master))
    slave = pickle.dumps(sorted(slave))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())

def convert_to_hash_dict(obj_list):
    '''converts the list into a dict of objects where the keys are a hash of their master & slave scenes'''
    out_dict = {}
    for obj in obj_list:
        hsh = build_ms_hash(obj)
        out_dict[hsh] = obj
    return out_dict

def is_covered(acq_list_obj, slc_dct):
    '''returns True if the SLCs are in slc_dct, False otherwise'''
    master = acq_list_obj.get('_source', {}).get('metadata', {}).get('master_scenes', [])
    slave = acq_list_obj.get('_source', {}).get('metadata', {}).get('slave_scenes', [])
    scenes = list(set(master).union(set(slave)))
    starttimes = [parse_from_fn(x) for x in scenes]
    for st in starttimes:
        if not slc_dct.get(st, False):
            return False
    return True

def get_missing(acq_list_obj, slc_dct, acq_dct):
    '''returns the missing SLCS (as a space delimited string) from the SLC list that are not in the acq-list object'''
    master = acq_list_obj.get('_source', {}).get('metadata', {}).get('master_scenes', [])
    slave = acq_list_obj.get('_source', {}).get('metadata', {}).get('slave_scenes', [])
    scenes = list(set(master).union(set(slave)))
    starttimes = [parse_from_fn(x) for x in scenes]
    out_list = []
    for st in starttimes:
        if not slc_dct.get(st, False):
            out_list.append(st)
    return out_list

def convert_to_dict(input_list):
    '''attempts to convert the input list to a dict where the keys are starttime'''
    out_dict = {}
    for obj in input_list:
        st = parse_start_time(obj)
        out_dict[st] = obj
    return out_dict
    
def parse_start_time(obj):
    '''gets start time'''
    st = obj.get('_source', {}).get('starttime', False)
    return dateutil.parser.parse(st).strftime('%Y-%m-%dT%H:%M:%S')

def parse_from_fn(obj_string):
    '''parses starttime from filename string'''
    reg = '([1-2][0-9]{7}T[0-9]{6})'
    dt = dateutil.parser.parse(re.findall(reg, obj_string)[0])
    return dt.strftime('%Y-%m-%dT%H:%M:%S')




