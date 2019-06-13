#!/usr/bin/env python

"""Generates the Standard Product Ops Report"""
from __future__ import print_function
import re
import os
import json
import shutil
import urllib3
import hashlib
import datetime
import requests
# from openpyxl import Workbook
import dateutil.parser
from hysds.celery import app

from pprint import pprint

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

VERSION = 'v2.0'
PRODUCT_NAME = 'AOI_Ops_Report-{}-TN{}-{}-{}'

# maps the dataset type to the elasticsearch index
IDX_DCT = {
    'audit_trail': 'grq_*_s1-gunw-acqlist-audit_trail',
    'ifg': 'grq_*_s1-gunw',
    'acq-list': 'grq_*_s1-gunw-acq-list',
    'ifg-cfg': 'grq_*_s1-gunw-ifg-cfg',
    'ifg-blacklist': 'grq_*_blacklist',
    'slc': 'grq_*_s1-iw_slc',
    'acq': 'grq_*_acquisition-s1-iw_slc',
    'aoi_track': 'grq_*_s1-gunw-aoi_track'
}


def generate_aoi_track_report(aoi_idx, aoi_id):
    """
    Queries for relevant products & builds the report by track.
    :param aoi_idx, str, ES index for AOI's
    :param aoi_id: area of interest id in elasticsearch, ex. AOI_monitoring_hawaiian_chain_tn124_hawaii
    :return: str, html with 2 tables
    """

    if aoi_id is False or aoi_idx is False:
        raise Exception('invalid inputs of aoi_id: {}, aoi_index: {}'.format(aoi_id, aoi_idx))

    aoi = get_aoi(aoi_id, aoi_idx)
    track_acq_lists = sort_by_track(get_objects('acq-list', aoi))

    for track in track_acq_lists.keys():
        # TODO: add title header with track number for email
        acqs = get_objects('acq', aoi, track)
        slcs = get_objects('slc', aoi, track)

        audit_trail = get_objects('audit_trail', aoi, track)
        if len(audit_trail) < 1:
            print('no audit trail products found for track {}'.format(track))
            continue
        else:
            print('Generating report for track: {}'.format(track))

        allowed_hashes = list(set(store_by_hash(audit_trail).keys()))  # allow only hashes foud in audit-trail
        acq_lists = filter_hashes(get_objects('acq-list', aoi, track), allowed_hashes)
        ifg_cfgs = filter_hashes(get_objects('ifg-cfg', aoi, track), allowed_hashes)
        ifgs = filter_hashes(get_objects('ifg', aoi, track), allowed_hashes)
        aoi_tracks = get_objects('aoi_track', aoi, track)

        now = datetime.datetime.now().strftime('%Y%m%dT%H%M')
        product_id = PRODUCT_NAME.format(aoi_id, track, now, VERSION)
        generate(product_id, aoi, track, acqs, slcs, acq_lists, ifg_cfgs, ifgs, audit_trail, aoi_tracks)
        print('generated {} for track: {}'.format(product_id, track))


def generate(product_id, aoi, track, acqs, slcs, acq_lists, ifg_cfgs, ifgs, audit_trail, aoi_tracks):
    """generates an enumeration comparison report for the given aoi & track"""
    acq_dct = store_by_id(acqs)
    acq_map_dct = store_by_slc_id(acqs)
    slc_dct = store_by_id(slcs)
    acq_list_dct = store_by_hash(acq_lists)  # converts dict where key is hash of master/slave slc ids
    ifg_cfg_dct = store_by_hash(ifg_cfgs)  # converts dict where key is hash of master/slave slc ids
    ifg_dct = store_by_hash(ifgs)  # converts dict where key is hash of master/slave slc ids
    aoi_track_dct = store_by_gunw(aoi_tracks)

    # generating missing SLCs html table
    missing_slcs_table = generate_missing_slcs_table(slc_dct, acq_lists)
    print(missing_slcs_table)

    # generating product report status html report
    product_status_table = generate_product_status_table(acq_list_dct, ifg_cfg_dct, ifg_dct, slc_dct, acq_map_dct,
                                                         aoi_track_dct)
    print(product_status_table)


def dict_to_inline_style(css_styles):
    """
    flattens dictionary to inline style
    :param css_styles: obj with css styles
    :return: str, flattened inline styles
    """
    inline_styles = '"'
    for key in css_styles:
        inline_styles += key + ':' + css_styles[key] + ';'
    inline_styles += '"'
    return inline_styles


def create_html_table_header(header):
    style_dict = {
        'border': '1px solid #dddddd',
        'text-align': 'left',
        'padding': '5px',
        'font-size': '10px',
        'font-family': 'Arial, Helvetica, sans-serif'
    }
    inline_style = dict_to_inline_style(style_dict)

    html_string = '<tr>'
    for cell in header:
        html_string += '<th style=' + inline_style + '>' + str(cell) + '</th>'
    html_string += '</tr>'
    return html_string


def create_html_table_row(row, counter):
    style_dict = {
        'border': '1px solid #dddddd',
        'text-align': 'left',
        'padding': '5px',
        'font-size': '10px',
        'font-family': 'Arial, Helvetica, sans-serif'
    }
    html_string = '<tr>' if counter % 2 == 0 else '<tr style="background-color:#dddddd">'
    td_style = dict_to_inline_style(style_dict)

    for cell in row:
        html_string += '<td style=' + td_style + '>' + str(cell) + '</td>'
    html_string += '</tr>'

    return html_string


def generate_product_status_table(acq_list_dict, ifg_cfg_dct, ifg_dct, slc_dct, acq_map_dct, aoi_track_dct):
    """
    generate the sheet for enumerated products
    :param acq_list_dict: dict type,
    :param ifg_cfg_dct: dict type,
    :param ifg_dct: dict type,
    :param slc_dct: dict type,
    :param acq_map_dct: dict type,
    :param aoi_track_dct: dict type,
    :return: html table element, <table><tr>...</tr></table>
    """

    # title = ['Date Pair', 'Acquisition-List', 'IFG-CFG', 'IFG', 'hash', 'Missing SLC IDs', 'Missing ACQ IDs', 'AOI track ID']
    title = ['Date Pair', 'Acquisition-List', 'IFG-CFG', 'IFG', 'Missing SLC IDs', 'Missing ACQ IDs']
    table_header = create_html_table_header(title)
    html_table_template = """
    <table style="border-collapse:collapse;">
        {header}
        {rows}
    </table>
    """

    html_rows = ''
    report_rows = []
    counter = 0

    for id_hash in sort_into_hash_list(acq_list_dict):
        acq_list = acq_list_dict.get(id_hash, {})
        ifg_cfg = ifg_cfg_dct.get(id_hash, {})
        ifg_cfg_id = ifg_cfg.get('_id', 'MISSING')
        ifg = ifg_dct.get(id_hash, {})
        date_pair = gen_date_pair(acq_list)
        acq_list_id = acq_list.get('_id', 'MISSING')
        ifg_id = ifg.get('_id', 'MISSING')
        aoi_track_id = aoi_track_dct.get(ifg_id, 'MISSING')

        missing_slcs = []
        missing_acqs = []

        acq_list_slcs = acq_list.get('_source', {}).get('metadata', {}).get('master_scenes', {}) + \
                        acq_list.get('_source', {}).get('metadata', {}).get('slave_scenes', {})

        for slc_id in acq_list_slcs:
            if not slc_dct.get(slc_id, False):
                missing_slcs.append(slc_id)
                missing_acq = acq_map_dct.get(slc_id, False)
                if missing_acq:
                    missing_acq_id = missing_acq.get('_id')
                    missing_acqs.append(missing_acq_id)

        missing_slc_str = ', '.join(missing_slcs)
        missing_acq_str = ', '.join(missing_acqs)
        if ifg_cfg_id == 'MISSING' or ifg_id == 'MISSING' or len(missing_acqs) > 0 or len(missing_slcs) > 0:
            # [date_pair, acq_list_id, ifg_cfg_id, ifg_id, id_hash, missing_slc_str, missing_acq_str, aoi_track_id]
            row = [date_pair, acq_list_id, ifg_cfg_id, ifg_id, missing_slc_str, missing_acq_str]
            report_rows.append(row)

    # creating summary row in the main product report
    numerical_summary_row = [
        'Total Missing',
        sum(1 if row[1] == 'MISSING' or row[1] == '' else 0 for row in report_rows),
        sum(1 if row[2] == 'MISSING' or row[2] == '' else 0 for row in report_rows),
        sum(1 if row[3] == 'MISSING' or row[3] == '' else 0 for row in report_rows),
        len(set([row[4] for row in report_rows if row[4] != ''])),
        len(set([row[5] for row in report_rows if row[5] != ''])),
    ]
    html_rows += create_html_table_header(numerical_summary_row)
    for row in report_rows:
        html_rows += create_html_table_row(row, counter)
        counter += 1

    return html_table_template.format(header=table_header, rows=html_rows)


def generate_missing_slcs_table(slc_dct, acq_lists):
    """
    generates the sheet for missing slcs
    :param slc_dct:
    :param acq_lists:
    :return: str, html string for the missing SLCs table
    """
    table_header = create_html_table_header(['Missing SLCs'])

    html_table_template = """
    <table style="border-collapse:collapse;">
        {table_header}
        {rows}
    </table>
    """
    rows = ''

    missing = []
    for acq_list in acq_lists:
        master_scenes = acq_list.get('_source', {}).get('metadata', {}).get('master_scenes', [])
        slave_scenes = acq_list.get('_source', {}).get('metadata', {}).get('slave_scenes', [])
        all_scenes = master_scenes + slave_scenes

        for slc_id in all_scenes:
            if slc_dct.get(slc_id, False) is False:
                missing.append(slc_id)

    missing = list(set(missing))
    counter = 0
    for slc in missing:
        rows += create_html_table_row([slc], counter)
        counter += 1

    return html_table_template.format(table_header=table_header, rows=rows)


def filter_hashes(obj_list, allowed_hashes):
    """filters out all objects in the object list that aren't storing any of the allowed hashes."""
    filtered_objs = []
    for obj in obj_list:
        full_id_hash = get_hash(obj)
        if full_id_hash in allowed_hashes:
            filtered_objs.append(obj)
    return filtered_objs


def store_by_hash(obj_list):
    """returns a dict where the objects are stored by their full_id_hash. drops duplicates."""
    result_dict = {}
    for obj in obj_list:
        full_id_hash = get_hash(obj)
        if full_id_hash in result_dict.keys():
            result_dict[full_id_hash] = get_most_recent(obj, result_dict.get(full_id_hash))
        else:
            result_dict[full_id_hash] = obj
    return result_dict


def get_most_recent(obj1, obj2):
    """returns the object with the most recent ingest time"""
    ctime1 = dateutil.parser.parse(obj1.get('_source', {}).get('creation_timestamp', False))
    ctime2 = dateutil.parser.parse(obj2.get('_source', {}).get('creation_timestamp', False))
    if ctime1 > ctime2:
        return obj1
    return obj2


def store_by_id(obj_list):
    """returns a dict where the objects are stored by their object id"""
    result_dict = {}
    for obj in obj_list:
        obj_id = obj.get('_source', {}).get('id', False)
        if obj_id:
            result_dict[obj_id] = obj
    return result_dict


def sort_by_track(es_result_list):
    """
    Goes through the objects in the result list, and places them in an dict where key is track
    """
    sorted_dict = {}
    for result in es_result_list:
        track = get_track(result)
        if track in sorted_dict.keys():
            sorted_dict.get(track, []).append(result)
        else:
            sorted_dict[track] = [result]
    return sorted_dict


def store_by_slc_id(obj_list):
    """returns a dict where acquisitions are stored by their slc id"""
    result_dict = {}
    for obj in obj_list:
        slc_id = obj.get('_source', {}).get('metadata', {}).get('title', False)
        if slc_id:
            result_dict[slc_id] = obj
    return result_dict


def store_by_gunw(obj_list):
    """returns a dict where the key is GUNW id and the value is the AOI_TRACK id"""
    result_dict = {}
    for obj in obj_list:
        aoi_track_id = obj.get('_source', {}).get('id', False)
        gunw_ids = obj.get('_source', {}).get('metadata', {}).get('s1-gunw-ids', [])
        for gunw_id in gunw_ids:
            result_dict[gunw_id] = aoi_track_id
    return result_dict


def get_track(es_obj):
    """returns the track from the elasticsearch object"""
    es_ds = es_obj.get('_source', {})
    # iterate through ds
    track_met_options = ['track_number', 'track', 'trackNumber', 'track_Number']
    for tkey in track_met_options:
        track = es_ds.get(tkey, False)
        if track:
            return track
    # if that doesn't work try metadata
    es_met = es_ds.get('metadata', {})
    for tkey in track_met_options:
        track = es_met.get(tkey, False)
        if track:
            return track
    raise Exception('unable to find track for: {}'.format(es_obj.get('_id', '')))


def gen_date_pair(obj):
    """returns the date pair string for the input object"""
    st = dateutil.parser.parse(obj.get('_source').get('starttime')).strftime('%Y%m%d')
    et = dateutil.parser.parse(obj.get('_source').get('endtime')).strftime('%Y%m%d')
    return '{}-{}'.format(et, st)


def sort_into_hash_list(obj_dict):
    """builds a list of hashes where the hashes are sorted by the objects endtime"""
    sorted_obj = sorted(obj_dict.keys(), key=lambda x: get_endtime(obj_dict.get(x)), reverse=True)
    return sorted_obj  # [obj.get('_source', {}).get('metadata', {}).get('full_id_hash', '') for obj in sorted_obj]


def get_endtime(obj):
    """returns the endtime"""
    return dateutil.parser.parse(obj.get('_source', {}).get('endtime'))


def get_hash(es_obj):
    """retrieves the full_id_hash. if it doesn't exists, it
        attempts to generate one"""
    full_id_hash = es_obj.get('_source', {}).get('metadata', {}).get('full_id_hash', False)
    if full_id_hash:
        return full_id_hash
    return gen_hash(es_obj)


def gen_hash(es_obj):
    """copy of hash used in the enumerator"""
    met = es_obj.get('_source', {}).get('metadata', {})
    master_slcs = met.get('master_scenes', met.get('reference_scenes', False))
    slave_slcs = met.get('slave_scenes', met.get('secondary_scenes', False))
    master_ids_str = ""
    slave_ids_str = ""
    for slc in sorted(master_slcs):
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]
        if master_ids_str == "":
            master_ids_str = slc
        else:
            master_ids_str += " " + slc
    for slc in sorted(slave_slcs):
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]
        if slave_ids_str == "":
            slave_ids_str = slc
        else:
            slave_ids_str += " " + slc
    id_hash = hashlib.md5(json.dumps([master_ids_str, slave_ids_str]).encode("utf8")).hexdigest()
    return id_hash


def get_objects(object_type, aoi, track_number=False):
    """
    returns all objects of the object type ['ifg, acq-list, 'ifg-blacklist'] that intersect both
    temporally and spatially with the aoi
    """
    idx = IDX_DCT.get(object_type)  # determine index
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    track_field = 'track_number'
    if object_type == 'slc' and track_number:
        track_field = 'trackNumber'

    grq_query = {
        "query": {
            "filtered": {
                "query": {
                    "geo_shape": {
                        "location": {
                            "shape": location
                        }
                    }
                },
                "filter": {
                    "bool": {
                        "must": [
                            # {"term": {"metadata.{}".format(track_field): track_number}},
                            {"range": {"endtime": {"gte": starttime}}},
                            {"range": {"starttime": {"lte": endtime}}}
                        ]
                    }
                }
            }
        },
        "from": 0,
        "size": 1000
    }
    if track_number:
        grq_query['query']['filtered']['filter']['bool']['must'].append({
            "term": {
                "metadata.{}".format(track_field): track_number
            }
        })

    if object_type == 'audit_trail' or object_type == 'aoi_track':
        grq_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"metadata.aoi.raw": aoi.get('_source').get('id')}},
                        {"term": {"metadata.track_number": track_number}}
                    ]
                }
            },
            "from": 0,
            "size": 1000
        }

    results = query_es(grq_url, grq_query)
    return results


def query_es(grq_url, es_query):
    """
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    """
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

    # pprint(es_query)

    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')

    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)

    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))

    return results_list


def get_aoi(aoi_id, index):
    'retrieves the AOI from ES'
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, index)
    es_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"id.raw": aoi_id}}
                ]
            }
        }
    }

    result = query_es(grq_url, es_query)
    if len(result) < 1:
        raise Exception('Found no results for AOI: {}'.format(aoi_id))
    return result[0]


def load_context():
    """loads the context file into a dict"""
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except Exception as e:
        print(e)
        raise Exception('unable to parse _context.json from work directory')


def get_all_aois(es_index):
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, es_index)

    es_query = {
        "size": 1000,
        "fields": ["_id"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"dataset_type.raw": "area_of_interest"}}
                ]
            }
        }
    }

    res = query_es(grq_url, es_query)
    list_aoi = [row['fields']['_id'] for row in res]
    return list_aoi


if __name__ == '__main__':
    ctx = load_context()
    aoi_index = ctx.get('aoi_index', False)

    """
    TODO:
        get all aoi records from ES, put in variable `aoi_list`
        loop through each AOI with generate_aoi_track_report function

    """
    aoi_list = get_all_aois(aoi_index)
    pprint(aoi_list)

    for _id in aoi_list:
        generate_aoi_track_report(aoi_index, _id)
