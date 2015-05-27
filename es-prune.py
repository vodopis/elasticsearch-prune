#!/usr/bin/python

import os, sys
import json
import subprocess
import datetime

ES_HOST = 'localhost'
ES_PORT = '9200'
ES_INDEX_TYPE = 'mytype'
SCROLL_ID_LIFESPAN = '1m'
SCROLL_SIZE = 1000
PRUNE_OLDER_THAN_MONTHS = 3

SCROLL_ID_CMD = """curl -s -XPOST "http://{es_host}:{es_port}/logstash-{prune_date_fmt}/{es_index_type}/_search?pretty=true&search_type=scan&scroll={scroll_id_lifespan}" -d'
{{
    "size":  {scroll_size},
    "fields": ["_id"],
    "query": {{
        "match_all" : {{}}

    }}
}}'"""

SCROLL_RESULTS_CMD = """curl -s -XGET 'http://{es_host}:{es_port}/_search/scroll?pretty=true&scroll=10m&scroll_id={scroll_id}'"""

BULK_UPDATE_CMD = 'curl -s -XPOST --data-binary @- http://'+ES_HOST+':'+ES_PORT+'/_bulk?pretty=true'

def add_months(d,x):
    newmonth = ((( d.month - 1) + x ) % 12 ) + 1
    newyear  = d.year + ((( d.month - 1) + x ) / 12 )
    return datetime.date( newyear, newmonth, d.day)


def es_prune(prune_date):
    prune_date_fmt = prune_date.strftime('%Y.%m.%d')
    bulk_json_prefix = '{ "update": { "_index": "logstash-' + prune_date_fmt + '", "_type": "' + ES_INDEX_TYPE + '", "_id": "'

    siq = SCROLL_ID_CMD.format(es_host=ES_HOST, es_port=ES_PORT, prune_date_fmt=prune_date_fmt,scroll_id_lifespan=SCROLL_ID_LIFESPAN,scroll_size=SCROLL_SIZE, es_index_type=ES_INDEX_TYPE)
    try:
        siq_output = subprocess.check_output(siq, shell=True)
    except subprocess.CalledProcessError as e:
        print 'ERROR: failed to execute scroll id query: ' + e.output + ', code=' + str(e.returncode)
        raise

    siq_json = json.loads(siq_output)
    try:
    	scroll_id = siq_json['_scroll_id']
    except KeyError:
        print 'ERROR: failed to retrieve scroll_id. Returned json='+json.dumps(siq_json)
        raise

    id_ctr = 0
    while True:
        print 'scroll_id='+scroll_id

        srq = SCROLL_RESULTS_CMD.format(es_host=ES_HOST, es_port=ES_PORT, scroll_id=scroll_id)
        try:
            srq_output = subprocess.check_output(srq, shell=True)
        except subprocess.CalledProcessError as e:
            print 'ERROR: failed to execute scroll results query: ' + e.output
            raise

        srq_json = json.loads(srq_output)
        scroll_id = srq_json['_scroll_id']
        hits = srq_json['hits']['hits']

        if len(hits) == 0:
           print 'No (more) hits, exiting..'
           break

        bulk_in = []
        for hit in hits:
            id_ctr += 1
            if id_ctr % 1000 == 0:
                print str(id_ctr) + ': ' + hit['_id']

            bulk_in.append(bulk_json_prefix)
            bulk_in.append(hit['_id'])
            bulk_in.append('"}\n')
            bulk_in.append('{ "doc" : {"message" : "", "body" : ""} }\n')

        print 'Submitting bulk request for ' + str(len(hits)) + ' documents'

        bulk_proc = subprocess.Popen(BULK_UPDATE_CMD.split(),stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        bulk_out = bulk_proc.communicate(''.join(bulk_in))[0]
        if bulk_proc.returncode != 0:
            print 'ERROR: Bulk api call failed. Return code: ' + str(bulk_proc.returncode) + '. Add -v flag to curl params to see cause.'
            raise OSError(bulk_proc.returncode, 'Bulk api call failed')
        print 'Bulk api returned '+str(len(bulk_out))+' bytes'

def main():
    print 'Program started on ' + datetime.datetime.now().isoformat()

    prune_date = add_months(datetime.datetime.utcnow(), -PRUNE_OLDER_THAN_MONTHS)
    es_prune(prune_date)

    print 'Program finished on ' + datetime.datetime.now().isoformat()

if __name__ == "__main__":
    main()
