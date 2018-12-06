import json
import csv
from collections import deque
import elasticsearch
import elasticsearch.helpers
import jinja2

from elastictools.indextools import IndexTools


class DocTools:
    def __init__(self, hosts=None, es=None):
        """
        Initialize an ElasticSearch instance with list of hosts
        :param hosts: list of host, ex.:
            [
                {'host': 'localhost:9200'},
                {'host': 'othernode', 'port': 443, 'url_prefix': 'es', 'use_ssl': True},
            ]
        """
        self._indextool = None
        if es:
            self._es = es
        else:
            if hosts is None:
                raise ValueError('hosts or es param missing.')
            self._hosts = hosts
            self._es = elasticsearch.Elasticsearch(hosts)

    @classmethod
    def from_url(cls, es_url):
        "Initialize an ElasticSearch with single url"
        hosts = [es_url]
        return cls(hosts=hosts)

    @classmethod
    def from_es(cls, es):
        "Initialize an ElasticSearch instance"
        return cls(es=es)

    def indextool(self):
        """
        Get indextool instance
        :return:
        """
        if not self._indextool:
            self._indextool = IndexTools.from_es(self._es)

        return self._indextool

    @staticmethod
    def render(obj, params):
        """
        Render a jinja2 template
        :param obj: string or dict
        :param params: a dictionary of params
        :return:
        """

        if type(obj) is str:
            t = jinja2.Template(obj)
            return t.render(params)
        else:
            obj = json.dumps(obj)
            t = jinja2.Template(obj)
            return json.loads(t.render(**params))

    def count(self, index_name, body, params, **kwargs):
        """
        Count the number of document in an index, that match the body search
        :param index_name:
        :param body:
        :param params:
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        if params:
            body = DocTools.render(body, params)
        # print(body)
        return self._es.count(index = index_name, body=body)['count']

    def index(self, index_name, body, params=None, id=None, **kwargs):
        """
        Create or update a document
        :param index_name:
        :param body:
        :param params:
        :param id: if None, will generate, if not None, will replace index if existed
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        if params:
            body = DocTools.render(body, params)
        # print(body)
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        if id:
            return self._es.index(index = index_name, body=body, doc_type=doctype, id=id, **kwargs)
        else:
            return self._es.index(index=index_name, body=body, doc_type=doctype, **kwargs)

    def delete(self, index_name, id, **kwargs):
        """
        Delete a document with id = `id` in an index
        :param index_name:
        :param id:
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        return self._es.delete(index=index_name, id=id, doc_type=doctype, **kwargs)

    def exists(self, index_name, id, **kwargs):
        """
        Check if a document exists in an index or not
        :param index_name:
        :param id:
        :param kwargs:
        :return: boolean
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        return self._es.exists(index=index_name, id=id, doc_type=doctype, **kwargs)

    def get(self, index_name, id, source=False, **kwargs):
        """
        Get a document in an index by it id
        :param index_name:
        :param id:
        :param source:
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        if source:
            return self._es.get_source(index=index_name, id=id, doc_type=doctype, **kwargs)
        else:
            return self._es.get(index=index_name, id=id, doc_type=doctype, **kwargs)

    @staticmethod
    def make_search_body(body=None, params=None, from_=None, size=None, query=None, _source=None, highlight=None,
                         aggs=None, sort=None, script_fields=None, post_filter=None, rescore=None, min_score=None,
                         collapse=None, source_includes=None, source_excludes=None):
        """
        Return a body (Python dict) for search query, based on multiple criteria
        :param body:
        :param params:
        :param from_:
        :param size:
        :param query:
        :param _source:
        :param highlight:
        :param aggs:
        :param sort:
        :param script_fields:
        :param post_filter:
        :param rescore:
        :param min_score:
        :param collapse:
        :param source_includes: list of fields
        :param source_excludes:
        :return:
        """
        if not body:
            body = {}

        if query is None:
            query = {"match_all": {}}
        body['query'] = query
        if _source:
            body['_source'] = _source
        if source_excludes or source_includes:
            if not _source:
                _source = {}
            if source_includes:
                _source['includes'] = source_includes
            if source_excludes:
                _source['excludes'] = source_excludes
            body['_source'] = _source

        if highlight:
            body['highlight'] = highlight
        if aggs:
            body['aggs'] = aggs
        if from_:
            body['from'] = from_
        if size:
            body['size'] = size
        if sort:
            body['sort'] = sort
        if script_fields:
            body['script_fields'] = script_fields
        if post_filter:
            body['post_filter'] = post_filter
        if rescore:
            body['rescore'] = rescore
        if min_score:
            body['min_score'] = min_score
        if collapse:
            body['collapse'] = collapse

        if params:
            body = DocTools.render(body, params)

        return body

    def search(self, index_name, body=None, params=None, source_only=False, reserve_id_score=False, **kwargs):
        """
        Execute a search query
        :param index_name:
        :param body:
        :param params:
        :param source_only: get source documents only as Python list, with elastics `_id` and `_score`
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        if params:
            body = DocTools.render(body, params)
        res = self._es.search(index_name, body=body, **kwargs)
        if source_only:
            tmp = res['hits']['hits']
            res = []
            for doc in tmp:
                if reserve_id_score:
                    doc['_source']['_id'] = doc['_id']
                    doc['_source']['_score'] = doc['_score']
                res.append(doc['_source'])
        return res

    def dump(self, index_name, query=None, params=None,
             datetime_field=None, datetime_from=None, datetime_to=None, to_file=False, page_size=1000,
             source_excludes=None, source_includes=None, **kwargs):
        """

        :param index_name:
        :param query:
        :param params:
        :param datetime_field:
        :param datetime_from:  20181101T000000+07:00
        :param datetime_to:    20181107T235959+07:00
        :param to_file:
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))

        sort = None
        if datetime_field:
            sort = [
                {
                  datetime_field: {
                    "order": "asc"
                  }
                }
            ]
            query={
                "bool": {
                    "must": [query] if query else [{"match_all": {}}],
                    "filter": [
                        {
                            "range": {
                                "request_time": {
                                    "gte": datetime_from,
                                    "lt": datetime_to,
                                    "format": "basic_date_time_no_millis"
                                }
                            }
                        }
                    ]
                }
            }
        body = self.make_search_body(query=query, params=params, sort=sort)
        res = self.search(index_name, body=body, source_only=False, **kwargs)
        total = res['hits']['total']
        _from = 0
        _size = page_size
        res = []
        if to_file:
            file = open(to_file, 'w')
            file.write('[')
        while _from < total:
            print('reading {} to {}...'.format(_from+1, min(_from+_size, total)))
            body = self.make_search_body(query=query, params=params, sort=sort, from_=_from, size=_size,
                                         source_includes=source_includes, source_excludes=source_excludes)
            r = self.search(index_name, body=body, source_only=True, **kwargs)
            _from += _size
            if to_file:
                file.write(',\n'.join([json.dumps(rec) for rec in r]) + (',' if _from<total else ''))
            else:
                res.extend(r)
            # res.append(body)

        if to_file:
            file.write(']')
            file.close()
            return total
        return res

    def msearch(self, indices, queries, return_body_only=False, **kwargs):
        """
        Execute a msearch query
        :param return_body_only: if set, not execute the actual search, just return body
        :param indices: list of indices
        :param queries: list of query body
        :param kwargs:
        :return:
        """
        body=''
        for i in range(len(indices)):
            index = json.dumps({'index':indices[i]})
            query = json.dumps(queries[i])
            body += index + '\n' + query + '\n'
        if return_body_only:
            return body
        return self._es.msearch(body=body)

    def bulk(self, index_name, actions, thread_count=1, **kwargs):
        """
        Do bulk actions, if thread_count = 1, otherwise call parallel_bulk
        :param index_name:
        :param actions: any iterable, can also be a generator, in search result format (with `_source`) or orignal format
        :param thread_count: 1 if using bulk, other wise, usi aarop
        :param kwargs:
        :return:
        """
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        if thread_count<=1:
            print('Normal bulk')
            return elasticsearch.helpers.bulk(self._es, actions, index=index_name, doc_type=doctype, **kwargs)
        else:
            print('Parallel bulk', thread_count)
            return deque(elasticsearch.helpers.parallel_bulk(self._es, actions, index=index_name, doc_type=doctype,
                                                       thread_count=thread_count, **kwargs), maxlen=0)

    def bulk_insert_from_csv(self, filename, index_name, csv_fields=None, thread_count=1, **kwargs):
        """
        bulk insert form csv file
        :param filename:
        :param index_name:
        :param csv_fields: None - use first row as header
        :param thread_count:
        :param kwargs:
        :return:
        """
        with open(filename) as f:
            reader = csv.DictReader(f, fieldnames=csv_fields)
            return self.bulk(index_name, reader, thread_count, **kwargs)

    def bulk_insert_from_json(self, filename, index_name, thread_count=1, **kwargs):
        """
        bulk insert form csv file
        :param filename:
        :param index_name:
        :param thread_count:
        :param kwargs:
        :return:
        """
        with open(filename) as f:
            data = json.load(f)
            return self.bulk(index_name, data, thread_count, **kwargs)