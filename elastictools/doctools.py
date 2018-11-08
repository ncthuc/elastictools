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
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        return self._es.delete(index=index_name, id=id, doc_type=doctype, **kwargs)

    def exists(self, index_name, id, **kwargs):
        if not self.indextool().exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self.indextool().get_mapping(index_name))
        return self._es.exists(index=index_name, id=id, doc_type=doctype, **kwargs)

    def get(self, index_name, id, source=False, **kwargs):
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
                         collapse=None):
        if not body:
            body = {}

        if query is None:
            query = {"match_all": {}}
        body['query'] = query
        if _source:
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

    def search(self, index_name, body=None, params=None, source_only=False, **kwargs):
        """

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
                doc['_source']['_id'] =doc['_id']
                doc['_source']['_score'] = doc['_score']
                res.append(doc['_source'])
        return res

    def msearch(self, indices, queries, return_body_only=False, **kwargs):
        """

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

        :param filename:
        :param index_name:
        :param csv_fields: None - use first row as header
        :param thread_count:
        :param kwargs:
        :return:
        """
        with open(filename) as f:
            reader = csv.DictReader(f, fieldnames=csv_fields)
            return  self.bulk(index_name, reader, thread_count, **kwargs)