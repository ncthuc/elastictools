import json

import elasticsearch
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
        if not self._indextool.exists(index_name):
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
        if not self._indextool.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        if params:
            body = DocTools.render(body, params)
        print(body)
        doctype = IndexTools.mapping_get_doctype(self._indextool.get_mapping(index_name))
        if id:
            return self._es.index(index = index_name, body=body, doc_type=doctype, id=id, **kwargs)
        else:
            return self._es.index(index=index_name, body=body, doc_type=doctype, **kwargs)

    def delete(self, index_name, id, **kwargs):
        if not self._indextool.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self._indextool.get_mapping(index_name))
        return self._es.delete(index=index_name, id=id, doc_type=doctype, **kwargs)

    def exists(self, index_name, id, **kwargs):
        if not self._indextool.exists(index_name):  
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self._indextool.get_mapping(index_name))
        return self._es.exists(index=index_name, id=id, doc_type=doctype, **kwargs)

    def get(self, index_name, id, source=False, **kwargs):
        if not self._indextool.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        doctype = IndexTools.mapping_get_doctype(self._indextool.get_mapping(index_name))
        if source:
            return self._es.get_source(index=index_name, id=id, doc_type=doctype, **kwargs)
        else:
            return self._es.get(index=index_name, id=id, doc_type=doctype, **kwargs)


