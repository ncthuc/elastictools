import elasticsearch


class IndexTool:
    def __init__(self, hosts):
        """
        Initialize an ElasticSearch instance with list of hosts
        :param hosts: list of host, ex.:
            [
                {'host': 'localhost:9200'},
                {'host': 'othernode', 'port': 443, 'url_prefix': 'es', 'use_ssl': True},
            ]
        """
        self._hosts = hosts
        self._es = elasticsearch.Elasticsearch(hosts)

    @classmethod
    def from_url(cls, es_url):
        "Initialize an ElasticSearch with single url"
        hosts = [es_url]
        return cls(hosts)

    def exists(self, index_name, **kwargs):
        """
        Check if an index or multiple indices existed in ES
        :param index_name: an index name, or list for index names
        :param kwargs:
        :return: True if every index in index_name exists
        """
        return self._es.indices.exists(index_name, **kwargs)

    def exists_type(self, index_name, doc_type, **kwargs):
        """
        Check if an index or multiple indices existed in ES
        :param index_name: an index name, or list for index names, '_all' for all
        :param doc_type: an doc_type name, or list for doc_type names
        :param kwargs:
        :return: True/False
        """
        return self._es.indices.exists_type(index_name, doc_type, **kwargs)

    def get_info(self, index_name, **kwargs):
        """
        Get info of an index
        :param index_name: an index name
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            return None

        return self._es.indices.get(index_name, **kwargs)[index_name]

    def get_mapping(self, index_name, **kwargs):
        """
        Get mapping of an index
        :param index_name: an index name
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            return None
        return self._es.indices.get_mapping(index=index_name, **kwargs)[index_name]['mappings']

    def clone_mapping(self, index_name, doc_type=None, **kwargs):
        """
        Get mapping of an index
        :param doc_type: new doc type for result mapping, None if unchange
        :param index_name: an index name
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            return None
        mapping = self._es.indices.get_mapping(index=index_name, **kwargs)[index_name]['mappings']
        if doc_type:
            self.set_doctype(mapping, doc_type)
        return mapping

    def set_doctype(self, mapping, doc_type):
        if len(mapping.keys()) != 1:
            raise ValueError('There should be exactly one doc_type in a mapping.')
        key = list(mapping.keys())[0]
        if key == doc_type:
            return mapping
        mapping[doc_type] = mapping[key]
        mapping.pop(key)
        return mapping

    def get_settings(self, index_name, **kwargs):
        """
        Get settings of an index
        :param index_name: an index name, or list for index names, '_all' for all
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            return None
        return self._es.indices.get_settings(index=index_name, **kwargs)[index_name]['settings']

    def clone_settings(self, index_name, **kwargs):
        """
        Clone settings of an index, return dictionary with current index specific data removed
        :param index_name: an index name, or list for index names, '_all' for all
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            return None
        settings = self._es.indices.get_settings(index=index_name, **kwargs)[index_name]['settings']
        settings['index'].pop('creation_date', None)
        settings['index'].pop('version', None)
        settings['index'].pop('uuid', None)
        settings['index'].pop('provided_name', None)
        return settings

    def stats(self, index_name, **kwargs):
        """
        Get settings of an index
        :param index_name: an index name, or list for index names, '_all' for all
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            return None
        return self._es.indices.stats(index=index_name, **kwargs)['indices'][index_name]

    def create(self, index_name, body=None, mapping=None, settings=None, overwrite=False,  **kwargs):
        """
        Get settings of an index
        :param body: if specified, ignore settings and mapping
        :param settings:
        :param mapping:
        :param overwrite:
        :param index_name: an index name, or list for index names, '_all' for all
        :param kwargs:
        :return:
        """
        if self.exists(index_name):
            if not overwrite:
                raise ValueError('{} index already existed.'.format(index_name))
            self.delete(index_name)
        if body:
            return self._es.indices.create(index=index_name, body=body, **kwargs)
        else:
            return self._es.indices.create(index=index_name, body={'settings': settings, 'mappings': mapping}, **kwargs)


    def delete(self, index_name, **kwargs):
        return self._es.indices.delete(index=index_name, ignore=404, **kwargs)