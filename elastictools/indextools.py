import elasticsearch


class IndexTools:
    def __init__(self, hosts=None, es=None):
        """
        Initialize an ElasticSearch instance with list of hosts
        :param hosts: list of host, ex.:
            [
                {'host': 'localhost:9200'},
                {'host': 'othernode', 'port': 443, 'url_prefix': 'es', 'use_ssl': True},
            ]
        """
        self._doctool = None
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

    # def doctool(self):
    #     if not self._doctool:
    #         self._doctool = DocTool.from_es(self._es)
    #
    #     return self._doctool

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
        Check if a doctytpe existed in a index ES
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
            raise ValueError('index not existed: {}'.format(index_name))
        mapping = self._es.indices.get_mapping(index=index_name, **kwargs)[index_name]['mappings']
        if doc_type:
            IndexTools.mapping_set_doctype(mapping, doc_type)
        return mapping

    @staticmethod
    def mapping_get_doctype(mapping):
        """
        Get doctype of the mapping
        :param mapping:
        :return:
        """
        if len(mapping.keys()) != 1:
            raise ValueError('There should be exactly one doc_type in a mapping.')
        key = list(mapping.keys())[0]

        return key

    @staticmethod
    def mapping_set_doctype(mapping, doc_type):
        """
        Set/change doctype of a mapping
        :param mapping:
        :param doc_type:
        :return:
        """
        key = IndexTools.mapping_get_doctype(mapping)
        if key == doc_type:
            return mapping
        mapping[doc_type] = mapping[key]
        mapping.pop(key)
        return mapping

    @staticmethod
    def mapping_cast(mapping, property_name, new_type):
        """
        Change type of a property inside a mapping
        :param mapping:
        :param property_name:
        :param new_type:
        :return:
        """
        if len(mapping.keys()) != 1:
            raise ValueError('There should be exactly one doc_type in a mapping.')
        key = list(mapping.keys())[0]
        if property_name not in mapping[key]['properties']:
            return mapping
        if type(new_type) is dict:
            mapping[key]['properties'][property_name] = new_type
        else:
            mapping[key]['properties'][property_name]['type'] = new_type
        return mapping

    @staticmethod
    def mapping_rename(mapping, property_name, new_name):
        """
        Rename a property inside a mapping
        :param mapping:
        :param property_name:
        :param new_name:
        :return:
        """
        if len(mapping.keys()) != 1:
            raise ValueError('There should be exactly one doc_type in a mapping.')
        key = list(mapping.keys())[0]
        if property_name not in mapping[key]['properties']:
            return mapping

        mapping[key]['properties'][new_name] = mapping[key]['properties'][property_name]
        mapping[key]['properties'].pop(property_name)
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
            raise ValueError('index not existed: {}'.format(index_name))
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

    def create_if_not_exists(self, index_name, body=None, mapping=None, settings=None, **kwargs):
        if self.exists(index_name):
            return True
        return self.create(index_name, body, mapping, settings, **kwargs)

    def delete(self, index_name, **kwargs):
        """
        Delete an index
        :param index_name:
        :param kwargs:
        :return:
        """
        return self._es.indices.delete(index=index_name, ignore=404, **kwargs)

    def clone(self, src_index, dest_index, mapping=None, settings=None, size=None, script=None, overwrite=None,
              wait_for_completion=False,
              **kwargs):
        """
        Create dest_index with mapping and settings and reindex src_index into dest_index
        :param src_index: source index name
        :param dest_index: destination index name
        :param mapping: mapping of new index, if None will clone mapping from src_index
        :param settings: settings of new index, if None will clone settings from src_index
        :param kwargs:
        :return:
        """

        if not self.exists(src_index):
            raise ValueError('src_index not existed: {}'.format(src_index))

        if not mapping:
            mapping = self.clone_mapping(src_index)

        if not settings:
            settings = self.clone_settings(src_index)

        self.create(dest_index, mapping=mapping, settings=settings, overwrite=overwrite)

        body = {
            "source": {
                "index": src_index
            },
            "dest": {
                "index": dest_index
            }
        }

        if size:
            body['size'] = size

        if script:
            body['script'] = script

        return self._es.reindex(body=body, wait_for_completion=wait_for_completion, **kwargs)

    def close(self, index_name, **kwargs):
        """
        close an index
        :param index_name:
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        return self._es.indices.close(index=index_name, **kwargs)

    def open(self, index_name, **kwargs):
        """
        Open an index
        :param index_name:
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        return self._es.indices.open(index=index_name, **kwargs)

    def reopen(self, index_name, **kwargs):
        """
        this will reload synonym file also
        :param index_name:
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        self._es.indices.close(index=index_name, **kwargs)
        return self._es.indices.open(index=index_name, **kwargs)

    def refresh(self, index_name, **kwargs):
        """
        Refresh to make all create/update effected
        :param index_name:
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        return self._es.indices.refresh(index=index_name, **kwargs)

    def truncate(self, index_name, wait_for_completion=False, **kwargs):
        """
        Remove all documents in an index
        :param index_name:
        :param wait_for_completion:
        :param kwargs:
        :return:
        """
        if not self.exists(index_name):
            raise ValueError('index not existed: {}'.format(index_name))
        query = {
            "query": {
                "match_all": {}
            }
        }
        return self._es.delete_by_query(index=index_name, body=query, wait_for_completion=wait_for_completion, **kwargs)
