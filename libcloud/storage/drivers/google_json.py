# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Module for Google Cloud Storage Driver.
"""
from __future__ import with_statement

import datetime
import time
import sys

from libcloud.common.base import LazyObject
from libcloud.common.google import GoogleOAuth2Credential
from libcloud.common.google import GoogleResponse
from libcloud.common.google import GoogleBaseConnection
from libcloud.common.google import GoogleBaseError
from libcloud.common.google import ResourceNotFoundError
from libcloud.common.google import ResourceExistsError
from libcloud.common.types import ProviderError

from libcloud.storage.base import StorageDriver, Container, Object

from libcloud.utils.iso8601 import parse_date

API_VERSION = 'v1'
DEFAULT_TASK_COMPLETION_TIMEOUT = 180

class GCSResponse(GoogleResponse):
    pass


class GCSConnection(GoogleBaseConnection):
    """
    Connection class for the GCS driver.
    """
    host = 'www.googleapis.com'
    responseCls = GCSResponse

    def __init__(self, user_id, key, secure, auth_type=None,
                 credential_file=None, project=None, **kwargs):
        super(GCSConnection, self).__init__(
            user_id, key, secure=secure, auth_type=auth_type,
            credential_file=credential_file, **kwargs)
        self.request_path = '/storage/%s' % API_VERSION
        self.gcs_params = None

    def pre_connect_hook(self, params, headers):
        """
        Update URL parameters with values from self.gce_params.

        @inherits: :class:`GoogleBaseConnection.pre_connect_hook`
        """
        params, headers = super(GCSConnection, self).pre_connect_hook(params,
                                                                      headers)
        if self.gcs_params:
            params.update(self.gcs_params)
        return params, headers

    def request(self, *args, **kwargs):
        """
        Perform request then do GCE-specific processing of URL params.

        @inherits: :class:`GoogleBaseConnection.request`
        """
        response = super(GCSConnection, self).request(*args, **kwargs)

        # If gce_params has been set, then update the pageToken with the
        # nextPageToken so it can be used in the next request.
        if self.gcs_params:
            if 'nextPageToken' in response.object:
                self.gcs_params['pageToken'] = response.object['nextPageToken']
            elif 'pageToken' in self.gcs_params:
                del self.gcs_params['pageToken']
            self.gcs_params = None

        return response


class GoogleStorageDriver(StorageDriver):

    connectionCls = GCSConnection
    name = "Google Cloud Storage"
    website ='https://cloud.google.com/storage/'
    hash_type = 'md5'
    supports_chunked_encoding = False

    def __init__(self, user_id, key=None, project=None, auth_type=None, **kwargs):
         super(GoogleStorageDriver, self).__init__(user_id, key, **kwargs)
         self.project = project

    def iterate_containers(self):
        request = '/b'
        params = {'project': self.project}
        response = self.connection.request(request, method='GET', params=params)
        containers = self._to_containers(data=response.object)
        return containers

    def iterate_container_objects(self, container):
        request = '/b/%s/o' % container.name
        response = self.connection.request(request, method='GET')
        objects = self._to_objects(data=response.object, container=container)
        return objects

    def get_container(self, container_name):
        request = '/b/%s' % container_name
        response = self.connection.request(request, method='GET')
        container = self._to_container(item=response.object)
        if container:
            return container
        else:
            raise ContainerDoesNotExistError(value=None, driver=self,
                                             container_name=container_name)

    def get_object(self, container_name, object_name):
        container = self.get_container(container_name)
        request = '/b/%s/o/%s' % (container_name, object_name)
        response = self.connection.request(request, method='GET')
        obj = self._to_object(item=response.object, container=container)
        if obj is not None:
            return obj
        else:
            raise ObjectDoesNotExistError(value=None, driver=self, object_name=object_name)
        return obj

    def delete_object(self, obj):
        object_name = obj.name
        container_name = obj.container.name
        request = 'b/%s/o/%s' % (container_name, object_name)
        response = self.connection.request(request, method='DELETE')
        if response.object == "":
            return True
        return False

    def create_container(self, container_name):
        request = '/b'
        params = {'project': self.project}
        data = {'name': container_name}
        response = self.connection.request(request, method='POST', params=params, data=data)
        container = self._to_container(item=response.object)
        return container

    def delete_container(self,container):
        request = '/b/%s' % container.name
        response = self.connection.request(request, method='DELETE')
        if response.object == "":
            return True
        return False

    def download_object(self, obj, destination_path, overwrite_existing=False, delete_on_failure=True):
        container_name = obj.container.name
        object_name = obj.name
        params = {'alt': 'media'}
        request = '/b/%s/o/%s' % (container_name, object_name)
        response = self.connection.request(request, method='GET', params=params)

        
    def _to_containers(self, data):
        containers = []
        for item in data['items']:
            container = self._to_container(item=item)
            containers.append(container)
        return containers

    def _to_container(self, item):
        container = Container(name=item['name'], extra=item, driver=self)
        return container

    def _to_objects(self, data, container):
        objects = []
        for item in data['items']:
            obj = self._to_object(item=item, container=container)
            objects.append(obj)
        return objects

    def _to_object(self, item, container=None):
        meta_data = item.get('metadata', {})
        obj = Object(name=item['name'], size=item['size'], hash=item['md5Hash'], extra=item,
                     meta_data=meta_data, container=container, driver=self)
        return obj
