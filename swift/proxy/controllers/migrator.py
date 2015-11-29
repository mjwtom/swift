# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from time import time

from swift.common.utils import public, get_hmac, get_swift_info, json, \
    streq_const_time
from swift.proxy.controllers.base import Controller, delay_denial
from swift.common.swob import HTTPOk, HTTPServiceUnavailable, Request

import six
from six.moves.urllib.parse import unquote, quote
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout, ResponseTimeout, \
    InsufficientStorage, FooterNotSupported, MultiphasePUTNotSupported, \
    PutterConnectError, ChunkReadError
from swift.common.bufferedhttp import http_connect_raw
from swift.common.http import (
    is_informational, is_success, is_client_error, is_server_error,
    HTTP_OK, HTTP_CONTINUE, HTTP_CREATED, HTTP_MULTIPLE_CHOICES,
    HTTP_INTERNAL_SERVER_ERROR, HTTP_SERVICE_UNAVAILABLE,
    HTTP_INSUFFICIENT_STORAGE, HTTP_PRECONDITION_FAILED, HTTP_CONFLICT)
from eventlet.timeout import Timeout
from swift import gettext_ as _


class MigrationController(Controller):
    """WSGI controller for info requests"""
    server_type = 'Migration'

    def __init__(self, app, device,
                 **kwargs):
        Controller.__init__(self, app)
        self.device = unquote(device)
        self.dedupe = app.dedupe

    @public
    @delay_denial
    def OPTIONS(self, req):
        return HTTPOk(request=req, headers={'Allow': 'DISK_FAILURE'})

    def connect_node(self, node, path=None, headers=None):
        try:
            start_time = time.time()
            with ConnectionTimeout(self.app.conn_timeout):
                conn = http_connect_raw(
                    node['ip'], node['port'], 'MIGRATE',
                    path, headers)
            self.app.set_node_timing(node, time.time() - start_time)
            with Timeout(self.app.node_timeout):
                resp = conn.getexpect()
            if resp.status == HTTP_OK:
                conn.resp = None
                conn.node = node
                return conn
            elif is_server_error(resp.status):
                self.app.error_occurred(
                    node,
                    _('ERROR %(status)d Expect: 100-continue '
                      'From Object Server') % {
                          'status': resp.status})
        except (Exception, Timeout):
            self.app.exception_occurred(
                node, _('Object'),
                _('Expect: 100-continue on %s') % path)


    @public
    @delay_denial
    def DISK_FAILURE(self, req):
        """Handler for HTTP GET/HEAD requests."""
        """
        Handles requests to Disk Failure
        Should return a WSGI-style callable (such as swob.Response).

        :param req: swob.Request object
        """
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index')
        policy = POLICIES.get_by_index(policy_index)
        if not policy:
            # This indicates that a new policy has been created,
            # with rings, deployed, released (i.e. deprecated =
            # False), used by a client to create a container via
            # another proxy that was restarted after the policy
            # was released, and is now cached - all before this
            # worker was HUPed to stop accepting new
            # connections.  There should never be an "unknown"
            # index - but when there is - it's probably operator
            # error and hopefully temporary.
            raise HTTPServiceUnavailable('Unknown Storage Policy')
        obj_ring = self.app.get_object_ring(policy_index)
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        environ = {'from': nodes[0], 'to': nodes[1]}
        headers = self.generate_request_headers(req, additional=environ)
        conn = self.connect_node(nodes[0], headers)
        if HTTPOk != conn.resp:
            return HTTPServiceUnavailable(request = req)
        return HTTPOk(request=req,
                      headers={},
                      body='',
                      content_type='application/json; charset=UTF-8')
