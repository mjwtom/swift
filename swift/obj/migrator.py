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

import os
import errno
from os.path import isdir, isfile, join, dirname
import random
import shutil
import time
import itertools
from six import viewkeys
import six.moves.cPickle as pickle
from swift import gettext_ as _

import eventlet
from eventlet import GreenPool, tpool, Timeout, sleep, hubs
from eventlet.green import subprocess
from eventlet.support.greenlets import GreenletExit

from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, unlink_older_than, \
    compute_eta, get_logger, dump_recon_cache, ismount, \
    rsync_module_interpolation, mkdirs, config_true_value, list_from_csv, \
    get_hub, tpool_reraise, config_auto_int_value, storage_directory
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.obj import ssync_sender
from swift.obj.diskfile import DiskFileManager, get_data_dir, get_tmp_dir
from swift.common.storage_policy import POLICIES, DEDUPE_POLICY

from swift.common.utils import hash_path


hubs.use_hub(get_hub())


class ObjectMigrator(Daemon):
    """
    Migrate objects.

    Encapsulates most logic and data needed by the object migration process.
    Each call to .replicate() performs one replication pass.  It's up to the
    caller to do this in a loop.
    """

    def __init__(self, conf, logger=None):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: logging object
        """
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='object-migrator')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.bind_ip = conf.get('bind_ip', '0.0.0.0')
        self.servers_per_port = int(conf.get('servers_per_port', '0') or 0)
        self.port = None if self.servers_per_port else \
            int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.stats_interval = int(conf.get('stats_interval', '300'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.partition_times = []
        self.interval = int(conf.get('interval') or
                            conf.get('run_pause') or 30)
        self.rsync_timeout = int(conf.get('rsync_timeout', 900))
        self.rsync_io_timeout = conf.get('rsync_io_timeout', '30')
        self.rsync_bwlimit = conf.get('rsync_bwlimit', '0')
        self.rsync_compress = config_true_value(
            conf.get('rsync_compress', 'no'))
        self.rsync_module = conf.get('rsync_module', '').rstrip('/')
        if not self.rsync_module:
            self.rsync_module = '{replication_ip}::object'
            if config_true_value(conf.get('vm_test_mode', 'no')):
                self.logger.warn('Option object-migrator/vm_test_mode is '
                                 'deprecated and will be removed in a future '
                                 'version. Update your configuration to use '
                                 'option object-replicator/rsync_module.')
                self.rsync_module += '{replication_port}'
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.sync_method = getattr(self, conf.get('sync_method') or 'rsync')
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.default_headers = {
            'Content-Length': '0',
            'user-agent': 'object-migrator %s' % os.getpid()}
        self.rsync_error_log_line_length = \
            int(conf.get('rsync_error_log_line_length', 0))
        self._diskfile_mgr = DiskFileManager(conf, self.logger)

    def _zero_stats(self):
        """Zero out the stats."""
        self.stats = {'attempted': 0, 'success': 0, 'failure': 0,
                      'hashmatch': 0, 'rsync': 0,
                      'start': time.time(), 'failure_nodes': {}}

    def _add_failure_stats(self):
        self.stats['failure'] += 1

    def _get_my_replication_ips(self):
        my_replication_ips = set()
        ips = whataremyips()
        for policy in POLICIES:
            self.load_object_ring(policy)
            for local_dev in [dev for dev in policy.object_ring.devs
                              if dev and dev['replication_ip'] in ips and
                              dev['replication_port'] == self.port]:
                my_replication_ips.add(local_dev['replication_ip'])
        return list(my_replication_ips)

    # Just exists for doc anchor point
    def sync(self, node, job, suffixes, *args, **kwargs):
        """
        Synchronize local suffix directories from a partition with a remote
        node.

        :param node: the "dev" entry for the remote node to sync with
        :param job: information about the partition being synced
        :param suffixes: a list of suffixes which need to be pushed

        :returns: boolean and dictionary, boolean indicating success or failure
        """
        return self.sync_method(node, job, suffixes, *args, **kwargs)

    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """
        policy.load_ring(self.swift_dir)
        return policy.object_ring

    def _rsync(self, args):
        """
        Execute the rsync binary to replicate a partition.

        :returns: return code of rsync process. 0 is successful
        """
        start_time = time.time()
        ret_val = None
        try:
            with Timeout(self.rsync_timeout):
                proc = subprocess.Popen(args,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
                results = proc.stdout.read()
                ret_val = proc.wait()
        except Timeout:
            self.logger.error(_("Killing long-running rsync: %s"), str(args))
            proc.kill()
            return 1  # failure response code
        total_time = time.time() - start_time
        for result in results.split('\n'):
            if result == '':
                continue
            if result.startswith('cd+'):
                continue
            if not ret_val:
                self.logger.info(result)
            else:
                self.logger.error(result)
        if ret_val:
            error_line = _('Bad rsync return code: %(ret)d <- %(args)s') % \
                {'args': str(args), 'ret': ret_val}
            if self.rsync_error_log_line_length:
                error_line = error_line[:self.rsync_error_log_line_length]
            self.logger.error(error_line)
        elif results:
            self.logger.info(
                _("Successful rsync of %(src)s at %(dst)s (%(time).03f)"),
                {'src': args[-2], 'dst': args[-1], 'time': total_time})
        else:
            self.logger.debug(
                _("Successful rsync of %(src)s at %(dst)s (%(time).03f)"),
                {'src': args[-2], 'dst': args[-1], 'time': total_time})
        return ret_val

    def rsync(self, node, job, suffixes):
        """
        Uses rsync to implement the sync method. This was the first
        sync method in Swift.
        """
        if not os.path.exists(job['path']):
            return False, {}
        args = [
            'rsync',
            '--recursive',
            '--whole-file',
            '--human-readable',
            '--xattrs',
            '--itemize-changes',
            '--ignore-existing',
            '--timeout=%s' % self.rsync_io_timeout,
            '--contimeout=%s' % self.rsync_io_timeout,
            '--bwlimit=%s' % self.rsync_bwlimit,
        ]
        if self.rsync_compress and \
                job['region'] != node['region']:
            # Allow for compression, but only if the remote node is in
            # a different region than the local one.
            args.append('--compress')
        rsync_module = rsync_module_interpolation(self.rsync_module, node)
        had_any = False
        for suffix in suffixes:
            spath = join(job['path'], suffix)
            if os.path.exists(spath):
                args.append(spath)
                had_any = True
        if not had_any:
            return False, {}
        data_dir = get_data_dir(job['policy'])
        args.append(join(rsync_module, node['device'],
                    data_dir, job['partition']))
        return self._rsync(args) == 0, {}

    def ssync(self, node, job, suffixes, remote_check_objs=None):
        return ssync_sender.Sender(
            self, node, job, suffixes, remote_check_objs)()

    def _migrate(self, job):
        """
        High-level method that replicates a single partition that doesn't
        belong on this node.

        :param job: a dict containing info about the partition to be replicated
        """

        self.migration_count += 1
        self.logger.increment('partition.delete.count.%s' % (job['device'],))
        headers = dict(self.default_headers)
        headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        failure_devs_info = set()
        begin = time.time()
        try:
            responses = []
            synced_remote_regions = {}
            delete_objs = None
            for node in job['nodes']:
                self.stats['rsync'] += 1
                kwargs = {}
                if node['region'] in synced_remote_regions and \
                        self.conf.get('sync_method', 'rsync') == 'ssync':
                    kwargs['remote_check_objs'] = \
                        synced_remote_regions[node['region']]
                # candidates is a dict(hash=>timestamp) of objects
                # for deletion
                success, candidates = self.sync(
                    node, job, job['path'], **kwargs)
                if success:
                    with Timeout(self.http_timeout):
                        conn = http_connect(
                            node['replication_ip'],
                            node['replication_port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '/', headers=headers) #'/' + '-'.join(suffixes), headers=headers)
                        conn.getresponse().read()
                    if node['region'] != job['region']:
                        synced_remote_regions[node['region']] = viewkeys(
                            candidates)
                else:
                    failure_devs_info.add((node['replication_ip'],
                                           node['device']))
                responses.append(success)

        except (Exception, Timeout):
            self.logger.exception(_("Error syncing handoff partition"))
        finally:
            target_devs_info = set([(target_dev['replication_ip'],
                                     target_dev['device'])
                                    for target_dev in job['nodes']])
            self.stats['success'] += len(target_devs_info - failure_devs_info)
            self._add_failure_stats(failure_devs_info)
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.delete.timing', begin)

    def stats_line(self):
        """
        Logs various stats for the currently running replication pass.
        """
        if self.migration_count:
            elapsed = (time.time() - self.start) or 0.000001
            rate = self.migration_count / elapsed
            self.logger.info(
                _("%(replicated)d/%(total)d (%(percentage).2f%%)"
                  " partitions replicated in %(time).2fs (%(rate).2f/sec, "
                  "%(remaining)s remaining)"),
                {'replicated': self.migration_count, 'total': self.job_count,
                 'percentage': self.migration_count * 100.0 / self.job_count,
                 'time': time.time() - self.start, 'rate': rate,
                 'remaining': '%d%s' % compute_eta(self.start,
                                                   self.migration_count,
                                                   self.job_count)})
            if self.suffix_count:
                self.logger.info(
                    _("%(checked)d suffixes checked - "
                      "%(hashed).2f%% hashed, %(synced).2f%% synced"),
                    {'checked': self.suffix_count,
                     'hashed': (self.suffix_hash * 100.0) / self.suffix_count,
                     'synced': (self.suffix_sync * 100.0) / self.suffix_count})
                self.partition_times.sort()
                self.logger.info(
                    _("Partition times: max %(max).4fs, "
                      "min %(min).4fs, med %(med).4fs"),
                    {'max': self.partition_times[-1],
                     'min': self.partition_times[0],
                     'med': self.partition_times[
                         len(self.partition_times) // 2]})
        else:
            self.logger.info(
                _("Nothing replicated for %s seconds."),
                (time.time() - self.start))

    def kill_coros(self):
        """Utility function that kills all coroutines currently running."""
        for coro in list(self.run_pool.coroutines_running):
            try:
                coro.kill(GreenletExit)
            except GreenletExit:
                pass

    def heartbeat(self):
        """
        Loop that runs in the background during replication.  It periodically
        logs progress.
        """
        while True:
            eventlet.sleep(self.stats_interval)
            self.stats_line()

    def detect_lockups(self):
        """
        In testing, the pool.waitall() call very occasionally failed to return.
        This is an attempt to make sure the replicator finishes its replication
        pass in some eventuality.
        """
        while True:
            eventlet.sleep(self.lockup_timeout)
            if self.replication_count == self.last_replication_count:
                self.logger.error(_("Lockup detected.. killing live coros."))
                self.kill_coros()
            self.last_replication_count = self.replication_count

    def build_migration_jobs(self, policy, partition, nodes, account_name, container_name, object_name, remote):
        """
        Helper function for collect_jobs to build jobs for replication
        using replication style storage policy
        """
        ips = whataremyips(self.bind_ip)

        local_dev = [d for d in policy.object_ring.devs if (d and
                                                            is_local_device(
                                                                ips,
                                                                self.port,
                                                                d['replication_ip'],
                                                                d['replication_port']
                                                            ))]

        if account_name and container_name and object_name:
            name_hash = hash_path(account_name, container_name, object_name)

        jobs = []
        data_dir = get_data_dir(policy)
        for dev in local_dev:
            for node in nodes:
                if node['device'] == dev['device']:
                    dev_path = join(self.devices_dir, node['device'])
                    objs_path = join(dev_path, data_dir)
                    tmp_path = join(dev_path, get_tmp_dir(policy))
                    job_path = obj_dir = join(
                        dev_path, storage_directory(data_dir,
                                                       partition, name_hash))

                    if self.mount_check and not ismount(job_path):
                        self._add_failure_stats()
                    try:
                        jobs.append(
                            dict(path=job_path,
                                 device=node['device'],
                                 obj=obj_dir,
                                 nodes = nodes,
                                 policy=policy,
                                 partition=partition,
                                 region=dev['region']))
                    except ValueError:
                            self._add_failure_stats()
        return jobs

    def collect_jobs(self, policy_idx, account_name, container_name, object_name, dev):
        """
        Returns a sorted list of jobs (dictionaries) that specify the
        partitions, nodes, etc to be rsynced. (mjwtom: changed to return object)

        :param override_devices: if set, only jobs on these devices
            will be returned
        :param override_partitions: if set, only jobs on these partitions
            will be returned
        :param override_policies: if set, only jobs in these storage
            policies will be returned
        """
        policy = POLICIES.get_by_index(policy_idx)
        self.load_object_ring(policy)
        partition, nodes = policy.object_ring.get_nodes(
        account_name, container_name, object_name)

        jobs = []
        ips = whataremyips(self.bind_ip)
        jobs += self.build_migration_jobs(policy, partition, nodes, account_name, container_name, object_name, dev)
        random.shuffle(jobs)
        self.job_count = len(jobs)
        return jobs

    def migrate(self, policy_idx, account_name, container_name, object_name, dev):
        """Run a migration pass"""
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.migration_count = 0
        self.my_replication_ips = self._get_my_replication_ips()
        self.all_devs_info = set()

        stats = eventlet.spawn(self.heartbeat)
        lockup_detector = eventlet.spawn(self.detect_lockups)
        eventlet.sleep()  # Give spawns a cycle

        current_nodes = None
        try:
            self.run_pool = GreenPool(size=self.concurrency)
            jobs = self.collect_jobs(policy_idx, account_name, container_name, object_name, dev)
            for job in jobs:
                if self.mount_check and not ismount(job['obj']):
                    self._add_failure_stats()
                    self.logger.warn(_('%s is not mounted'), job['device'])
                    continue
                try:
                    if isfile(job['path']):
                        # Clean up any (probably zero-byte) files where a
                        # partition should be.
                        self.logger.warning(
                            'Removing partition directory '
                            'which was a file: %s', job['path'])
                        os.remove(job['path'])
                        continue
                except OSError:
                    continue
                self._migrate(job)  # self.run_pool.spawn(self._migrate, job)
            with Timeout(self.lockup_timeout):
                self.run_pool.waitall()
        except (Exception, Timeout):
            if current_nodes:
                self._add_failure_stats()
            else:
                self._add_failure_stats()
            self.logger.exception(_("Exception in top-level replication loop"))
            self.kill_coros()
        finally:
            stats.kill()
            lockup_detector.kill()
            self.stats_line()
            self.stats['attempted'] = self.migration_count

    def run_once(self, *args, **kwargs):
        self._zero_stats()
        self.logger.info(_("Running object migration in script mode."))

        dev = list_from_csv(kwargs.get('device'))
        account_name = list_from_csv(kwargs.get('account'))
        container_name = list_from_csv(kwargs.get('container'))
        object_name = list_from_csv(kwargs.get('object'))
        policy_idx = list_from_csv(kwargs.get('policy'))

        if dev[0]:
            dev = int(dev[0])
        if policy_idx[0]:
            policy_idx = int(policy_idx[0])
        if account_name[0]:
            account_name = account_name[0]
        if container_name[0]:
            container_name = container_name[0]
        if object_name[0]:
            object_name = object_name[0]

        # Run the migrator
        self.migrate(policy_idx, account_name, container_name, object_name, dev)
        total = (time.time() - self.stats['start']) / 60
        self.logger.info(
            _("Object migration complete (once). (%.02f minutes)"), total)
        if not dev:
            migration_last = time.time()
            dump_recon_cache({'migration_stats': self.stats,
                              'migration_time': total,
                              'migration_last': migration_last,
                              'object_migration_time': total,
                              'object_migration_last': migration_last},
                             self.rcache, self.logger)

    def run_forever(self, *args, **kwargs):
        self.logger.info(_("Starting object migration in daemon mode."))
        # Run the replicator continually
        while True:
            self._zero_stats()
            self.logger.info(_("Starting object migration pass."))
            # Run the migrator
            self.migrate()
            total = (time.time() - self.stats['start']) / 60
            self.logger.info(
                _("Object migration complete. (%.02f minutes)"), total)
            migration_last = time.time()
            dump_recon_cache({'migration_stats': self.stats,
                              'migration_time': total,
                              'migration_last': migration_last,
                              'object_migration_time': total,
                              'object_migration_last': migration_last},
                             self.rcache, self.logger)
            self.logger.debug('Migration sleeping for %s seconds.',
                              self.interval)
            sleep(self.interval)
