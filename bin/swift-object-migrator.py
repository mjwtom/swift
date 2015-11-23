#!/usr/bin/env python
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

from swift.obj.migrator import ObjectMigrator
from swift.common.utils import parse_options
from swift.common.daemon import run_daemon
from optparse import OptionParser

if __name__ == '__main__':
    parser = OptionParser("%prog CONFIG [options]")
    parser.add_option('-d', '--devices',
                      help='Replicate only given devices. '
                           'Comma-separated list')
    parser.add_option('-p', '--partitions',
                      help='Replicate only given partitions. '
                           'Comma-separated list')
    parser.add_option('-i', '--policies',
                      help='Replicate only given policy indices. '
                           'Comma-separated list')
    conf_file, options = parse_options(parser=parser, once=True)
    run_daemon(ObjectMigrator, conf_file, **options)
