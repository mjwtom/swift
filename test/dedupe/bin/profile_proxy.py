import sys
from swift.common.utils import parse_options
from swift.common.wsgi import run_wsgi
import cProfile


def proxy():
    conf_file, options = parse_options()
    sys.exit(run_wsgi(conf_file, 'proxy-server', **options))

if __name__ == '__main__':
    cmd = ''
    profile_file = '/home/mjwtom/profile_file'
    cProfile.run('proxy()', profile_file)