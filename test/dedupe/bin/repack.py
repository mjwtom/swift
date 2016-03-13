#!/usr/bin/python

import subprocess
import os
import sys


def repack(path):
    if os.path.isfile(path):
        if path.endswith('xz'):
            print 'repackaging file %s' % path
            dir, file = os.path.split(path)
            cmd = ['tar',
                   '-Jxvf',
                   path,
                   '-C',
                   dir]
            subprocess.call(cmd)
            print 'sucessully upacke %s' % path
            name = file.split('.')
            name = name[:-2]
            name = '.'.join(name)
            newpath = os.path.join(dir, name)
            cmd = ['tar',
                   '-cvf',
                   newpath+'.tar',
                   newpath]
            subprocess.call(cmd)
            cmd = ['rm',
                   '-rf',
                   newpath]
            subprocess.call(cmd)
    else:
        for f in os.listdir(path):
            subpath = os.path.join(path, f)
            repack(subpath)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'not enough paramaters'
        exit()
    repack(sys.argv[1])