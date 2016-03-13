#!/usr/bin/python

import subprocess
import os
import sys


def repack_tar(path):
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
            print 'sucessully upackage %s' % path
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
            repack_tar(subpath)

def stream_data(path):

    def recur_data(path, f, recipe, offset):
        if os.path.isfile(path):
            fin = open(path, 'rb')
            data = fin.read()
            l = len(data)
            f.write(data)
            recipe.append((path, offset, l))
            offset += l
            return offset

    if os.path.isfile(path):
        if path.endswith('xz'):
            print 'unpackaging file %s' % path
            dir, filename = os.path.split(path)
            cmd = ['tar',
                   '-Jxf',
                   path,
                   '-C',
                   dir]
            subprocess.call(cmd)
            print 'successfully unpackage %s' % path
            name = filename.split('.')
            name = name[:-2]
            name = '.'.join(name)
            outfile = os.path.join(dir, name+'.data')
            outdir = os.path.join(dir, name)
            f = open(outfile, 'wb')
            recur_data(outdir, f)
            f.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'not enough paramaters'
        exit()
    repack(sys.argv[1])