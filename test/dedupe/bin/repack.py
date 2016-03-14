#!/usr/bin/python

import subprocess
import os
import sys
import zlib
import pickle


def repack_tar(path):
    if os.path.isfile(path):
        if path.endswith('xz'):
            print 'repackaging file %s' % path
            dir, file = os.path.split(path)
            cmd = ['tar',
                   '-Jxf',
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
                   '-cf',
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
        if not os.path.exists(path):
            print 'no such file' % path
            return 0
        if os.path.isfile(path):
            fin = open(path, 'rb')
            data = fin.read()
            fin.close()
            l = len(data)
            f.write(data)
            size = os.path.getsize(path)
            if l != size:
                print 'wrong read'
            recipe.append((path, offset, l))
            offset += l
            return offset
        else:
            files = os.listdir(path)
            files.sort()
            for file in files:
                subpath = os.path.join(path, file)
                offset = recur_data(subpath, f, recipe, offset)
            return offset

    if not os.path.exists(path):
        print 'no such file' % path
        return 0
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
            recipe = []
            f = open(outfile, 'wb')
            print 'reading data...'
            recur_data(outdir, f, recipe, 0)
            f.close()
            data = pickle.dumps(recipe)
            data = zlib.compress(data)
            outfile = os.path.join(dir, name+'.ZlibCompressedRecipe')
            f = open(outfile, 'wb')
            pickle.dump(data, f)
            f.close()
            newpath = os.path.join(dir, name)
            cmd = ['rm',
                   '-rf',
                   newpath]
            subprocess.call(cmd)
    else:
        files = os.listdir(path)
        files.sort()
        for file in files:
            subpath = os.path.join(path, file)
            stream_data(subpath)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'not enough paramaters'
        exit()
    stream_data(sys.argv[1])