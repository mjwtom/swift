#!/usr/bin/python
import subprocess

cmd = ['python',
       '/home/mjwtom/PycharmProjects/swift/test/dedupe-aio/bin/testcases.py',
       '/home/mjwtom/kernel.pickle',
       '/home/mjwtom/result']

subprocess.call(cmd)