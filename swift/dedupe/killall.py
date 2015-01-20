#!/usr/bin/python

__author__ = 'mjwtom'

import os

os.system('ps -aux | grep swift-proxy-server | grep -v grep | cut -c 9-15 | xargs kill -s 9')
os.system('ps -aux | grep swift-account-server | grep -v grep | cut -c 9-15 | xargs kill -s 9')
os.system('ps -aux | grep swift-container-server | grep -v grep | cut -c 9-15 | xargs kill -s 9')
os.system('ps -aux | grep swift-object-server | grep -v grep | cut -c 9-15 | xargs kill -s 9')

