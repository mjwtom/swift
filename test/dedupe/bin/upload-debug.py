#!/home/mjwtom/install/python/bin/python
# -*- coding: utf-8 -*-


import os
import time

if __name__ == '__main__':
    start_time = time.time()
    os.system('/home/mjwtom/bin/swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing upload mjw /home/mjwtom/file')
    end_time = time.time()
    time_used = end_time - start_time
    size = os.path.getsize('/home/mjwtom/file')
    speed = size/time_used/1024/1024
    print ('transfer speed: %f MB/s\n' % speed)
