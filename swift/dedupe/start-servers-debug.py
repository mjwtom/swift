#!/usr/bin/python
import os

if __name__ == '__main__':
    print ('Reseting Swift......')
    os.system('resetswift')
    print ('Done')
    #start proxy-server
    print ('Starting Proxy Server......')
    os.system('/home/mjwtom/swift/bin/swift-proxy-server /etc/swift/proxy-server.conf &')
    print ('Done')
    #start account server
    print ('Starting Account Servers......')
    os.system('/home/mjwtom/swift/bin/swift-account-server /etc/swift/account-server/1.conf &')
    os.system('/home/mjwtom/swift/bin/swift-account-server /etc/swift/account-server/2.conf &')
    os.system('/home/mjwtom/swift/bin/swift-account-server /etc/swift/account-server/3.conf &')
    os.system('/home/mjwtom/swift/bin/swift-account-server /etc/swift/account-server/4.conf &')
    print ('Done')
    #start container server
    print ('Starting Container Servers......')
    os.system('/home/mjwtom/swift/bin/swift-container-server /etc/swift/container-server/1.conf &')
    os.system('/home/mjwtom/swift/bin/swift-container-server /etc/swift/container-server/2.conf &')
    os.system('/home/mjwtom/swift/bin/swift-container-server /etc/swift/container-server/3.conf &')
    os.system('/home/mjwtom/swift/bin/swift-container-server /etc/swift/container-server/4.conf &')
    print ('Done')
    #start object server
    print ('Starting Object Servers......')
    os.system('/home/mjwtom/swift/bin/swift-object-server /etc/swift/object-server/1.conf &')
    os.system('/home/mjwtom/swift/bin/swift-object-server /etc/swift/object-server/2.conf &')
    os.system('/home/mjwtom/swift/bin/swift-object-server /etc/swift/object-server/3.conf &')
    os.system('/home/mjwtom/swift/bin/swift-object-server /etc/swift/object-server/4.conf &')
    print ('Done')
