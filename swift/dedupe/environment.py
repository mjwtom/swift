#!/usr/bin/python

'''
I want to use this script to complete all the work. But now I still do not know how to do it. So, I have some work to be done by hand

1.Edit /etc/fstab and add:

/dev/sdb1 /mnt/sdb1 xfs noatime,nodiratime,nobarrier,logbufs=8 0 0

2.Add the following lines to /etc/rc.local (before the exit 0):

mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
chown <your-user-name>:<your-group-name> /var/cache/swift*
mkdir -p /var/run/swift
chown <your-user-name>:<your-group-name> /var/run/swift

3.On Ubuntu, edit the following line in /etc/default/rsync:

RSYNC_ENABLE=true

4.Edit /etc/rsyslog.conf and make the following change (usually in the “GLOBAL DIRECTIVES” section):

$PrivDropToGroup adm


'''

import os

if __name__ == '__main__':
    print ('updating the software ......')

    os.system('sudo apt-get update')
    os.system('sudo apt-get install curl gcc memcached rsync sqlite3 xfsprogs \
                     git-core libffi-dev python-setuptools')
    os.system('sudo apt-get install python-coverage python-dev python-nose \
                     python-simplejson python-xattr python-eventlet \
                     python-greenlet python-pastedeploy \
                     python-netifaces python-pip python-dnspython \
                     python-mock')

    print('creating the file for the loopback device ......')
    os.system('sudo mkdir /srv')
    os.system('sudo truncate -s 1GB /srv/swift-disk')
    os.system('sudo mkfs.xfs /srv/swift-disk')

    # do step 1 here

    os.system('sudo mkdir /mnt/sdb1')
    os.system('sudo mount /mnt/sdb1')
    os.system('sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4')
    os.system('sudo chown ${USER}:${USER} /mnt/sdb1/*')
    os.system('for x in {1..4}; do sudo ln -s /mnt/sdb1/$x /srv/$x; done')
    os.system('sudo mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5 \
              /srv/2/node/sdb2 /srv/2/node/sdb6 \
              /srv/3/node/sdb3 /srv/3/node/sdb7 \
              /srv/4/node/sdb4 /srv/4/node/sdb8 \
              /var/run/swift')
    os.system('sudo chown -R ${USER}:${USER} /var/run/swift')
    # **Make sure to include the trailing slash after /srv/$x/**
    os.system('for x in {1..4}; do sudo chown -R ${USER}:${USER} /srv/$x/; done')

    # do step 2 here
    '''
    2.Add the following lines to /etc/rc.local (before the exit 0):

    mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
    chown <your-user-name>:<your-group-name> /var/cache/swift*
    mkdir -p /var/run/swift
    chown <your-user-name>:<your-group-name> /var/run/swift
    '''

    os.system('cd $HOME/PycharmProjects/swift; sudo pip install -r requirements.txt; sudo python setup.py develop;')
    os.system('sudo pip install -U xattr')
    os.system('cd $HOME/PycharmProjects/swift; sudo pip install -r test-requirements.txt')

    os.system('sudo cp $HOME/PycharmProjects/swift/doc/saio/rsyncd.conf /etc/')
    os.system('sudo sed -i "s/<your-user-name>/${USER}/" /etc/rsyncd.conf')

    # do step 3 here
    os.system('sudo service rsync restart')
    os.system('rsync rsync://pub@localhost/')

    os.system('sudo service memcached start')
    os.system('sudo chkconfig memcached on')
    os.system('sudo cp $HOME/PycharmProjects/swift/doc/saio/rsyslog.d/10-swift.conf /etc/rsyslog.d/')

    # do step 4 here

    os.system('sudo mkdir -p /var/log/swift/hourly')

    os.system('sudo chown -R syslog.adm /var/log/swift')
    os.system('sudo chmod -R g+w /var/log/swift')
    os.system('sudo service rsyslog restart')

    os.system('sudo rm -rf /etc/swift')
    os.system('cd $HOME/PycharmProjects/swift/doc; sudo cp -r saio/swift /etc/swift; cd -')
    os.system('sudo chown -R ${USER}:${USER} /etc/swift')

    os.system('find /etc/swift/ -name \*.conf | xargs sudo sed -i "s/<your-user-name>/${USER}/"')

    os.system('mkdir -p $HOME/bin')
    os.system('cd $HOME/PycharmProjects/swift/doc; cp saio/bin/* $HOME/bin; cd -')
    os.system('chmod +x $HOME/bin/*')

    os.system('echo "export SAIO_BLOCK_DEVICE=/srv/swift-disk" >> $HOME/.bashrc')
    os.system('sed -i "/find \/var\/log\/swift/d" $HOME/bin/resetswift')
    os.system('sed -i "s/service \(.*\) restart/systemctl restart \1.service/" $HOME/bin/resetswift')
    os.system('cp $HOME/swift/test/sample.conf /etc/swift/test.conf')
    os.system('sudo cp $HOME/PycharmProjects/swift/test/sample.conf /etc/swift/test.conf')
    os.system('echo "export SWIFT_TEST_CONFIG_FILE=/etc/swift/test.conf" >> $HOME/.bashrc')
    os.system('echo "export PATH=${PATH}:$HOME/bin" >> $HOME/.bashrc')
    os.system('. $HOME/.bashrc')
    os.system('remakerings')
