#!/usr/bin/env bash


ps -aux | grep swift-proxy-server | grep -v grep | cut -c 9-15 | xargs kill -s 9
ps -aux | grep swift-account-server | grep -v grep | cut -c 9-15 | xargs kill -s 9
ps -aux | grep swift-container-server | grep -v grep | cut -c 9-15 | xargs kill -s 9
ps -aux | grep swift-object-server | grep -v grep | cut -c 9-15 | xargs kill -s 9

# remove the database file
rm ~/*.db -rf
rm /etc/swift/*.db -rf

