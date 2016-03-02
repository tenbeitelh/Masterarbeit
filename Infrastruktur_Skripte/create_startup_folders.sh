#!/bin/bash
ssh root@hueserver.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@datanode1.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@datanode2.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@datanode3.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@datanode4.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@datanode5.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@datanode6.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

ssh root@secondary.hs.osnabrueck.de
mkdir /var/run/knox
mkdir /var/run/flume
exit

