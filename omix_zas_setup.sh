#!/bin/bash
apt-get install python3-dateutil
install -o root -g root -m 744 omix_zas.py /usr/local/sbin/omix_zas.py
install -o root -g root -m 644 omix_zas.service /etc/systemd/system/omix_zas.service
install -o root -g root -m 644 omix_zas.syslog /etc/rsyslog.d/omix_zas.conf
install -o root -g root -m 644 omix_zas.logrotate /etc/logrotate.d/omix_zas
systemctl daemon-reload
systemctl restart rsyslog
systemctl enable omix_zas
systemctl start omix_zas
