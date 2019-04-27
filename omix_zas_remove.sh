#!/bin/bash
rm /usr/local/sbin/omix_zas.py
rm  /etc/systemd/system/omix_zas.service
rm /etc/rsyslog.d/omix_zas.conf
rm /etc/logrotate.d/omix_zas
systemctl daemon-reload
systemctl restart rsyslog
