#!/bin/bash
install -o root -g root -m 744 omix_rep.py /usr/local/sbin/omix_rep.py
install -o root -g root -m 744 omix_ssh-copy-id.py /usr/local/sbin/omix_ssh-copy-id.py
install -o root -g root -m 644 omix_rep.service /etc/systemd/system/omix_rep.service
install -o root -g root -m 644 omix_rep.syslog /etc/rsyslog.d/omix_rep.conf
install -o root -g root -m 644 omix_rep.logrotate /etc/logrotate.d/omix_rep
install -o root -g root -m 644 -d /var/log/omix_replicate
systemctl daemon-reload
systemctl restart rsyslog
systemctl enable omix_rep
systemctl start omix_rep
