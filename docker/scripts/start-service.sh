#!/bin/bash

# supervisor will start up and watch the services
/usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf
