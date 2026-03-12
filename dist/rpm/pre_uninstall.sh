#!/bin/sh
set -e

# Only run on full removal (not upgrade)
# $1 == 0 means full removal, $1 == 1 means upgrade
if [ "$1" = "0" ]; then
    # Stop and disable service
    if systemctl is-active --quiet runifi 2>/dev/null; then
        systemctl stop runifi || true
    fi
    if systemctl is-enabled --quiet runifi 2>/dev/null; then
        systemctl disable runifi || true
    fi
fi
