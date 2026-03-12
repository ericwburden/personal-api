#!/usr/bin/env bash
set -e

mkdir -p $HOME/backups

tar -czf \
  $HOME/backups/personal-data-$(date +%F-%H%M).tar.gz \
  $HOME/personal-data

find $HOME/backups -mtime +7 -delete
