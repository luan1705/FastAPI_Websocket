#!/bin/bash
# Pull code mới và ghi đè toàn bộ folder local
cd $(pwd) || exit
git fetch origin main
git reset --hard origin/main
echo "Pulled latest code from GitHub (overwrite local changes)"
