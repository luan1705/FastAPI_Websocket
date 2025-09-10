#!/bin/bash
# Pull code mới từ GitHub mà không ghi đè thay đổi local
cd $(pwd) || exit
git pull origin main
echo "Pulled latest code from GitHub (merge with local changes)"
