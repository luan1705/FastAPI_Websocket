#!/bin/bash

# Commit message mặc định "Update"
MSG=${1:-"Update"}

# Thư mục project
cd $(pwd) || exit

git add .
git commit -m "$MSG"
git push origin main
