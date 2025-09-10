#!/bin/bash
# Pull code mới, tạo backup main và checkout commit/branch
cd $(pwd) || exit

# Commit hoặc branch muốn checkout (mặc định là main)
TARGET=${1:-main}

# Pull code mới từ GitHub
git fetch origin main

# Tạo backup branch giữ code mới
git branch -f backup-main main

# Checkout commit hoặc branch muốn test
git checkout "$TARGET"

echo "Checked out $TARGET. To return to main: git checkout main"
echo "Backup branch with latest main is 'backup-main'"
