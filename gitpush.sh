#!/bin/bash
MSG=${1:-"Update"}
git add .
git commit -m "$MSG"
git push origin main
