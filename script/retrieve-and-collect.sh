#!/bin/sh

if [ $# -lt 1 ]; then
  TARGET=$(date --date 'TZ="Japan/Tokyo" -1 day' '+%Y%m%d')
else
  TARGET=$1
fi

cd "$(dirname $0)" || exit

python3 retrieve_request.py

python3 collect_request.py "${TARGET}"

# aws lambda invoke --function-name stop-collect-server /dev/null