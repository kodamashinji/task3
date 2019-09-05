# coding=utf-8

"""
dummyデータを投入するためのスクリプトを出力するスクリプト
"""

import sys
import time
import datetime
import uuid
import random

tz = 9 * 60 * 60


def make_dummy_data(count: int, from_time: int, to_time: int) -> None:
    user_id_list = [str(uuid.uuid4()) for _ in range(count)]
    for _ in range(count):
        user_id = random.choice(user_id_list)

        # 東京23区の緯度経度
        # 西 (練馬区) 139.33.46  = 139.5627777777778
        # 東 (江戸川区) 139.55.07 = 139.9186111111111
        # 北 (足立区) 35.49.04 = 35.81777777777778
        # 南 (大田区) 35.46.48 = 35.78

        latitude = random.uniform(35.78, 35.81777777777778)
        longitude = random.uniform(139.5627777777778, 139.9186111111111)

        timestamp = random.randint(from_time, to_time)

        print('curl -X POST -H "Content-Type: application/json" -d \
              \'{{"user_id":"{user_id}","location": \
              {{"lat_north_south":"N","latitude":"{latitude}","lon_west_east":"E","longitude":"{longitude}"}},\
               "timestamp":{timestamp}}}\' \
              https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register'
              .format(user_id=user_id, longitude=longitude, latitude=latitude, timestamp=timestamp))


if __name__ == "__main__":
    if len(sys.argv) == 1:  # 引数なしの場合は昨日のデータ
        print("dummy_data_maker.py 件数 [開始日付(YYYYMMDD)] [終了日付(YYYYMMDD)]")
        print("終了日付を省略した場合は、開始日付と同じ日付になります")
        print("開始日付を省略した場合は、対象は昨日になります")
        sys.exit(0)

    count = int(sys.argv[1])

    if len(sys.argv) < 3:
        local_time = int(time.time()) + tz
        from_time = local_time - (local_time % (60 * 60 * 24)) - (60 * 60 * 24) - tz
    else:
        from_time = int(datetime.datetime.strptime(sys.argv[2], '%Y%m%d').timestamp())

    if len(sys.argv) < 4:
        to_time = from_time + 24 * 60 * 60 - 1
    else:
        to_time = int(datetime.datetime.strptime(sys.argv[3], '%Y%m%d').timestamp()) + 24 * 60 * 60 - 1

    make_dummy_data(count, from_time, to_time)
