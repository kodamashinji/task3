#!/bin/sh

curl -X POST -H "Content-Type: application/json" -d '{"user_id":"hogehoge","location":{"lat_north_south":"N","latitude":"35.7","lon_west_east":"E","longitude":"139.7"},"timestamp":1555055157}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"hogehoge2","location":{"lat_north_south":"N","latitude":"35.71","lon_west_east":"E","longitude":"139.71"},"timestamp":1555055160}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"hogehoge3","location":{"lat_north_south":"N","latitude":"35.72","lon_west_east":"E","longitude":"139.72"},"timestamp":1555055150}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"hogehoge","location":{"lat_north_south":"S","latitude":"35.72","lon_west_east":"W","longitude":"139.72"},"timestamp":1555055250}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register

