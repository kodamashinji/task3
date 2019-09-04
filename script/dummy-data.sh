#!/bin/sh

curl -X POST -H "Content-Type: application/json" -d '{"user_id":"d3bb7765-30dc-488d-92ac-e3ee33f82afb","location":{"lat_north_south":"N","latitude":"35.7","lon_west_east":"E","longitude":"139.7"},"timestamp":1566745200}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"3e93183e-1e77-4b53-900c-a359410dfd86","location":{"lat_north_south":"N","latitude":"35.71","lon_west_east":"E","longitude":"139.71"},"timestamp":1566777600}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"cb20c69a-28f0-4c54-be47-d02bc60eb09f","location":{"lat_north_south":"N","latitude":"35.72","lon_west_east":"E","longitude":"139.72"},"timestamp":1566831600}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"546150dd-9955-43f8-8afc-6ab7202af45e","location":{"lat_north_south":"S","latitude":"35.72","lon_west_east":"W","longitude":"139.72"},"timestamp":1566864000}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register
curl -X POST -H "Content-Type: application/json" -d '{"user_id":"be016f09-bca4-409e-87f2-e24614042ba2","location":{"lat_north_south":"S","latitude":"35.72","lon_west_east":"W","longitude":"139.72"},"timestamp":1566910472}'  https://gaau8g4xac.execute-api.ap-northeast-1.amazonaws.com/production/register

