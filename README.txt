ディレクトリ構成

task3/
  aurora/                           --- RDBに関する実装
    ddl.sql                         --- Aurora MySQL用のDDL (今回は使用していない)
  doc/                              --- 各種ドキュメント
    aws.txt                         --- 今回設定したaws設定のメモ
    arch.pdf                        --- arch.pptxのPDF
    arch.pptx                       --- 全体設計を説明するPowerPoint
    randomnote.txt                  --- 思いついたものをひとまず書き留めているメモ
    requirement.txt                 --- オリジナルの要求仕様
    specification.txt               --- 実装に使用した要求仕様 (オリジナルに変更追加を行った内容)
  lambda/                           --- Lambda用ソース (python3)
    get_location_list.py            --- 結果をダウンロードするためのAPI Gatewayから呼び出されるlambda
    parse_request.py                --- JSONデータを処理してSQSにリクエストを積むlambda。API Gateway経由で呼び出される
    start_collect_server.py         --- ec2(加工用サーバ)立ち上げ用lambda
    stop_collect_server.py          --- ec2(加工用サーバ)終了用lambda
    store_request.py                --- SQSからリクエストを取り出し、まとめてS3に書き出す1lambda。数分毎に呼び出される
    test/
      test_get_location_list.py     --- get_location_list.pyのテストファイル
      test_parse_request.py         --- parse_request.pyのテストファイル
      test_store_request.py         --- store_request.pyのテストファイル
  script/                           --- 各種プログラム (python3, shell-script)
    collect_request.py              --- Redshiftから指定日のレコードを読み込み、CSVとしてS3のダウンロード可能なフォルダに書き出す。
                                        加工サーバ内で日付変更後に呼び出される。
    get_connection_string.py        --- collect, retrieve共通のRedshift接続文字列取得用スクリプト
    retrieve-and-collect.sh         --- retrieve_request, collect_requestを呼び出した後、Lambda stop-collect-serverを呼び出すスクリプト
    retrieve_request.py             --- S3のファイルを読み込み、レコードを日付毎に分けて別フォルダに書き出すプログラム。
                                        書き出されたファイルはRedshift spectrumから参照される。加工サーバ内で日に数回呼び出される。
    test/
      test_collect_request.py       --- collect_request.pyのテストファイル
      test_get_connection_string.py --- get_connection_string.pyのテストファイル
      test_retrieve_request.py      --- retrieve_request.pyのテストファイル
      dummy-data.sh                 --- ダミーデータ投入用スクリプト
      dummy_data_maker.py           --- ダミーデータ投入用スクリプト生成用スクリプト
  redshift/                         --- Redshiftに関する実装
    ddl.sql                         --- Redshiftに投入されるDDL
