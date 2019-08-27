ディレクトリ構成

task3/
  aurora/                    --- RDBに関する実装 (今回は使用しないかも?)
  doc/                       --- 各種ドキュメント
    aws.txt                  --- 今回設定したaws設定のメモ
    randomnote.txt           --- 思いついたものをひとまず書き留めているメモ
    requirement.txt          --- オリジナルの要求仕様
    specification.txt        --- 実装に使用した要求仕様 (オリジナルに変更追加を行った内容)
  python3/                   --- 各種プログラム (lambdaと加工用サーバで使用するものが混在している)
    start-collect-server.py  --- ec2(加工用サーバ)立ち上げ用lambda
    stop-collect-server.py   --- ec2(加工用サーバ)終了用lambda
    parse-request.py         --- JSONデータを処理してSQSにリクエストを積むlambda。API Gateway経由で呼び出される
    store-request.py         --- SQSからリクエストを取り出し、まとめてS3に書き出す1lambda。数分毎に呼び出される
    retrieve-request.py      --- S3のファイルを読み込み、レコードを日付毎に分けて別フォルダに書き出すプログラム。
                                 書き出されたファイルはRedshift spectrumから参照される。加工サーバ内で日に数回呼び出される。
    collect-request.py       --- Redshiftから指定日のレコードを読み込み、CSVとしてS3のダウンロード可能なフォルダに書き出す。
                                 加工サーバ内で日付変更後に呼び出される。
  redshift/                  --- Redshiftに関する実装
    ddl.sql                  --- Redshiftに投入されるDDL
