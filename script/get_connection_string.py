# coding=utf-8

"""
retrieve_request, collect_request共通で使用するRedshiftへの接続文字列取得関数
"""

import os


def get_connection_string(config_file: str = None) -> str:
    """
    Redshiftへのコネクション設定ファイル取得

    Returns
    -------
    str
        $HOME/.pgpassの先頭行を取得して返す
    """
    if config_file is None:
        config_file = os.environ.get('HOME', '/root') + '/.pgpass'
    with open(config_file, 'r') as fd:
        for line in fd:
            s_line = line.strip()
            if len(s_line) == 0 or s_line[0] == '#':
                continue
            host, port, dbname, user, password = s_line.split(':')
            return 'dbname={0} user={1} password={2} host={3} port={4}'.format(dbname, user, password, host, port)
