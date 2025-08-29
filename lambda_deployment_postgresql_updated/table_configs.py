"""
テーブル設定定義
SQL Server → PostgreSQL データ同期処理用のテーブル設定
"""

# テーブル設定辞書
TABLE_CONFIGS = {
    # 既存テーブル: Customer (McTM)
    'customer': {
        'db_type': 'mctm',
        'sql_server_db': 'McTM',
        'sql_table': '[McTM].[dbo].[Customer]',
        'pg_table': 'mctm_customer',
        'columns': [
            'CD', 'Name', 'NameYomi', 'Dairiten_ID', 
            'CreationTime', 'ModifiedTime', 'Note'
        ],
        'pg_columns': [
            'cd', 'name', 'nameyomi', 'dairiten_id',
            'creationtime', 'modifiedtime', 'note'
        ],
        'primary_key': 'CD',
        'order_by': 'CD',
        'batch_size': 10000,
        'description': 'McTM顧客マスタ'
    },
    
    # 新規テーブル1: McModule (McTM)
    'mctm_module': {
        'db_type': 'mctm',
        'sql_server_db': 'McTM',
        'sql_table': '[McTM].[dbo].[McModule]',
        'pg_table': 'mctm_module',
        'columns': [
            'ID', 'IPAddress', 'Carrier', 'Tel', 'SIMNo', 'Customer_CD',
            'IsDeleted', 'CreationTime', 'ModifiedTime', 'DeletedTime', 'PSID',
            'IIJ_SvcCode1', 'IIJ_SvcCode2', 'PPP_ID', 'PPP_Pass',
            'VersionInfo1', 'VersionInfo2', 'VersionInfo3', 'VersionInfo4',
            'VersionInfo5', 'ReceivedTime'
        ],
        'pg_columns': [
            'id', 'ipaddress', 'carrier', 'tel', 'simno', 'customer_cd',
            'isdeleted', 'creationtime', 'modifiedtime', 'deletedtime', 'psid',
            'iij_svccode1', 'iij_svccode2', 'ppp_id', 'ppp_pass',
            'versioninfo1', 'versioninfo2', 'versioninfo3', 'versioninfo4',
            'versioninfo5', 'receivedtime'
        ],
        'primary_key': 'ID',
        'order_by': 'ID',
        'batch_size': 10000,
        'description': 'McTMモジュール管理'
    },
    
    # 新規テーブル2: Customer (VoipDB)
    'voipdb_customer': {
        'db_type': 'voipdb',
        'sql_server_db': 'VoipDB',
        'sql_table': '[VoipDB].[dbo].[Customer]',
        'pg_table': 'voipdb_customer',
        'columns': [
            'Cd', 'GlobalCustomerCd', 'CustomerName', 'SessionServerAddress',
            'SessionServerTcpPort', 'SessionServerUdpPort', 'LicenseCount',
            'LicenseCountForSmapho', 'LicenseSerial', 'MaintenancePasswd',
            'MutsuuwaDisconnectTime', 'RouteMode', 'MaxRecieveChannel',
            'VPTSetting_ID', 'ShuhenDistance', 'DisconnectMode', 'IsSuspended',
            'RelayBufferSize', 'MaxRelayMemberCount', 'MaxJoinChannelCount',
            'AttributeJson', 'SipTranscoderId1', 'SipTranscoderId2',
            'LicenseCountForSmaphoTrial', 'SmaphoTrialFrom', 'SmaphoTrialTo',
            'MaxAvailableGroupCd', 'LicenseCountForMaintenance'
        ],
        'pg_columns': [
            'cd', 'globalcustomercd', 'customername', 'sessionserveraddress',
            'sessionservertcpport', 'sessionserverudpport', 'licensecount',
            'licensecountforsmapho', 'licenseserial', 'maintenancepasswd',
            'mutsuuwadisconnecttime', 'routemode', 'maxrecievechannel',
            'vptsetting_id', 'shuhendistance', 'disconnectmode', 'issuspended',
            'relaybuffersize', 'maxrelaymembercount', 'maxjoinchannelcount',
            'attributejson', 'siptranscoderid1', 'siptranscoderid2',
            'licensecountforsmaphotrial', 'smaphotrialfrom', 'smaphotrialto',
            'maxavailablegroupcd', 'licensecountformaintenance'
        ],
        'primary_key': 'Cd',
        'order_by': 'Cd',
        'batch_size': 10000,
        'description': 'VoipDB顧客情報'
    },
    
    # 新規テーブル3: UserAgent (VoipDB)
    'voipdb_useragent': {
        'db_type': 'voipdb',
        'sql_server_db': 'VoipDB',
        'sql_table': '[VoipDB].[dbo].[UserAgent]',
        'pg_table': 'voipdb_useragent',
        'columns': [
            'CustomerCd', 'Cd', 'IPAddress', 'VoiceListenPort', 'SessionListenPort',
            'UAType', 'LicenseCert', 'DisplayName', 'DefaultGroupCd', 'Network',
            'OpCenters', 'OpCenters_CD', 'MainChannelGroupCd', 'CurrentChannelNo',
            'CurrentChannelNos', 'IsStateListener', 'ProductType', 'ProductVersion',
            'Priority', 'AudioFmtVersion', 'SessionFmtVersion', 'UID', 'AttributeJson',
            'GgwTerminalID', 'Id', 'IsSmaphoTrialLicense', 'ApplyedSettingID',
            'ApplyedSettingRevNo'
        ],
        'pg_columns': [
            'customercd', 'cd', 'ipaddress', 'voicelistenport', 'sessionlistenport',
            'uatype', 'licensecert', 'displayname', 'defaultgroupcd', 'network',
            'opcenters', 'opcenters_cd', 'mainchannelgroupcd', 'currentchannelno',
            'currentchannelnos', 'isstatelistener', 'producttype', 'productversion',
            'priority', 'audiofmtversion', 'sessionfmtversion', 'uid', 'attributejson',
            'ggwterminalid', 'id', 'issmaphotriallicense', 'applyedsettingid',
            'applyedsettingrevno'
        ],
        'primary_key': 'Id',
        'order_by': 'Id',
        'batch_size': 10000,
        'description': 'VoipDBユーザーエージェント'
    }
}

# テーブル同期順序（依存関係なしのため任意順序）
DEFAULT_SYNC_ORDER = ['customer', 'mctm_module', 'voipdb_customer', 'voipdb_useragent']

# 注意: ハードコーディングされたデフォルト値を削除しました
# すべての接続情報は環境変数から取得します
# config.py モジュールを使用して設定を取得してください

def get_table_config(table_name):
    """指定されたテーブルの設定を取得"""
    if table_name not in TABLE_CONFIGS:
        raise ValueError(f"未知のテーブル名: {table_name}")
    return TABLE_CONFIGS[table_name]

def get_available_tables():
    """利用可能なテーブル一覧を取得"""
    return list(TABLE_CONFIGS.keys())

def validate_table_config(table_name):
    """テーブル設定の妥当性チェック"""
    config = get_table_config(table_name)
    
    required_fields = [
        'db_type', 'sql_server_db', 'sql_table', 'pg_table',
        'columns', 'pg_columns', 'primary_key', 'order_by', 'batch_size'
    ]
    
    for field in required_fields:
        if field not in config:
            raise ValueError(f"テーブル '{table_name}' の設定に必須フィールド '{field}' がありません")
    
    if len(config['columns']) != len(config['pg_columns']):
        raise ValueError(f"テーブル '{table_name}' のカラム数が一致しません")
    
    return True

def get_sql_query(table_name):
    """指定されたテーブル用のSQLクエリを生成"""
    config = get_table_config(table_name)
    
    # カラム名を配列から文字列に変換
    columns_str = ", ".join([f"[{col}]" for col in config['columns']])
    
    query = f"""
    SELECT {columns_str}
    FROM {config['sql_table']} WITH (NOLOCK)
    ORDER BY [{config['order_by']}]
    """
    
    return query.strip()

def get_pg_insert_query(table_name):
    """指定されたテーブル用のPostgreSQL INSERTクエリを生成"""
    config = get_table_config(table_name)
    
    # PostgreSQLカラム名（引用符付き）
    pg_columns_quoted = [f'"{col}"' for col in config['pg_columns']]
    columns_str = ", ".join(pg_columns_quoted)
    
    return f"INSERT INTO {config['pg_table']} ({columns_str}) VALUES %s"

# 設定の妥当性チェック実行
if __name__ == '__main__':
    print("=== テーブル設定検証 ===")
    for table_name in get_available_tables():
        try:
            validate_table_config(table_name)
            print(f"OK {table_name}: 設定OK")
            
            # サンプルクエリ表示
            sql_query = get_sql_query(table_name)
            pg_query = get_pg_insert_query(table_name)
            
            print(f"   SQL: {sql_query[:100]}...")
            print(f"   PG:  {pg_query[:100]}...")
            
        except Exception as e:
            print(f"NG {table_name}: {str(e)}")
    
    print(f"\n利用可能テーブル: {', '.join(get_available_tables())}")
    print(f"デフォルト同期順序: {' → '.join(DEFAULT_SYNC_ORDER)}")
