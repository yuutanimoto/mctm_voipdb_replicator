"""
AWS Lambda関数 - マルチテーブル対応版
SQL Server → PostgreSQL データ同期処理（複数テーブル対応）
"""

import os
import json
import logging
from datetime import datetime
from multi_table_manager import MultiTableSyncManager
from table_configs import get_available_tables, DEFAULT_SYNC_ORDER

# AWS Lambda用ログ設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def execute_multi_table_sync(target_tables=None):
    """複数テーブルの同期処理実行（順次実行）"""
    logger.info("=== マルチテーブル同期処理開始 ===")
    
    try:
        # 対象テーブルの決定
        if target_tables is None:
            target_tables = DEFAULT_SYNC_ORDER
        
        logger.info(f"対象テーブル: {target_tables}")
        logger.info(f"実行モード: 順次実行")
        
        # マネージャー作成
        manager = MultiTableSyncManager(
            target_tables=target_tables
        )
        
        # 同期実行
        result = manager.sync_all_tables()
        result['mode'] = 'multi_sync'
        
        return result
        
    except Exception as e:
        logger.error(f"マルチテーブル同期エラー: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'mode': 'multi_sync',
            'total_transferred': 0
        }

def execute_multi_table_test(target_tables=None):
    """複数テーブルの接続テスト実行（順次実行）"""
    logger.info("=== マルチテーブル接続テスト開始 ===")
    
    try:
        # 対象テーブルの決定
        if target_tables is None:
            target_tables = DEFAULT_SYNC_ORDER
        
        logger.info(f"対象テーブル: {target_tables}")
        logger.info(f"実行モード: 順次実行")
        
        # マネージャー作成
        manager = MultiTableSyncManager(
            target_tables=target_tables
        )
        
        # テスト実行
        result = manager.test_all_connections()
        result['mode'] = 'multi_test'
        
        return result
        
    except Exception as e:
        logger.error(f"マルチテーブル接続テストエラー: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'mode': 'multi_test'
        }

def execute_single_table_sync(table_name):
    """単一テーブルの同期処理実行"""
    logger.info(f"=== 単一テーブル同期処理開始: {table_name} ===")
    
    try:
        from table_sync_processor import TableSyncProcessor
        
        processor = TableSyncProcessor(table_name)
        result = processor.sync_table()
        
        return {
            'success': result['success'],
            'table_name': table_name,
            'transferred_count': result['transferred_count'],
            'execution_time': result['execution_time'],
            'validation_passed': result['validation_passed'],
            'error': result.get('error'),
            'mode': 'single_sync'
        }
        
    except Exception as e:
        logger.error(f"単一テーブル同期エラー ({table_name}): {str(e)}")
        return {
            'success': False,
            'table_name': table_name,
            'transferred_count': 0,
            'error': str(e),
            'mode': 'single_sync'
        }

def get_table_info():
    """利用可能なテーブル情報を取得"""
    try:
        available_tables = get_available_tables()
        
        from table_configs import get_table_config
        table_info = []
        
        for table_name in available_tables:
            config = get_table_config(table_name)
            table_info.append({
                'table_name': table_name,
                'description': config['description'],
                'sql_table': config['sql_table'],
                'pg_table': config['pg_table'],
                'column_count': len(config['columns']),
                'db_type': config['db_type'],
                'sql_server_db': config['sql_server_db']
            })
        
        return {
            'success': True,
            'available_tables': available_tables,
            'default_sync_order': DEFAULT_SYNC_ORDER,
            'table_details': table_info,
            'mode': 'info'
        }
        
    except Exception as e:
        logger.error(f"テーブル情報取得エラー: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'mode': 'info'
        }

def lambda_handler(event, context):
    """Lambda関数のエントリーポイント（マルチテーブル対応版）"""
    logger.info("=== Lambda関数実行開始 (マルチテーブル対応) ===")
    logger.info(f"Event: {json.dumps(event, ensure_ascii=False)}")
    
    start_time = datetime.now()
    
    try:
        # モード判定
        mode = event.get('mode', 'multi_sync')  # デフォルトはマルチテーブル同期
        logger.info(f"実行モード: {mode}")
        
        # 実行パラメータ取得
        target_tables = event.get('tables')  # 対象テーブル指定

        table_name = event.get('table_name')  # 単一テーブル名
        
        # モード別処理
        if mode == 'multi_sync':
            # 複数テーブル同期
            result = execute_multi_table_sync(target_tables)
            
        elif mode == 'multi_test':
            # 複数テーブル接続テスト
            result = execute_multi_table_test(target_tables)
            
        elif mode == 'single_sync':
            # 単一テーブル同期
            if not table_name:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'success': False,
                        'error': 'single_syncモードではtable_nameパラメータが必要です',
                        'available_tables': get_available_tables(),
                        'timestamp': datetime.now().isoformat()
                    }, ensure_ascii=False)
                }
            result = execute_single_table_sync(table_name)
            
        elif mode == 'info':
            # テーブル情報取得
            result = get_table_info()
            
        else:
            logger.error(f"不正なモード: {mode}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'error': f'不正なモード: {mode}',
                    'valid_modes': [
                        'multi_sync', 'multi_test', 'single_sync', 'info'
                    ],
                    'timestamp': datetime.now().isoformat()
                }, ensure_ascii=False)
            }
        
        # 実行時間計算
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # レスポンス作成
        result['execution_time'] = execution_time
        result['timestamp'] = end_time.isoformat()
        result['library'] = 'pymssql + psycopg2'
        result['version'] = 'multi-table-v1.0'
        
        status_code = 200 if result.get('success', False) else 500
        
        response = {
            'statusCode': status_code,
            'body': json.dumps(result, ensure_ascii=False, default=str)
        }
        
        logger.info(f"=== Lambda関数実行完了 (実行時間: {execution_time:.2f}秒) ===")
        
        # 結果サマリーログ
        if mode in ['multi_sync', 'single_sync', 'sync']:
            transferred = result.get('total_transferred', result.get('transferred_count', 0))
            logger.info(f"結果: データ転送 {'OK' if result.get('success') else 'NG'} ({transferred}件)")
        elif mode in ['multi_test', 'test']:
            logger.info(f"結果: 接続テスト {'OK' if result.get('success') else 'NG'}")
        
        return response
        
    except Exception as e:
        logger.error(f"Lambda関数実行エラー: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'library': 'pymssql + psycopg2',
                'version': 'multi-table-v1.0'
            }, ensure_ascii=False)
        }

if __name__ == '__main__':
    import sys
    
    # ローカル実行時の詳細ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== ローカル実行モード (マルチテーブル対応) ===")
    print("環境変数設定例:")
    print("# SQL Server (McTM)")
    print("export SQL_SERVER_MCTM_HOST='10.35.11.13'")
    print("export SQL_SERVER_MCTM_DB='McTM'")
    print("export SQL_SERVER_MCTM_USER='sa'")
    print("export SQL_SERVER_MCTM_PASSWORD='your_password'")
    print("export SQL_SERVER_MCTM_PORT='1433'")
    print("# SQL Server (VoipDB)")
    print("export SQL_SERVER_VOIPDB_HOST='10.35.11.14'")
    print("export SQL_SERVER_VOIPDB_DB='VoipDB'")
    print("export SQL_SERVER_VOIPDB_USER='sa'")
    print("export SQL_SERVER_VOIPDB_PASSWORD='your_password'")
    print("export SQL_SERVER_VOIPDB_PORT='1433'")
    print("# PostgreSQL")
    print("export PG_HOST='your-rds-endpoint.amazonaws.com'")
    print("export PG_DB='dx_sim_management_db'")
    print("export PG_USER='postgres'")
    print("export PG_PASSWORD='your_password'")
    print("export PG_PORT='5432'")
    print("# 詳細は .env.example を参照してください")
    print("=" * 70)
    
    # コマンドライン引数解析
    mode = 'multi_sync'  # デフォルト
    target_tables = None
    table_name = None
    
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg in ['multi_sync', 'multi_test', 'single_sync', 'info']:
            mode = arg
        elif arg.startswith('--tables='):
            target_tables = arg.split('=')[1].split(',')
        elif arg.startswith('--table='):
            table_name = arg.split('=')[1]
        elif '=' not in arg and i == 0:  # 最初の引数はモード
            mode = arg
    
    print(f"実行モード: {mode}")
    print("使用方法:")
    print("  python lambda_function.py [mode] [--tables=table1,table2] [--table=table_name]")
    print("  モード:")
    print("    multi_sync  : 複数テーブル同期 (デフォルト)")
    print("    multi_test  : 複数テーブル接続テスト")
    print("    single_sync : 単一テーブル同期 (--table必須)")
    print("    info        : テーブル情報表示")
    print("  オプション:")
    print("    --tables=t1,t2    : 対象テーブル指定")
    print("    --table=table_name: 単一テーブル名")
    print(f"  利用可能テーブル: {', '.join(get_available_tables())}")
    print("=" * 70)
    
    start_time = datetime.now()
    
    try:
        # イベントオブジェクト作成
        event = {
            'mode': mode
        }
        
        if target_tables:
            event['tables'] = target_tables
        if table_name:
            event['table_name'] = table_name
        
        # Lambda関数実行
        response = lambda_handler(event, None)
        
        # 結果表示
        result = json.loads(response['body'])
        
        print(f"\n=== 実行結果 (ステータス: {response['statusCode']}) ===")
        
        if result.get('success'):
            print("処理成功")
            
            if mode in ['multi_sync', 'single_sync']:
                transferred = result.get('total_transferred', result.get('transferred_count', 0))
                print(f"転送件数: {transferred:,}件")
                
                if 'table_results' in result:
                    print("テーブル別結果:")
                    for table, table_result in result['table_results'].items():
                        count = table_result.get('transferred_count', 0)
                        time = table_result.get('execution_time', 0)
                        status = "OK" if table_result.get('success') else "NG"
                        print(f"  {status} {table}: {count:,}件 ({time:.2f}秒)")
                        
            elif mode == 'multi_test':
                print("接続テスト結果:")
                if 'table_results' in result:
                    for table, table_result in result['table_results'].items():
                        status = "OK" if table_result.get('overall_success') else "NG"
                        print(f"  {status} {table}")
                    
            elif mode == 'info':
                print("利用可能テーブル:")
                for table_detail in result.get('table_details', []):
                    print(f"  • {table_detail['table_name']}: {table_detail['description']}")
                    print(f"    SQL: {table_detail['sql_table']}")
                    print(f"    PG:  {table_detail['pg_table']}")
                    print(f"    カラム数: {table_detail['column_count']}")
        else:
            print("処理失敗")
            print(f"エラー: {result.get('error', '不明なエラー')}")
        
        # 実行時間表示
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        print(f"\n実行時間: {execution_time:.2f}秒")
        
        # 終了コード設定
        if result.get('success'):
            print("処理完了")
            sys.exit(0)
        else:
            print("処理失敗")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nユーザーによる中断")
        sys.exit(130)
    except Exception as e:
        print(f"\n予期しないエラー: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
