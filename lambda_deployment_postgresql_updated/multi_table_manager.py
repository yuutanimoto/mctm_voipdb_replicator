"""
複数テーブル同期管理クラス
複数のテーブルを統合的に管理し、一括同期処理を行う
"""

import json
import logging
from datetime import datetime
from table_configs import get_available_tables, DEFAULT_SYNC_ORDER
from table_sync_processor import TableSyncProcessor

logger = logging.getLogger(__name__)

class MultiTableSyncManager:
    """複数テーブルの同期処理を管理するクラス"""
    
    def __init__(self, target_tables=None):
        """
        初期化
        
        Args:
            target_tables (list): 同期対象テーブルリスト（Noneの場合は全テーブル）
        """
        self.target_tables = target_tables if target_tables else DEFAULT_SYNC_ORDER
        
        # テーブル名の妥当性チェック
        available_tables = get_available_tables()
        for table in self.target_tables:
            if table not in available_tables:
                raise ValueError(f"未知のテーブル名: {table}. 利用可能: {available_tables}")
        
        logger.info(f"MultiTableSyncManager初期化完了")
        logger.info(f"対象テーブル: {self.target_tables}")
        logger.info(f"実行モード: 順次実行")
    
    def sync_single_table(self, table_name):
        """単一テーブルの同期実行"""
        logger.info(f"単一テーブル同期開始: {table_name}")
        
        try:
            processor = TableSyncProcessor(table_name)
            result = processor.sync_table()
            return result
            
        except Exception as e:
            logger.error(f"単一テーブル同期エラー ({table_name}): {str(e)}")
            return {
                'table_name': table_name,
                'success': False,
                'transferred_count': 0,
                'execution_time': 0,
                'error': str(e),
                'validation_passed': False
            }
    
    def test_single_table_connections(self, table_name):
        """単一テーブルの接続テスト"""
        logger.info(f"単一テーブル接続テスト開始: {table_name}")
        
        try:
            processor = TableSyncProcessor(table_name)
            result = processor.test_connections()
            return result
            
        except Exception as e:
            logger.error(f"単一テーブル接続テストエラー ({table_name}): {str(e)}")
            return {
                'table_name': table_name,
                'sql_server_success': False,
                'postgresql_success': False,
                'overall_success': False,
                'error': str(e)
            }
    
    def sync_all_tables_sequential(self):
        """全テーブルの順次同期実行"""
        logger.info("=== 順次同期処理開始 ===")
        start_time = datetime.now()
        
        results = {}
        overall_success = True
        total_transferred = 0
        
        for table_index, table_name in enumerate(self.target_tables, 1):
            logger.info(f"テーブル同期開始 [{table_index}/{len(self.target_tables)}]: {table_name}")
            table_start_time = datetime.now()
            
            try:
                result = self.sync_single_table(table_name)
                results[table_name] = result
                
                table_duration = (datetime.now() - table_start_time).total_seconds()
                
                if result['success']:
                    total_transferred += result['transferred_count']
                    logger.info(f"{table_name}: {result['transferred_count']:,}件転送完了 "
                              f"(テーブル処理時間: {table_duration:.2f}秒)")
                    
                    # 進捗状況表示
                    progress_percent = (table_index / len(self.target_tables)) * 100
                    logger.info(f"全体進捗: {progress_percent:.1f}% ({table_index}/{len(self.target_tables)}テーブル完了)")
                    
                else:
                    overall_success = False
                    logger.error(f"{table_name}: {result.get('error', '不明なエラー')} "
                               f"(処理時間: {table_duration:.2f}秒)")
                    
            except Exception as e:
                table_duration = (datetime.now() - table_start_time).total_seconds()
                error_result = {
                    'table_name': table_name,
                    'success': False,
                    'transferred_count': 0,
                    'execution_time': table_duration,
                    'error': str(e),
                    'validation_passed': False
                }
                results[table_name] = error_result
                overall_success = False
                logger.error(f"{table_name}: 予期しないエラー - {str(e)} "
                            f"(処理時間: {table_duration:.2f}秒)")
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info("=== 順次同期処理終了 ===")
        logger.info(f"総実行時間: {execution_time:.2f}秒")
        logger.info(f"総転送件数: {total_transferred:,}件")
        
        if execution_time > 0:
            avg_rate = total_transferred / execution_time
            logger.info(f"平均転送レート: {avg_rate:.0f}件/秒")
        
        # テーブル別統計サマリー
        success_count = sum(1 for r in results.values() if r['success'])
        logger.info(f"処理結果: 成功 {success_count}/{len(results)}テーブル")
        
        if success_count > 0:
            logger.info("テーブル別サマリー:")
            for table_name, result in results.items():
                if result['success']:
                    logger.info(f"  {table_name}: {result['transferred_count']:,}件 "
                              f"({result['execution_time']:.1f}秒)")
                else:
                    logger.info(f"  {table_name}: 失敗 - {result.get('error', '不明')}")
        
        return {
            'success': overall_success,
            'execution_mode': 'sequential',
            'table_results': results,
            'total_transferred': total_transferred,
            'execution_time': execution_time,
            'processed_tables': len(results),
            'successful_tables': sum(1 for r in results.values() if r['success']),
            'failed_tables': sum(1 for r in results.values() if not r['success'])
        }
    

    
    def sync_all_tables(self):
        """全テーブルの同期実行（順次実行）"""
        return self.sync_all_tables_sequential()
    
    def test_all_connections_sequential(self):
        """全テーブルの接続テスト（順次実行）"""
        logger.info("=== 順次接続テスト開始 ===")
        start_time = datetime.now()
        
        results = {}
        
        for table_name in self.target_tables:
            logger.info(f"接続テスト実行: {table_name}")
            
            try:
                result = self.test_single_table_connections(table_name)
                results[table_name] = result
                
                if result['overall_success']:
                    logger.info(f"{table_name}: 接続OK")
                else:
                    logger.error(f"{table_name}: 接続NG")
                    
            except Exception as e:
                error_result = {
                    'table_name': table_name,
                    'sql_server_success': False,
                    'postgresql_success': False,
                    'overall_success': False,
                    'error': str(e)
                }
                results[table_name] = error_result
                logger.error(f"{table_name}: 接続テストエラー - {str(e)}")
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        overall_success = all(result['overall_success'] for result in results.values())
        
        logger.info("=== 順次接続テスト終了 ===")
        logger.info(f"実行時間: {execution_time:.2f}秒")
        
        return {
            'success': overall_success,
            'execution_mode': 'sequential_test',
            'table_results': results,
            'execution_time': execution_time,
            'tested_tables': len(results),
            'successful_connections': sum(1 for r in results.values() if r['overall_success']),
            'failed_connections': sum(1 for r in results.values() if not r['overall_success'])
        }
    

    
    def test_all_connections(self):
        """全テーブルの接続テスト（順次実行）"""
        return self.test_all_connections_sequential()
    
    def get_table_summary(self):
        """対象テーブルの概要情報を取得"""
        from table_configs import get_table_config
        
        summary = []
        for table_name in self.target_tables:
            config = get_table_config(table_name)
            summary.append({
                'table_name': table_name,
                'description': config['description'],
                'sql_table': config['sql_table'],
                'pg_table': config['pg_table'],
                'column_count': len(config['columns']),
                'batch_size': config['batch_size'],
                'db_type': config['db_type'],
                'sql_server_db': config['sql_server_db']
            })
        
        return summary
    
    def print_execution_summary(self, result):
        """実行結果のサマリーを表示"""
        logger.info("=" * 60)
        logger.info("実行結果サマリー")
        logger.info("=" * 60)
        
        logger.info(f"実行モード: {result['execution_mode']}")
        logger.info(f"総合結果: {'成功' if result['success'] else '失敗'}")
        logger.info(f"実行時間: {result['execution_time']:.2f}秒")
        
        if 'total_transferred' in result:
            logger.info(f"総転送件数: {result['total_transferred']:,}件")
            logger.info(f"処理テーブル数: {result['processed_tables']}")
            logger.info(f"成功テーブル数: {result['successful_tables']}")
            logger.info(f"失敗テーブル数: {result['failed_tables']}")
        else:
            logger.info(f"テストテーブル数: {result['tested_tables']}")
            logger.info(f"接続成功数: {result['successful_connections']}")
            logger.info(f"接続失敗数: {result['failed_connections']}")
        

        
        logger.info("-" * 60)
        logger.info("テーブル別結果:")
        
        for table_name, table_result in result['table_results'].items():
            status = "OK" if table_result.get('success', table_result.get('overall_success', False)) else "NG"
            
            if 'transferred_count' in table_result:
                logger.info(f"  {status} {table_name}: {table_result['transferred_count']:,}件 "
                          f"({table_result['execution_time']:.2f}秒)")
            else:
                sql_status = "OK" if table_result.get('sql_server_success', False) else "NG"
                pg_status = "OK" if table_result.get('postgresql_success', False) else "NG"
                logger.info(f"  {status} {table_name}: SQL{sql_status} PG{pg_status}")
            
            if table_result.get('error'):
                logger.info(f"      エラー: {table_result['error']}")
        
        logger.info("=" * 60)

# テスト実行
if __name__ == '__main__':
    import sys
    
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 引数解析
    mode = sys.argv[1] if len(sys.argv) > 1 else 'sync'
    
    if mode not in ['test', 'sync']:
        print("使用方法: python multi_table_manager.py [test|sync]")
        sys.exit(1)
    
    # テーブル指定（引数で指定可能）
    target_tables = None
    for arg in sys.argv:
        if arg.startswith('--tables='):
            target_tables = arg.split('=')[1].split(',')
            break
    
    try:
        manager = MultiTableSyncManager(
            target_tables=target_tables
        )
        
        logger.info(f"対象テーブル: {manager.target_tables}")
        
        if mode == 'test':
            result = manager.test_all_connections()
            print(f"\n接続テスト結果:")
        else:
            result = manager.sync_all_tables()
            print(f"\n同期結果:")
        
        manager.print_execution_summary(result)
        print(f"\n詳細結果: {json.dumps(result, ensure_ascii=False, indent=2, default=str)}")
        
    except Exception as e:
        logger.error(f"実行エラー: {str(e)}")
        sys.exit(1)
