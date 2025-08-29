"""
個別テーブル同期処理クラス
SQL Server → PostgreSQL の単一テーブル同期を担当
"""

import os
import pymssql
import psycopg2
from psycopg2 import extras
import json
import logging
from datetime import datetime
from table_configs import (
    get_table_config, 
    get_sql_query, 
    get_pg_insert_query
)
from config import DatabaseConfig, ConfigurationError

logger = logging.getLogger(__name__)

class TableSyncProcessor:
    """単一テーブルの同期処理を行うクラス"""
    
    def __init__(self, table_name):
        """
        初期化
        
        Args:
            table_name (str): 同期対象テーブル名
        """
        self.table_name = table_name
        self.config = get_table_config(table_name)
        self.sql_conn = None
        self.pg_conn = None
        self.sql_cursor = None
        self.pg_cursor = None
        
    def get_sql_server_config(self):
        """SQL Server接続設定を取得"""
        try:
            # 環境変数から設定を取得
            db_type = self.config['db_type']
            sql_config = DatabaseConfig.get_sql_server_config(db_type)
            
            # データベース名をテーブル設定から上書き（必要に応じて）
            sql_config['database'] = self.config['sql_server_db']
            
            return sql_config
            
        except ConfigurationError as e:
            raise RuntimeError(f"SQL Server設定エラー ({self.table_name}): {str(e)}")
    
    def get_postgresql_config(self):
        """PostgreSQL接続設定を取得"""
        try:
            # 環境変数から設定を取得
            pg_config = DatabaseConfig.get_postgresql_config()
            return pg_config
            
        except ConfigurationError as e:
            raise RuntimeError(f"PostgreSQL設定エラー ({self.table_name}): {str(e)}")
    
    def connect_sql_server(self):
        """SQL Serverに接続"""
        sql_config = self.get_sql_server_config()
        
        logger.info(f"SQL Serverへの接続開始: {sql_config['host']}:{sql_config['port']}/{sql_config['database']}")
        
        try:
            self.sql_conn = pymssql.connect(
                server=sql_config['host'],
                user=sql_config['user'],
                password=sql_config['password'],
                database=sql_config['database'],
                port=sql_config['port'],
                timeout=sql_config['timeout'],
                login_timeout=sql_config['login_timeout'],
                charset=sql_config['charset']
            )
            self.sql_cursor = self.sql_conn.cursor(as_dict=False)
            logger.info("SQL Server接続成功")
            return True
            
        except pymssql.Error as e:
            logger.error(f"SQL Server接続失敗: {str(e)}")
            raise
    
    def connect_postgresql(self):
        """PostgreSQLに接続"""
        pg_config = self.get_postgresql_config()
        
        logger.info(f"PostgreSQLへの接続開始: {pg_config['host']}:{pg_config['port']}/{pg_config['database']}")
        
        try:
            self.pg_conn = psycopg2.connect(
                host=pg_config['host'],
                dbname=pg_config['database'],
                user=pg_config['user'],
                password=pg_config['password'],
                port=pg_config['port'],
                connect_timeout=pg_config['connect_timeout']
            )
            self.pg_cursor = self.pg_conn.cursor()
            logger.info("PostgreSQL接続成功")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL接続失敗: {str(e)}")
            raise
    
    def extract_data_from_sql_server(self):
        """SQL Serverからデータを抽出"""
        if not self.sql_conn or not self.sql_cursor:
            raise RuntimeError("SQL Server接続が確立されていません")
        
        query = get_sql_query(self.table_name)
        logger.info(f"SQL Serverからデータ抽出開始: {self.config['sql_table']}")
        logger.info(f"実行クエリ: {query[:100]}...")
        
        try:
            # SELECT実行時間計測開始
            select_start_time = datetime.now()
            
            self.sql_cursor.execute(query)
            rows = self.sql_cursor.fetchall()
            
            # SELECT実行時間計測終了
            select_end_time = datetime.now()
            select_duration = (select_end_time - select_start_time).total_seconds()
            
            logger.info(f"データ抽出完了: {len(rows):,}件 (SELECT実行時間: {select_duration:.2f}秒)")
            
            if len(rows) > 0:
                logger.info(f"取得レート: {len(rows)/select_duration:.0f}件/秒")
            
            # タプル形式に変換
            conversion_start_time = datetime.now()
            data_to_insert = [tuple(row) for row in rows]
            conversion_duration = (datetime.now() - conversion_start_time).total_seconds()
            
            if conversion_duration > 0.1:  # 0.1秒以上かかった場合のみログ出力
                logger.info(f"データ変換完了: {conversion_duration:.2f}秒")
            
            # 抽出件数を保存（検証用）
            self.extracted_count = len(rows)
            
            return data_to_insert
            
        except pymssql.Error as e:
            logger.error(f"データ抽出失敗: {str(e)}")
            raise
    
    def clear_postgresql_table(self):
        """PostgreSQLテーブルをクリア"""
        if not self.pg_conn or not self.pg_cursor:
            raise RuntimeError("PostgreSQL接続が確立されていません")
        
        table_name = self.config['pg_table']
        logger.info(f"PostgreSQLテーブルクリア開始: {table_name}")
        
        try:
            truncate_start_time = datetime.now()
            self.pg_cursor.execute(f"TRUNCATE TABLE {table_name}")
            truncate_duration = (datetime.now() - truncate_start_time).total_seconds()
            
            logger.info(f"テーブルクリア完了: {table_name} ({truncate_duration:.2f}秒)")
            
        except psycopg2.Error as e:
            logger.error(f"テーブルクリア失敗: {table_name} - {str(e)}")
            raise
    
    def load_data_to_postgresql(self, data_to_insert):
        """PostgreSQLにデータをロード"""
        if not self.pg_conn or not self.pg_cursor:
            raise RuntimeError("PostgreSQL接続が確立されていません")
        
        if not data_to_insert:
            logger.warning("挿入するデータがありません")
            return 0
        
        insert_query = get_pg_insert_query(self.table_name)
        batch_size = self.config['batch_size']
        total_records = len(data_to_insert)
        
        logger.info(f"PostgreSQLデータロード開始: {total_records:,}件 (バッチサイズ: {batch_size}件)")
        logger.info(f"対象テーブル: {self.config['pg_table']}")
        
        try:
            # バッチサイズで分割してインサート
            batch_count = (total_records + batch_size - 1) // batch_size
            insert_start_time = datetime.now()
            processed_records = 0
            
            logger.info(f"バッチ処理開始: 全{batch_count}バッチ")
            
            for batch_num in range(batch_count):
                batch_start_time = datetime.now()
                
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, total_records)
                batch_data = data_to_insert[start_idx:end_idx]
                
                extras.execute_values(
                    self.pg_cursor,
                    insert_query,
                    batch_data,
                    page_size=batch_size
                )
                
                batch_duration = (datetime.now() - batch_start_time).total_seconds()
                processed_records += len(batch_data)
                progress_percent = (processed_records / total_records) * 100
                
                # 進捗状況の詳細ログ
                logger.info(f"  バッチ {batch_num + 1:3d}/{batch_count}: {len(batch_data):,}件挿入 "
                          f"({batch_duration:.2f}秒) - 進捗: {progress_percent:.1f}% "
                          f"({processed_records:,}/{total_records:,}件)")
                
            
            # 最終統計
            total_insert_duration = (datetime.now() - insert_start_time).total_seconds()
            final_rate = total_records / total_insert_duration if total_insert_duration > 0 else 0
            
            logger.info(f"データロード完了: {total_records:,}件 "
                      f"(総時間: {total_insert_duration:.2f}秒, 平均レート: {final_rate:.0f}件/秒)")
            
            return total_records
            
        except psycopg2.Error as e:
            logger.error(f"データロード失敗 (バッチ {batch_num + 1}/{batch_count}): {str(e)}")
            raise
    
    def validate_transfer(self):
        """転送結果の検証（SQL Serverへの追加リクエストなし）"""
        if not self.pg_conn or not hasattr(self, 'extracted_count'):
            raise RuntimeError("データベース接続または抽出数が不明")
        
        try:
            # PostgreSQLのレコード数のみチェック
            pg_count_query = f"SELECT COUNT(*) FROM {self.config['pg_table']}"
            self.pg_cursor.execute(pg_count_query)
            pg_count = self.pg_cursor.fetchone()[0]
            
            # 事前に取得したSQL Serverの件数と比較
            logger.info(f"転送検証: SQL Server={self.extracted_count}件, PostgreSQL={pg_count}件")
            
            if self.extracted_count == pg_count:
                logger.info("転送検証成功: レコード数が一致")
                return True
            else:
                logger.error(f"転送検証失敗: レコード数不一致 (差分: {self.extracted_count - pg_count})")
                return False
                
        except Exception as e:
            logger.error(f"転送検証エラー: {str(e)}")
            return False
    
    def sync_table(self):
        """テーブル同期の実行"""
        start_time = datetime.now()
        result = {
            'table_name': self.table_name,
            'success': False,
            'transferred_count': 0,
            'execution_time': 0,
            'error': None,
            'validation_passed': False
        }
        
        logger.info(f"=== テーブル同期開始: {self.table_name} ({self.config['description']}) ===")
        
        try:
            # 1. データベース接続
            self.connect_sql_server()
            self.connect_postgresql()
            
            # 2. データ抽出
            data_to_insert = self.extract_data_from_sql_server()
            
            if not data_to_insert:
                logger.warning("転送対象データが0件です")
                result.update({
                    'success': True,
                    'transferred_count': 0,
                    'validation_passed': True
                })
                return result
            
            # 3. テーブルクリア
            self.clear_postgresql_table()
            
            # 4. データロード
            transferred_count = self.load_data_to_postgresql(data_to_insert)
            
            # 5. コミット
            logger.info("トランザクションコミット開始...")
            commit_start_time = datetime.now()
            self.pg_conn.commit()
            commit_duration = (datetime.now() - commit_start_time).total_seconds()
            logger.info(f"トランザクションコミット完了 ({commit_duration:.2f}秒)")
            
            # 6. 検証
            validation_passed = self.validate_transfer()
            
            # 7. 結果更新
            result.update({
                'success': True,
                'transferred_count': transferred_count,
                'validation_passed': validation_passed
            })
            
        except Exception as e:
            logger.error(f"テーブル同期エラー: {str(e)}")
            
            # ロールバック
            if self.pg_conn:
                try:
                    self.pg_conn.rollback()
                    logger.info("PostgreSQLトランザクションロールバック")
                except:
                    pass
            
            result.update({
                'success': False,
                'error': str(e)
            })
            
        finally:
            # 接続クローズ
            self.close_connections()
            
            # 実行時間計算
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            result['execution_time'] = execution_time
            
            logger.info(f"=== テーブル同期終了: {self.table_name} (実行時間: {execution_time:.2f}秒) ===")
            
            if result['success']:
                logger.info(f"同期成功: {result['transferred_count']}件転送")
            else:
                logger.error(f"同期失敗: {result.get('error', '不明なエラー')}")
        
        return result
    
    def test_connections(self):
        """接続テストのみ実行"""
        logger.info(f"=== 接続テスト開始: {self.table_name} ===")
        
        result = {
            'table_name': self.table_name,
            'sql_server_success': False,
            'postgresql_success': False,
            'overall_success': False
        }
        
        try:
            # SQL Server接続テスト
            try:
                self.connect_sql_server()
                
                # テーブル存在確認
                table_check_query = f"SELECT COUNT(*) FROM {self.config['sql_table']} WITH (NOLOCK)"
                self.sql_cursor.execute(table_check_query)
                count = self.sql_cursor.fetchone()[0]
                
                logger.info(f"SQL Server テーブル確認: {count}件")
                result['sql_server_success'] = True
                
            except Exception as e:
                logger.error(f"SQL Server接続テスト失敗: {str(e)}")
            
            # PostgreSQL接続テスト
            try:
                self.connect_postgresql()
                
                # テーブル存在確認
                table_check_query = f"SELECT COUNT(*) FROM {self.config['pg_table']}"
                self.pg_cursor.execute(table_check_query)
                count = self.pg_cursor.fetchone()[0]
                
                logger.info(f"PostgreSQL テーブル確認: {count}件")
                result['postgresql_success'] = True
                
            except Exception as e:
                logger.error(f"PostgreSQL接続テスト失敗: {str(e)}")
            
            result['overall_success'] = result['sql_server_success'] and result['postgresql_success']
            
        finally:
            self.close_connections()
            
        logger.info(f"=== 接続テスト終了: {self.table_name} ===")
        return result
    
    def close_connections(self):
        """データベース接続をクローズ"""
        try:
            if self.sql_cursor:
                self.sql_cursor.close()
            if self.sql_conn:
                self.sql_conn.close()
                logger.info("SQL Server接続クローズ")
        except:
            pass
        
        try:
            if self.pg_cursor:
                self.pg_cursor.close()
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("PostgreSQL接続クローズ")
        except:
            pass

# テスト実行
if __name__ == '__main__':
    import sys
    
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("使用方法: python table_sync_processor.py <table_name> [test|sync]")
        print("利用可能テーブル: customer, mctm_module, voipdb_customer, voipdb_useragent")
        sys.exit(1)
    
    table_name = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else 'sync'
    
    processor = TableSyncProcessor(table_name)
    
    if mode == 'test':
        result = processor.test_connections()
        print(f"接続テスト結果: {json.dumps(result, ensure_ascii=False, indent=2)}")
    else:
        result = processor.sync_table()
        print(f"同期結果: {json.dumps(result, ensure_ascii=False, indent=2, default=str)}")
