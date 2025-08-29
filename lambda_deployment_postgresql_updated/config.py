"""
環境変数管理モジュール
データベース接続情報を環境変数から取得し、バリデーションを行う
"""

import os
from typing import Dict, Any

# .envファイルの読み込み（存在する場合）
try:
    from dotenv import load_dotenv
    # プロジェクトルートの.envファイルを探して読み込み
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"OK .envファイルを読み込みました: {env_path}")
    else:
        print(f"INFO .envファイルが見つかりません: {env_path}")
except ImportError:
    print("INFO python-dotenvがインストールされていません。環境変数のみ使用します。")
    print("  ローカル開発では 'pip install python-dotenv' の実行を推奨します。")

class ConfigurationError(Exception):
    """設定エラー"""
    pass

class DatabaseConfig:
    """データベース設定管理クラス"""
    
    @staticmethod
    def get_required_env(key: str) -> str:
        """必須環境変数を取得（未設定時はエラー）"""
        value = os.getenv(key)
        if value is None:
            raise ConfigurationError(f"必須環境変数が設定されていません: {key}")
        return value
    
    @staticmethod
    def get_optional_env(key: str, default: Any) -> Any:
        """任意環境変数を取得（デフォルト値付き）"""
        value = os.getenv(key)
        if value is None:
            return default
        
        # 型変換
        if isinstance(default, int):
            try:
                return int(value)
            except ValueError:
                raise ConfigurationError(f"環境変数の型が不正です: {key}={value} (整数が必要)")
        elif isinstance(default, bool):
            return value.lower() in ('true', '1', 'yes', 'on')
        else:
            return value

    @staticmethod
    def get_sql_server_config(db_type: str) -> Dict[str, Any]:
        """
        SQL Server接続設定を取得
        
        Args:
            db_type (str): データベースタイプ ('mctm' または 'voipdb')
            
        Returns:
            Dict[str, Any]: 接続設定辞書
        """
        if db_type not in ['mctm', 'voipdb']:
            raise ConfigurationError(f"不正なデータベースタイプ: {db_type}")
        
        prefix = f"SQL_SERVER_{db_type.upper()}"
        
        try:
            config = {
                'host': DatabaseConfig.get_required_env(f"{prefix}_HOST"),
                'database': DatabaseConfig.get_required_env(f"{prefix}_DB"),
                'user': DatabaseConfig.get_required_env(f"{prefix}_USER"),
                'password': DatabaseConfig.get_required_env(f"{prefix}_PASSWORD"),
                'port': DatabaseConfig.get_optional_env(f"{prefix}_PORT", 1433),
                'timeout': DatabaseConfig.get_optional_env(f"{prefix}_TIMEOUT", 60),
                'login_timeout': DatabaseConfig.get_optional_env(f"{prefix}_LOGIN_TIMEOUT", 30),
                'charset': DatabaseConfig.get_optional_env(f"{prefix}_CHARSET", 'UTF-8')
            }
            return config
            
        except ConfigurationError as e:
            raise ConfigurationError(f"SQL Server ({db_type}) 設定エラー: {str(e)}")

    @staticmethod
    def get_postgresql_config() -> Dict[str, Any]:
        """
        PostgreSQL接続設定を取得
        
        Returns:
            Dict[str, Any]: 接続設定辞書
        """
        try:
            config = {
                'host': DatabaseConfig.get_required_env('PG_HOST'),
                'database': DatabaseConfig.get_required_env('PG_DB'),
                'user': DatabaseConfig.get_required_env('PG_USER'),
                'password': DatabaseConfig.get_required_env('PG_PASSWORD'),
                'port': DatabaseConfig.get_optional_env('PG_PORT', 5432),
                'connect_timeout': DatabaseConfig.get_optional_env('PG_CONNECT_TIMEOUT', 30)
            }
            return config
            
        except ConfigurationError as e:
            raise ConfigurationError(f"PostgreSQL 設定エラー: {str(e)}")

    @staticmethod
    def validate_all_configs() -> Dict[str, Dict[str, Any]]:
        """
        全データベース設定を検証
        
        Returns:
            Dict[str, Dict[str, Any]]: 全設定辞書
        """
        configs = {}
        
        try:
            configs['sql_server_mctm'] = DatabaseConfig.get_sql_server_config('mctm')
            configs['sql_server_voipdb'] = DatabaseConfig.get_sql_server_config('voipdb')
            configs['postgresql'] = DatabaseConfig.get_postgresql_config()
            
            return configs
            
        except ConfigurationError as e:
            raise ConfigurationError(f"設定検証失敗: {str(e)}")

def get_database_config_by_host(host: str) -> Dict[str, Any]:
    """
    ホスト名からデータベース設定を取得（後方互換性用）
    
    Args:
        host (str): ホスト名
        
    Returns:
        Dict[str, Any]: 接続設定辞書
    """
    # ホスト名からデータベースタイプを判定
    if host == '10.35.11.13' or host == 'imesh-fdc-mdb-mctm-pri.private':
        return DatabaseConfig.get_sql_server_config('mctm')
    elif host == '10.35.11.14' or host == 'imesh-fdc-mdb-voipdb-pri.private':
        return DatabaseConfig.get_sql_server_config('voipdb')
    else:
        raise ConfigurationError(f"未知のホスト名: {host}")

# 設定検証用
if __name__ == '__main__':
    print("=== 環境変数設定検証 ===")
    
    try:
        configs = DatabaseConfig.validate_all_configs()
        
        print("設定検証成功:")
        
        for db_name, config in configs.items():
            print(f"\n[{db_name}]")
            for key, value in config.items():
                if 'password' in key.lower():
                    print(f"  {key}: ***masked***")
                else:
                    print(f"  {key}: {value}")
                    
        print("\n全ての必須環境変数が正しく設定されています。")
        
    except ConfigurationError as e:
        print(f"設定検証失敗: {str(e)}")
        print("\n必要な環境変数を.envファイルまたは環境変数として設定してください。")
        print("詳細は .env.example を参照してください。")
        exit(1)