# McTM VoipDB Replicator

SQL Server → PostgreSQL データ同期システム

**メインプログラム**: `lambda_function.py`

## ローカル実行手順

### 1. プロジェクト配置

WSLのUbuntuでプロジェクトを開く：

```bash
# プロジェクトフォルダを以下の場所に配置　またはgit cloneしてくる
\\wsl.localhost\Ubuntu-24.04\home\init_user\mctm_voipdb_replicator
```

### 2. Cursorで開く

```bash
# Ubuntu 24.04.1 LTSターミナルで実行
cd mctm_voipdb_replicator
cursor .
```

### 3. Python環境の確認とセットアップ

#### インストール済みPythonバージョンの確認

```bash
# 利用可能なPythonバージョンを確認
ls /usr/bin/python*
# 例: /usr/bin/python3  /usr/bin/python3.12  /usr/bin/python3.13

# 各バージョンの詳細確認
python3 --version
python3.12 --version
python3.13 --version

# デフォルトのpython3が何を指しているか確認
which python3
```

#### Python 3.13のインストール（必要な場合）

Python 3.13がインストールされていない場合：

```bash
# deadsnakes PPAを追加（最新Pythonバージョン用）
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

# Python 3.13をインストール
sudo apt install python3.13 python3.13-venv python3.13-dev

# インストール確認
python3.13 --version
```

#### 仮想環境の作成

```bash
# プロジェクトルートで仮想環境を作成
python3.13 -m venv .venv

# 仮想環境を有効化してpipをアップデート
source .venv/bin/activate
python -m pip install --upgrade pip

# 作成確認
which python
# → /home/init_user/mctm_voipdb_replicator/.venv/bin/python

# 一旦無効化
deactivate
```

### 4. 仮想環境の有効化

```bash
# ターミナル → 新しいターミナル
source .venv/bin/activate
# 先頭に(.venv)が表示されてればOK
```

### 5. プログラム実行

```bash
cd lambda_deployment_postgresql_updated

# 全テーブル同期
python lambda_function.py

# 単一テーブル同期（テスト用）
python lambda_function.py single_sync --table=voipdb_customer

# 接続テスト
python lambda_function.py multi_test

# テーブル情報確認
python lambda_function.py info
```

### 注意事項（ローカル実行）

`.env`ファイルで以下のIPアドレスを指定：

```bash
SQL_SERVER_MCTM_HOST=10.35.11.13
SQL_SERVER_VOIPDB_HOST=10.35.11.14
```

## Lambda デプロイメント手順

### 1. プロジェクト配置

```bash
# プロジェクトフォルダを以下の場所に配置
\\wsl.localhost\Ubuntu-24.04\home\init_user\mctm_voipdb_replicator
```

### 2. 追加モジュールのインストール（必要な場合）

新しいPythonモジュール（例：requests）を使いたい場合：

```bash
cd lambda_deployment_postgresql_updated

# 追加モジュールを deployment フォルダに直接インストール
pip install requests -t .

# 複数モジュールを同時にインストールする場合
pip install requests beautifulsoup4 -t .

# 特定バージョンを指定する場合
pip install requests==2.31.0 -t .
```

**重要な注意事項**:
- **WSL Ubuntu環境必須**: pymssql, psycopg2などのC系ライブラリを含むため、**必ずWSLのUbuntu環境**でインストールする
- **Windows環境NG**: WindowsでインストールしたPythonパッケージはLinux環境のLambdaで動作しない
- **バイナリ互換性**: C拡張を含むライブラリ（numpy, pandas, lxml等）は特にLinux環境でのインストールが必要
- `-t .` オプションで現在のフォルダ（lambda_deployment_postgresql_updated）に直接インストール
- インストール後は `build.sh` でzipファイルに含まれる
- 既存の依存関係との競合に注意（pymssql, psycopg2 など）

**なぜWSL Ubuntuを使用するのか**:
```
Windows環境     → Lambda Linux環境    = 動作しない ❌
WSL Ubuntu環境  → Lambda Linux環境    = 正常動作 ✅
```

### 3. ビルド実行（zipにパッケージングする）

```bash
cd lambda_test/lambda_deployment_postgresql_updated
chmod +x build.sh
./build.sh
```

### 4. Lambda関数にアップロード

作成されたzipファイルをAWS Lambdaにアップロード：

```bash
# アップロードファイル
\\wsl.localhost\Ubuntu-24.04\home\init_user\mctm_voipdb_replicator\lambda_deployment_postgresql_updated\lambda_function_with_postgresql.zip
```

### 注意事項（Lambda実行）

Lambda関数の設定 → 環境変数で以下を指定：

```bash
SQL_SERVER_MCTM_HOST=imesh-fdc-mdb-mctm-pri.private
SQL_SERVER_VOIPDB_HOST=imesh-fdc-mdb-voipdb-pri.private
```

## Ubuntu 環境構成メモ

### Python環境

```bash
# Pythonバージョン確認
python3 --version
# → Python 3.12.3

python --version  # 仮想環境内
# → Python 3.13.7
```

### 仮想環境管理

```bash
# 仮想環境の作成
python3.13 -m venv .venv

# 仮想環境へ切り替え
source .venv/bin/activate

# pipアップデート
python -m pip install --upgrade pip
```

## 利用可能テーブル

- **customer** - McTM顧客マスタ
- **mctm_module** - McTMモジュール管理
- **voipdb_customer** - VoipDB顧客情報
- **voipdb_useragent** - VoipDBユーザーエージェント

## 実行モード

- `multi_sync` - 複数テーブル一括同期（デフォルト）
- `multi_test` - 全データベース接続テスト
- `single_sync --table=テーブル名` - 単一テーブル同期
- `info` - テーブル情報表示

## 設定ファイル

### 環境変数設定

```bash
# .env.example を .env にコピーして編集
cp .env.example .env
```

### 設定確認

```bash
# 設定の妥当性チェック
python config.py
```