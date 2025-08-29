#!/bin/bash
# Lambda デプロイメントパッケージビルドスクリプト

echo "=== Lambda デプロイメントパッケージ作成 ==="

# 古いzipファイルを削除
if [ -f "lambda_function_with_postgresql.zip" ]; then
    rm lambda_function_with_postgresql.zip
    echo "既存のzipファイルを削除しました"
fi

# zipファイル作成（不要なファイルを除外）
echo "zipファイルを作成中..."
zip -r lambda_function_with_postgresql.zip . \
    -x "*.pyc" "*__pycache__*" "build.sh" "README.md" "*.git*" "*.DS_Store*"

# ファイルサイズ確認
if [ -f "lambda_function_with_postgresql.zip" ]; then
    size=$(du -sh lambda_function_with_postgresql.zip | cut -f1)
    echo "✅ 作成完了: lambda_function_with_postgresql.zip ($size)"
    
    # ファイル数確認
    file_count=$(unzip -l lambda_function_with_postgresql.zip | tail -1 | awk '{print $2}')
    echo "📦 含まれるファイル数: $file_count"
else
    echo "❌ zipファイルの作成に失敗しました"
    exit 1
fi

echo "=== 完了 ==="
