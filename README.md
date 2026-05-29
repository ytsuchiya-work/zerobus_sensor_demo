# Zerobus Sensor Data Demo

Databricks Zerobus SDK を使用したリアルタイム IoT センサーデータ取り込みのデモアプリケーション。センサーデータの生成、Zerobus 経由での Delta Table への取り込み、Web UI でのリアルタイム可視化までのエンドツーエンドパイプラインを実演します。

## 概要

このデモでは以下を実演します：

- **Zerobus SDK** によるストリーミングデータの取り込み
- **Delta Table** へのリアルタイムデータ蓄積
- **SQL Warehouse** を介したデータクエリ
- **SSE (Server-Sent Events)** による Web UI へのリアルタイムプッシュ
- **Databricks Apps** による Web アプリのホスティング

## アーキテクチャ

```
┌─────────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│  Data Generator │───▶│  Zerobus SDK │───▶│  Delta Table │───▶│SQL Warehouse │───▶│  Web App │
│  (Notebook)     │    │  (Ingest)    │    │  (UC)        │    │  (Query)     │    │  (SSE)   │
└─────────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────┘
                                                                                         │
                                                                                         ▼
                                                                                   ┌──────────┐
                                                                                   │  Browser │
                                                                                   │  (UI)    │
                                                                                   └──────────┘
```

### データフロー

1. **Notebook** が IoT センサーデータを生成（5台のセンサー、1秒間隔）
2. **Zerobus SDK** がデータを JSON レコードとして取り込み
3. Zerobus がデータを **Unity Catalog Delta Table** に書き込み
4. **FastAPI アプリ** が SQL Warehouse 経由でテーブルをポーリング
5. **SSE** でブラウザにリアルタイムプッシュ

## ディレクトリ構成

```
.
├── app/                          # Databricks App (FastAPI)
│   ├── main.py                   # バックエンド：SQL Warehouse ポーリング + SSE
│   ├── app.yaml                  # Databricks Apps 設定
│   ├── requirements.txt          # Python 依存パッケージ
│   └── static/
│       └── index.html            # フロントエンド UI
├── job/
│   └── sensor_data_generator.py  # Databricks Notebook (データ生成)
├── setup/
│   └── init.sql                  # テーブル作成 DDL
└── README.md
```

## 技術詳細

### データ生成 (`job/sensor_data_generator.py`)

- 5台の仮想 IoT センサー（`sensor-floor1-A`, `sensor-floor1-B`, `sensor-floor2-A`, `sensor-floor2-B`, `sensor-roof-C`）
- 各ラウンドで全センサーの測定値を生成（温度、湿度、気圧、バッテリー、ステータス）
- ランダムウォークによるリアルな時系列データシミュレーション
- Zerobus SDK の `ingest_record()` + `flush()` でレコード単位の取り込み
- 処理時間を差し引いた正確な1秒間隔での生成

### Web アプリ (`app/`)

| コンポーネント | 技術 |
|---|---|
| バックエンド | FastAPI + uvicorn |
| リアルタイム通信 | SSE (sse-starlette) |
| データアクセス | Databricks SDK → Statement Execution API |
| フロントエンド | Vanilla JS + Chart.js |
| ホスティング | Databricks Apps |

**バックエンドの仕組み:**

- 2つの非同期ループが並行実行：
  - `fast_count_loop`: レコード数カウントを最速で更新
  - `full_data_loop`: センサーデータ全体 + デバイスサマリーを更新
- SSE ストリームがキャッシュの変更を検知してブラウザにプッシュ（20ms 間隔でチェック）

### Zerobus 設定

- **エンドポイント**: `{workspace_id}.zerobus.ap-northeast-1.cloud.databricks.com`
- **認証**: OAuth M2M（Service Principal の client_id / client_secret を Secrets に格納）
- **レコードフォーマット**: JSON
- **ターゲットテーブル**: `{catalog}.zerobus.sensor_data` (VARIANT 型の payload カラム)

## セットアップ

### 前提条件

- Databricks ワークスペース（Zerobus が有効化済み）
- Unity Catalog カタログ・スキーマ
- SQL Warehouse
- Service Principal（Zerobus 用）

### 手順

1. **テーブル作成**

   ```sql
   CREATE SCHEMA IF NOT EXISTS <catalog>.zerobus;
   CREATE TABLE IF NOT EXISTS <catalog>.zerobus.sensor_data (
     id INT,
     device STRING,
     payload VARIANT
   );
   ```

2. **Secrets 登録**

   ```bash
   databricks secrets create-scope zerobus-demo
   databricks secrets put-secret zerobus-demo client-id --string-value "<SP_CLIENT_ID>"
   databricks secrets put-secret zerobus-demo client-secret --string-value "<SP_CLIENT_SECRET>"
   ```

3. **Notebook をワークスペースにアップロード**

   ```bash
   databricks workspace import /Workspace/Users/<user>/zerobus-demo/sensor_data_generator \
     --file job/sensor_data_generator.py --format SOURCE --language PYTHON --overwrite
   ```

4. **Job を作成** — Notebook をタスクとして登録

5. **App をデプロイ**

   ```bash
   # app.yaml の DATABRICKS_WAREHOUSE_ID と SENSOR_JOB_ID を更新
   databricks apps deploy <app-name>
   ```

## 使用方法

1. アプリ URL にアクセス
2. 「Trigger Job」ボタンをクリック
3. Job が起動し、データ生成が開始
4. UI 上でリアルタイムにセンサーデータが表示される：
   - デバイスカード：各センサーの最新値
   - 折れ線グラフ：温度の時系列推移
   - データテーブル：直近レコード一覧

## 設定パラメータ

| パラメータ | デフォルト | 説明 |
|---|---|---|
| `num_records` | 100 | 生成するレコード数 |
| `interval_seconds` | 1.0 | ラウンド間の生成間隔（秒） |
| `DATABRICKS_WAREHOUSE_ID` | - | SQL Warehouse ID |
| `SENSOR_JOB_ID` | - | Notebook Job ID |
