# 📦 盤點系統使用說明

## 🎯 系統概述

盤點系統是一個完整的庫存盤點解決方案，支援QR Code掃描、手機端操作、資料管理和結果匯出等功能。

## 🚀 快速開始

### 1. 初始化資料庫

首次使用前，需要將Excel檔案轉換為資料庫：

**方法一：使用批次檔**
```bash
雙擊執行：inventory_system/run_data_conversion.bat
```

**方法二：手動執行**
```bash
cd inventory_system
python data_converter.py
```

**方法三：網頁介面**
- 登入系統後進入盤點頁面
- 點擊「初始化資料庫」按鈕

### 2. 準備Excel檔案

確保以下檔案存在於 `inventory_system/` 目錄：
- `產品對照資料.xlsx` - 產品資訊對照表
- `倉別.xlsx` - 倉別資料表

## 📱 使用流程

### 一般使用者操作

1. **登入系統**
   - 使用現有帳號密碼登入
   - 在首頁選擇「盤點系統」

2. **開始盤點**
   - 點擊「開始掃描」啟動相機
   - 掃描產品上的QR Code條碼
   - 或手動輸入QR Code

3. **輸入盤點資料**
   - 系統自動顯示產品資訊
   - 選擇對應的倉別
   - 輸入盤點數量
   - 點擊「送出盤點」

4. **查看和修改記錄**
   - 在「今日盤點記錄」區域查看已盤點項目
   - 點擊「修改」按鈕可調整數量
   - 系統自動記錄修改時間

### 管理員操作（C4D002帳號）

1. **進入管理後台**
   - 登入後在首頁會看到「盤點管理後台」選項
   - 點擊進入管理介面

2. **查看統計資料**
   - 總盤點記錄數
   - 今日盤點記錄數
   - 參與使用者統計
   - 倉別盤點統計
   - 最近7天趨勢圖表

3. **匯出盤點結果**
   - 選擇日期範圍（可選）
   - 點擊「匯出Excel檔案」
   - 檔案會儲存在 `inventory_system/exports/` 目錄

## 🔧 技術架構

### 資料庫結構

**products.db - 產品資料庫**
```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    qr_code TEXT UNIQUE,
    product_name TEXT,
    product_code TEXT,
    specification TEXT,
    unit TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

**warehouses.db - 倉別資料庫**
```sql
CREATE TABLE warehouses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    warehouse_code TEXT UNIQUE,
    warehouse_name TEXT,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

**inventory_records.db - 盤點記錄資料庫**
```sql
CREATE TABLE inventory_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    qr_code TEXT,
    product_name TEXT,
    warehouse_code TEXT,
    warehouse_name TEXT,
    quantity INTEGER,
    inventory_date DATE,
    inventory_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### 目錄結構

```
inventory_system/
├── __init__.py                 # Python模組初始化
├── inventory.py               # 主要Flask Blueprint
├── data_converter.py          # 資料轉換工具
├── run_data_conversion.bat    # 批次執行檔
├── README.md                  # 說明文件
├── database/                  # 資料庫檔案目錄
│   ├── products.db
│   ├── warehouses.db
│   └── inventory_records.db
├── templates/                 # HTML模板
│   ├── inventory_index.html   # 盤點主頁面
│   └── inventory_admin.html   # 管理後台
└── exports/                   # 匯出檔案目錄
```

## 📱 手機端使用

### 相機權限設定

**Android Chrome:**
1. 開啟Chrome設定
2. 進入「網站設定」→「相機」
3. 允許網站使用相機

**iOS Safari:**
1. 開啟「設定」→「Safari」
2. 進入「相機」設定
3. 選擇「允許」

### 最佳使用體驗

- 使用現代瀏覽器（Chrome、Safari、Edge）
- 確保網路連線穩定
- 在光線充足的環境下掃描
- 保持QR Code清晰可見

## 🔒 權限管理

### 一般使用者權限
- 查看自己的盤點記錄
- 進行盤點操作
- 修改自己當日的記錄

### 管理員權限（C4D002）
- 查看所有統計資料
- 匯出盤點結果
- 存取管理後台

## 🛠️ 故障排除

### 常見問題

**1. 相機無法啟動**
- 檢查瀏覽器權限設定
- 確認使用HTTPS連線
- 嘗試重新整理頁面

**2. QR Code掃描失敗**
- 確保QR Code清晰
- 調整光線和距離
- 使用手動輸入功能

**3. 產品查詢失敗**
- 檢查資料庫是否已初始化
- 確認QR Code格式正確
- 聯繫管理員檢查產品資料

**4. 資料庫初始化失敗**
- 確認Excel檔案存在且格式正確
- 檢查檔案路徑和權限
- 查看錯誤訊息並修正

### 效能優化建議

- 定期清理舊的盤點記錄
- 使用索引提升查詢速度
- 定期備份資料庫檔案

## 📞 技術支援

如遇到技術問題，請聯繫系統管理員並提供：
- 錯誤訊息截圖
- 操作步驟描述
- 使用的瀏覽器和裝置資訊

---

**版本：** 1.0  
**更新日期：** 2025-01-23  
**開發者：** BI系統開發團隊
