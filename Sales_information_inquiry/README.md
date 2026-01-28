# 出貨維修資料查詢系統

這是一個用於查詢銷售和維修資料的網頁應用系統。

## 系統需求

- Python 3.8 或以上版本
- 必要的 Python 套件（見 requirements.txt）

## 安裝步驟

1. 安裝所需的 Python 套件：
   ```bash
   pip install -r requirements.txt
   ```

2. 準備 Excel 檔案：
   - 2025銷售資料.xlsx（每日更新）
   - 昇峰銷售資料.xlsx（固定資料）
   - 維修資料.xlsx（每日更新）
   - 昇峰維修資料.xlsx（固定資料）

3. 執行資料轉換程式：
   ```bash
   python data_converter.py
   ```

4. 啟動網頁應用：
   ```bash
   python app.py
   ```

5. 開啟瀏覽器訪問：http://localhost:5000

## 功能說明

### 資料轉換功能
- 自動合併每日更新的銷售資料和固定銷售資料
- 自動合併每日更新的維修資料和固定維修資料
- 建立 SQLite 資料庫並優化查詢效能

### 查詢功能
- 支援關鍵字搜尋（地址、備註、客戶名稱）
- 顯示銷售和維修資料
- 可展開查看銷售資料的詳細明細

## 資料庫結構

### 銷售資料庫 (sales.sqlite)
- 主表：sales
- 明細表：sales_details

### 維修資料庫 (repair.sqlite)
- 主表：repair

## 注意事項

1. 每日更新資料時，請確保 Excel 檔案的欄位名稱與系統要求一致
2. 建議定期備份資料庫檔案
3. 系統預設在本機運行，如需對外開放請注意安全性設定 