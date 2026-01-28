#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BI系統整合資料匯入工具
支援所有分支系統的資料庫匯入：銷售資訊查詢系統、庫存查詢系統、採購到貨進度追蹤系統、CRM記事
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime
import time
import re

class BIIntegratedDataConverter:
    def __init__(self):
        # BI根目錄
        self.bi_root = os.path.dirname(__file__)

        # 統一資料來源目錄
        self.data_source_dir = os.path.join(self.bi_root, "資料來源")

        # 採購到貨進度追蹤系統 (ProcureTrack)
        self.procuretrack_dir = os.path.join(self.bi_root, "ProcureTrack")
        self.procuretrack_db_dir = os.path.join(self.procuretrack_dir, "database")
        self.procuretrack_db_path = os.path.join(self.procuretrack_db_dir, "procure.db")

        # 銷售資訊查詢系統路徑
        self.sales_base_dir = os.path.join(
            self.bi_root, "Sales_information_inquiry", "database"
        )
        self.sales_db_path = os.path.join(self.sales_base_dir, "sales.db")
        self.repair_db_path = os.path.join(self.sales_base_dir, "repair.db")
        self.custody_db_path = os.path.join(self.sales_base_dir, "custody.db")
        self.customer_db_path = os.path.join(self.sales_base_dir, "customer_new.db")
        self.service_card_db_path = os.path.join(self.sales_base_dir, "service_card.db")

        # 庫存查詢系統路徑
        self.inventory_inquiry_base_dir = os.path.join(
            self.bi_root, "Inventory_inquiry_system", "database"
        )
        self.inventory_data_db_path = os.path.join(
            self.inventory_inquiry_base_dir, "inventory_data.db"
        )

        # CRM 資料庫位置
        self.crm_dir = os.path.join(self.bi_root, "CRM")
        self.crm_db_path = os.path.join(self.crm_dir, "CRM記事.db")
        self.crm_source_path = os.path.join(self.data_source_dir, "CRM記事.xlsx")

        # AI 助理資料庫位置
        self.ai_assistant_db_dir = os.path.join(self.bi_root, "ai_assistant", "database")
        self.sales_ai_db_path = os.path.join(self.ai_assistant_db_dir, "sales_ai.db")

        # 確保所有目錄存在
        for db_dir in [
            self.data_source_dir,
            self.procuretrack_db_dir,
            self.sales_base_dir,
            self.inventory_inquiry_base_dir,
            self.crm_dir,
            self.ai_assistant_db_dir,
        ]:
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)

    def log_time(self, message):
        """記錄時間和訊息"""
        current_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{current_time}] {message}")

    def clean_date_string(self, date_str):
        """清理日期字串，移除中文星期幾等干擾字符"""
        if pd.isna(date_str) or date_str == "":
            return date_str

        date_str = str(date_str)

        # 移除中文星期幾：(一), (二), (三), (四), (五), (六), (日)
        import re

        date_str = re.sub(r"\s*\([一二三四五六日]\)\s*", "", date_str)

        # 移除其他可能的干擾字符
        date_str = date_str.strip()

        return date_str

    def normalize_phone_value(self, value):
        """將電話字串轉為僅包含數字"""
        if pd.isna(value):
            return ""

        value_str = str(value).strip()
        if value_str.lower() in {"nan", "none"}:
            return ""

        digits_only = re.sub(r"\D", "", value_str)
        return digits_only

    def normalize_phone_columns(self, df):
        """將資料表中的電話欄位統一移除非數字字元"""
        if df is None or df.empty:
            return df

        phone_keywords = ["電話", "手機", "Phone", "phone", "TEL", "tel"]
        for col in df.columns:
            col_name = str(col)
            if any(keyword in col_name for keyword in phone_keywords):
                df[col] = df[col].apply(self.normalize_phone_value)

        return df

    def remove_hyphen(self, value):
        """移除字串中的連字號"""
        if pd.isna(value):
            return ""

        value_str = str(value).strip()
        if value_str.lower() in {"nan", "none"}:
            return ""

        return value_str.replace("-", "")

    def normalize_remark_columns(self, df, columns):
        """移除備註欄位中電話的連字號"""
        if df is None or df.empty or not columns:
            return df

        def _clean_value(value):
            if pd.isna(value):
                return ""

            value_str = str(value).strip()
            if value_str.lower() in {"nan", "none"}:
                return ""

            return re.sub(r"(?<=\d)-(?=\d)", "", value_str)

        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(_clean_value)

        return df

    def convert_datetime_optimized(self, df, date_columns):
        """優化的日期時間轉換，支援多種格式包括 Excel 序列號"""
        for col in date_columns:
            if col in df.columns:
                self.log_time(f"🗓️ 處理日期欄位：{col}")

                # 先檢查原始資料
                original_data = df[col].copy()
                non_null_count = original_data.notna().sum()
                self.log_time(
                    f"   原始資料：{non_null_count}/{len(original_data)} 筆有值"
                )

                if non_null_count == 0:
                    self.log_time(f"   ⚠️ {col} 欄位全部為空，跳過轉換")
                    continue

                try:
                    converted = False

                    # 檢查資料類型
                    if pd.api.types.is_datetime64_any_dtype(original_data):
                        # 如果已經是 datetime 格式，直接轉換為字串
                        df[col] = original_data.dt.strftime("%Y-%m-%d %H:%M:%S")
                        converted_count = df[col].notna().sum()
                        self.log_time(
                            f"   ✅ 已是datetime格式，直接轉換：{converted_count} 筆"
                        )
                        converted = True

                    # 方法1：檢查是否為 Excel 序列號（數字格式的日期）
                    elif original_data.dtype in [
                        "int64",
                        "float64",
                    ] or pd.api.types.is_numeric_dtype(original_data):
                        try:
                            # 過濾掉明顯不是日期的數字（太小或太大）
                            numeric_data = pd.to_numeric(original_data, errors="coerce")
                            valid_range = (numeric_data >= 1) & (
                                numeric_data <= 100000
                            )  # Excel 日期範圍

                            if valid_range.any():
                                df[col] = pd.to_datetime(
                                    numeric_data,
                                    origin="1899-12-30",
                                    unit="D",
                                    errors="coerce",
                                )
                                converted_count = df[col].notna().sum()
                                if converted_count > 0:
                                    self.log_time(
                                        f"   ✅ Excel序列號轉換成功：{converted_count} 筆"
                                    )
                                    df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                                    converted = True
                        except Exception as e:
                            self.log_time(f"   ⚠️ Excel序列號轉換失敗：{e}")

                    # 方法2：處理字串格式的日期
                    if not converted:
                        # 先清理日期字串
                        cleaned_data = original_data.apply(self.clean_date_string)
                        self.log_time(f"   🧹 清理日期字串完成")

                        # 嘗試常見的日期格式（優先處理昇峰銷售資料的格式）
                        formats_to_try = [
                            "%Y/%m/%d",  # 昇峰銷售資料格式：2020/01/16
                            "%Y-%m-%d",  # 標準格式
                            "%Y/%m/%d %H:%M:%S",  # 昇峰銷售資料帶時間
                            "%Y-%m-%d %H:%M:%S",  # 標準格式帶時間
                            "%m/%d/%Y",  # 美式格式
                            "%d/%m/%Y",  # 歐式格式
                            "%Y%m%d",  # 緊湊格式
                        ]

                        for fmt in formats_to_try:
                            try:
                                test_conversion = pd.to_datetime(
                                    cleaned_data, format=fmt, errors="coerce"
                                )
                                converted_count = test_conversion.notna().sum()
                                if converted_count > 0:
                                    df[col] = test_conversion.dt.strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    )
                                    self.log_time(
                                        f"   ✅ 格式 {fmt} 轉換成功：{converted_count} 筆"
                                    )
                                    converted = True
                                    break
                                else:
                                    # 顯示為什麼這個格式沒有轉換成功
                                    sample_data = cleaned_data.dropna().head(3)
                                    if len(sample_data) > 0:
                                        self.log_time(
                                            f"   ⚠️ 格式 {fmt} 無法轉換，樣本數據：{list(sample_data)}"
                                        )
                            except Exception as e:
                                self.log_time(f"   ⚠️ 格式 {fmt} 轉換異常：{e}")
                                continue

                    # 方法3：使用 pandas 預設轉換
                    if not converted:
                        try:
                            cleaned_data = original_data.apply(self.clean_date_string)
                            test_conversion = pd.to_datetime(
                                cleaned_data, errors="coerce"
                            )
                            converted_count = test_conversion.notna().sum()
                            if converted_count > 0:
                                df[col] = test_conversion.dt.strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                self.log_time(
                                    f"   ✅ 預設轉換成功：{converted_count} 筆"
                                )
                                converted = True
                        except Exception as e:
                            self.log_time(f"   ⚠️ 預設轉換失敗：{e}")

                    # 最終檢查
                    if converted:
                        final_count = df[col].notna().sum()
                        self.log_time(f"   📊 最終轉換結果：{final_count} 筆有效日期")
                    else:
                        # 如果所有方法都失敗，保持原樣但轉為字串
                        df[col] = original_data.astype(str)
                        self.log_time(f"   ⚠️ 日期轉換失敗，保持原始格式")

                except Exception as e:
                    self.log_time(f"   ❌ {col} 轉換過程發生錯誤：{e}")
                    df[col] = original_data.astype(str)

        return df

    def convert_shengfeng_dates(self, df, date_columns):
        """專門處理昇峰銷售資料的日期轉換"""
        for col in date_columns:
            if col in df.columns:
                self.log_time(f"🗓️ 處理昇峰銷售資料日期欄位：{col}")

                original_data = df[col].copy()
                non_null_count = original_data.notna().sum()
                self.log_time(
                    f"   原始資料：{non_null_count}/{len(original_data)} 筆有值"
                )

                if non_null_count == 0:
                    self.log_time(f"   ⚠️ {col} 欄位全部為空，跳過轉換")
                    continue

                try:
                    # 顯示原始數據樣本
                    sample_data = original_data.dropna().head(5)
                    self.log_time(f"   📅 原始日期樣本：{list(sample_data)}")

                    converted = False

                    # 方法1：直接處理YYYY/MM/DD格式
                    if not converted:
                        try:
                            # 先轉換為字符串並清理
                            str_data = original_data.astype(str).str.strip()

                            # 使用正則表達式匹配YYYY/MM/DD格式
                            import re

                            date_pattern = r"^\d{4}/\d{1,2}/\d{1,2}$"
                            valid_dates = str_data[
                                str_data.str.match(date_pattern, na=False)
                            ]

                            if len(valid_dates) > 0:
                                # 轉換YYYY/MM/DD格式
                                converted_dates = pd.to_datetime(
                                    valid_dates, format="%Y/%m/%d", errors="coerce"
                                )

                                # 更新原始數據
                                df.loc[converted_dates.index, col] = (
                                    converted_dates.dt.strftime("%Y-%m-%d %H:%M:%S")
                                )

                                converted_count = converted_dates.notna().sum()
                                self.log_time(
                                    f"   ✅ YYYY/MM/DD格式轉換成功：{converted_count} 筆"
                                )
                                converted = True
                        except Exception as e:
                            self.log_time(f"   ⚠️ YYYY/MM/DD格式轉換失敗：{e}")

                    # 方法2：使用pandas的智能轉換
                    if not converted:
                        try:
                            test_conversion = pd.to_datetime(
                                original_data, errors="coerce"
                            )
                            converted_count = test_conversion.notna().sum()
                            if converted_count > 0:
                                df[col] = test_conversion.dt.strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                self.log_time(
                                    f"   ✅ 智能轉換成功：{converted_count} 筆"
                                )
                                converted = True
                        except Exception as e:
                            self.log_time(f"   ⚠️ 智能轉換失敗：{e}")

                    # 最終檢查
                    if converted:
                        final_count = df[col].notna().sum()
                        self.log_time(f"   📊 最終轉換結果：{final_count} 筆有效日期")

                        # 顯示轉換後的樣本
                        converted_sample = df[col].dropna().head(3)
                        self.log_time(f"   📅 轉換後樣本：{list(converted_sample)}")
                    else:
                        self.log_time(f"   ⚠️ 日期轉換失敗，保持原始格式")
                        df[col] = original_data.astype(str)

                except Exception as e:
                    self.log_time(f"   ❌ {col} 轉換過程發生錯誤：{e}")
                    df[col] = original_data.astype(str)

        return df

    def create_indexes_batch(self, cursor, table_name, index_configs):
        """批量建立索引"""
        for column, index_name in index_configs:
            try:
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column})"
                )
            except Exception as e:
                print(f"建立索引 {index_name} 失敗: {e}")

    def insert_data_in_chunks(self, df, conn, table_name, chunk_size=1000):
        """分批插入資料，避免 SQLite 變數限制"""
        total_rows = len(df)
        self.log_time(
            f"📊 開始分批插入 {table_name}：總共 {total_rows:,} 筆，每批 {chunk_size:,} 筆"
        )

        # 先刪除舊表
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # 分批插入
        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            if i == 0:
                # 第一批建立表格
                chunk.to_sql(table_name, conn, if_exists="replace", index=False)
            else:
                # 後續批次追加資料
                chunk.to_sql(table_name, conn, if_exists="append", index=False)

            progress = min(i + chunk_size, total_rows)
            self.log_time(
                f"📈 {table_name} 進度：{progress:,}/{total_rows:,} ({progress/total_rows*100:.1f}%)"
            )

        self.log_time(f"✅ {table_name} 插入完成")

    def convert_customer_data(self):
        """轉換客戶資料 - 使用客戶資料.xlsx"""
        self.log_time("🔄 開始轉換客戶資料...")

        try:
            # 刪除舊的客戶資料庫
            if os.path.exists(self.customer_db_path):
                os.remove(self.customer_db_path)

            # 讀取客戶資料.xlsx（從統一資料來源目錄）
            customer_xlsx_path = os.path.join(self.data_source_dir, "客戶資料.xlsx")
            if not os.path.exists(customer_xlsx_path):
                self.log_time(f"❌ 找不到檔案：{customer_xlsx_path}")
                return

            # 讀取Excel檔案
            df_customer = pd.read_excel(customer_xlsx_path)
            self.log_time(f"📊 讀取客戶資料：{len(df_customer)} 筆")

            # 建立新的客戶資料庫
            conn = sqlite3.connect(self.customer_db_path)
            cursor = conn.cursor()

            # 建立客戶基本資料表
            cursor.execute(
                """
                CREATE TABLE customer_basic (
                    客戶代碼 TEXT PRIMARY KEY,
                    客戶名稱 TEXT,
                    聯絡地址 TEXT,
                    郵遞區號 TEXT,
                    聯絡電話 TEXT,
                    聯絡人 TEXT,
                    業務人員名稱 TEXT,
                    銷售分類碼名稱 TEXT,
                    預設銷售通路名稱 TEXT,
                    正航舊編碼 TEXT,
                    舊編碼 TEXT,
                    建立時間 DATETIME DEFAULT CURRENT_TIMESTAMP,
                    更新時間 DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # 建立客戶聯絡人表
            cursor.execute(
                """
                CREATE TABLE customer_contacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    客戶代碼 TEXT,
                    聯絡人姓名 TEXT,
                    聯絡電話 TEXT,
                    聯絡手機 TEXT,
                    電子郵件 TEXT,
                    職稱 TEXT,
                    備註 TEXT,
                    FOREIGN KEY (客戶代碼) REFERENCES customer_basic(客戶代碼)
                )
            """
            )

            # 建立客戶送貨地址表
            cursor.execute(
                """
                CREATE TABLE customer_addresses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    客戶代碼 TEXT,
                    地址名稱 TEXT,
                    送貨地址 TEXT,
                    郵遞區號 TEXT,
                    聯絡人 TEXT,
                    聯絡電話 TEXT,
                    是否預設 INTEGER DEFAULT 0,
                    備註 TEXT,
                    FOREIGN KEY (客戶代碼) REFERENCES customer_basic(客戶代碼)
                )
            """
            )

            # 處理客戶資料欄位對應
            column_mapping = {
                "客戶代碼": ["客戶代碼", "Unnamed: 0"],
                "客戶名稱": ["客戶名稱", "Unnamed: 3"],
                "聯絡地址": ["聯絡地址", "Unnamed: 2"],
                "郵遞區號": ["郵遞區號", "Unnamed: 1"],
                "聯絡電話": ["聯絡電話", "Unnamed: 10"],
                "聯絡人": ["聯絡人", "Unnamed: 9"],
                "業務人員名稱": ["業務人員名稱", "Unnamed: 4"],
                "銷售分類碼名稱": ["銷售分類碼名稱", "Unnamed: 5"],
                "預設銷售通路名稱": ["預設銷售通路名稱", "Unnamed: 6"],
                "正航舊編碼": ["正航舊編碼", "Unnamed: 7"],
                "舊編碼": ["舊編碼", "Unnamed: 8"],
            }

            # 建立標準化的客戶資料 DataFrame
            customer_df = pd.DataFrame()
            for target_col, possible_cols in column_mapping.items():
                for col in possible_cols:
                    if col in df_customer.columns:
                        customer_df[target_col] = (
                            df_customer[col]
                            .astype(str)
                            .replace("nan", "")
                            .replace("None", "")
                        )
                        break
                if target_col not in customer_df.columns:
                    customer_df[target_col] = ""

            # 過濾掉無效的資料行
            customer_df = customer_df[
                (customer_df["客戶代碼"] != "")
                & (customer_df["客戶代碼"] != "客戶資料")
                & (customer_df["客戶代碼"] != "代碼")
                & (customer_df["客戶代碼"].notna())
            ]

            customer_df = self.normalize_phone_columns(customer_df)

            # 使用分批插入方法
            self.insert_data_in_chunks(
                customer_df, conn, "customer_basic", chunk_size=1000
            )

            # 建立索引
            index_configs = [
                ("客戶名稱", "idx_customer_name"),
                ("客戶代碼", "idx_customer_code"),
                ("聯絡電話", "idx_customer_phone"),
                ("聯絡地址", "idx_customer_address"),
            ]
            self.create_indexes_batch(cursor, "customer_basic", index_configs)

            conn.commit()
            conn.close()

            self.log_time(f"✅ 客戶資料轉換完成：{len(customer_df)} 筆")

        except Exception as e:
            self.log_time(f"❌ 轉換客戶資料時發生錯誤：{str(e)}")

    def convert_sales_data(self):
        """轉換銷售資料"""
        self.log_time("🔄 開始轉換銷售資料...")

        try:
            # 讀取銷售資料檔案（從統一資料來源目錄）
            df_shipment_path = os.path.join(self.data_source_dir, "發貨狀況分析表.xlsx")
            df_shengfeng_path = os.path.join(self.data_source_dir, "昇峰銷售資料.xlsx")

            df_list = []

            if os.path.exists(df_shipment_path):
                df_shipment = pd.read_excel(df_shipment_path)
                df_list.append(df_shipment)
                self.log_time(f"📊 發貨狀況分析表：{len(df_shipment)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shipment_path}")

            if os.path.exists(df_shengfeng_path):
                df_shengfeng = pd.read_excel(df_shengfeng_path)
                df_list.append(df_shengfeng)
                self.log_time(f"📊 昇峰銷售資料：{len(df_shengfeng)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shengfeng_path}")

            if not df_list:
                self.log_time("❌ 沒有找到任何銷售資料檔案")
                return

            # 轉換日期時間
            date_columns = ["發貨日期", "單據日期"]
            for i, df in enumerate(df_list):
                if i == 0:  # 發貨狀況分析表
                    df_list[i] = self.convert_datetime_optimized(df, date_columns)
                else:  # 昇峰銷售資料
                    df_list[i] = self.convert_shengfeng_dates(df, date_columns)

            # 合併資料
            df_combined = pd.concat(df_list, ignore_index=True)
            df_combined = self.normalize_remark_columns(df_combined, ["備註"])
            self.log_time(f"📊 合併後總筆數：{len(df_combined)}")

            # 建立資料庫連接
            conn = sqlite3.connect(self.sales_db_path)

            # 儲存主檔資料（去重複）
            main_columns = [
                "單據編號",
                "發貨日期",
                "客戶名稱",
                "送貨地址",
                "聯絡電話",
                "備註",
                "業務人員名稱",
                "客戶代碼",
            ]
            # 統一補齊與排序，確保優先保留含「客戶名稱」之紀錄
            try:
                if "客戶名稱" in df_combined.columns:
                    df_combined["客戶名稱"] = (
                        df_combined["客戶名稱"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "客戶" in df_combined.columns:
                    df_combined["客戶"] = (
                        df_combined["客戶"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "客戶名稱" not in df_combined.columns and "客戶" in df_combined.columns:
                    df_combined["客戶名稱"] = df_combined["客戶"]
                elif "客戶名稱" in df_combined.columns and "客戶" in df_combined.columns:
                    mask_empty = df_combined["客戶名稱"].isin(["", "nan", "None"]) | df_combined["客戶名稱"].isna()
                    df_combined.loc[mask_empty, "客戶名稱"] = df_combined.loc[mask_empty, "客戶"].fillna("")

                if "單據編號" in df_combined.columns and "客戶名稱" in df_combined.columns:
                    df_combined["__has_name__"] = df_combined["客戶名稱"].astype(str).str.strip().ne("")
                    df_combined.sort_values(["單據編號", "__has_name__"], ascending=[True, False], inplace=True)
            except Exception:
                pass

            available_main_columns = [
                col for col in main_columns if col in df_combined.columns
            ]
            df_main = df_combined[available_main_columns].drop_duplicates(
                subset=["單據編號"]
            )

            # 使用分批插入方法
            self.insert_data_in_chunks(df_main, conn, "sales_main", chunk_size=1000)

            # 儲存明細資料
            detail_columns = ["單據編號", "產品名稱", "交易數量", "交易價", "含稅金額"]
            available_detail_columns = [
                col for col in detail_columns if col in df_combined.columns
            ]
            df_detail = df_combined[available_detail_columns]

            # 使用分批插入方法
            self.insert_data_in_chunks(df_detail, conn, "sales_detail", chunk_size=1000)

            # 建立索引
            cursor = conn.cursor()
            sales_indexes = [
                ("單據編號", "idx_sales_main_doc_no"),
                ("客戶名稱", "idx_sales_main_customer"),
                ("送貨地址", "idx_sales_main_address"),
                ("聯絡電話", "idx_sales_main_phone"),
                ("備註", "idx_sales_main_remark"),
                ("客戶代碼", "idx_sales_main_customer_code"),
            ]
            self.create_indexes_batch(cursor, "sales_main", sales_indexes)

            detail_indexes = [("單據編號", "idx_sales_detail_doc_no")]
            self.create_indexes_batch(cursor, "sales_detail", detail_indexes)

            conn.commit()
            conn.close()

            self.log_time(
                f"✅ 銷售資料轉換完成：主檔 {len(df_main)} 筆，明細 {len(df_detail)} 筆"
            )

        except Exception as e:
            self.log_time(f"❌ 轉換銷售資料時發生錯誤：{str(e)}")

    def convert_repair_data(self):
        """轉換維修資料"""
        self.log_time("🔄 開始轉換維修資料...")

        try:
            # 讀取維修資料檔案（從統一資料來源目錄）
            df_repair_path = os.path.join(self.data_source_dir, "維修資料.xlsx")
            df_shengfeng_path = os.path.join(self.data_source_dir, "昇峰維修資料.xlsx")

            df_list = []

            if os.path.exists(df_repair_path):
                df_repair = pd.read_excel(df_repair_path)
                df_list.append(df_repair)
                self.log_time(f"📊 維修資料：{len(df_repair)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_repair_path}")

            if os.path.exists(df_shengfeng_path):
                df_shengfeng = pd.read_excel(df_shengfeng_path)
                df_list.append(df_shengfeng)
                self.log_time(f"📊 昇峰維修資料：{len(df_shengfeng)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shengfeng_path}")

            if not df_list:
                self.log_time("❌ 沒有找到任何維修資料檔案")
                return

            # 處理客戶欄位（避免浮點數顯示）
            for df in df_list:
                if "客戶" in df.columns:
                    df["客戶"] = (
                        df["客戶"].astype(str).replace("nan", "").replace("None", "")
                    )
                    # 去除浮點數的小數部分
                    df["客戶"] = df["客戶"].apply(
                        lambda x: x.split(".")[0] if "." in str(x) else x
                    )

            # 轉換日期時間
            date_columns = ["出勤開始時間", "發貨日期", "單據日期"]
            for i, df in enumerate(df_list):
                df_list[i] = self.convert_datetime_optimized(df, date_columns)

            # 合併資料
            df_combined = pd.concat(df_list, ignore_index=True)
            df_combined = self.normalize_remark_columns(df_combined, ["備註"])
            self.log_time(f"📊 合併後總筆數：{len(df_combined)}")

            # 添加客戶代碼欄位
            if "客戶" in df_combined.columns and "客戶代碼" not in df_combined.columns:
                df_combined["客戶代碼"] = df_combined["客戶"]

            # 建立資料庫連接
            # normalize customer name for repair
            try:
                if "客戶名稱" in df_combined.columns:
                    df_combined["客戶名稱"] = (
                        df_combined["客戶名稱"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "客戶" in df_combined.columns:
                    df_combined["客戶"] = (
                        df_combined["客戶"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "客戶名稱" not in df_combined.columns and "客戶" in df_combined.columns:
                    df_combined["客戶名稱"] = df_combined["客戶"]
                elif "客戶名稱" in df_combined.columns and "客戶" in df_combined.columns:
                    __mask_empty = df_combined["客戶名稱"].isin(["", "nan", "None"]) | df_combined["客戶名稱"].isna()
                    df_combined.loc[__mask_empty, "客戶名稱"] = df_combined.loc[__mask_empty, "客戶"].fillna("")
                if "單據編號" in df_combined.columns and "客戶名稱" in df_combined.columns:
                    df_combined["__has_name__"] = df_combined["客戶名稱"].astype(str).str.strip().ne("")
                    df_combined.sort_values(["單據編號", "__has_name__"], ascending=[True, False], inplace=True)
                    df_combined = df_combined.drop_duplicates(subset=["單據編號"], keep="first")
                    if "__has_name__" in df_combined.columns:
                        df_combined.drop(columns=["__has_name__"], inplace=True)
            except Exception:
                pass

            conn = sqlite3.connect(self.repair_db_path)

            # 使用分批插入方法
            self.insert_data_in_chunks(
                df_combined, conn, "repair_data", chunk_size=1000
            )

            # 建立索引
            cursor = conn.cursor()
            repair_indexes = [
                ("單據編號", "idx_repair_doc_no"),
                ("客戶名稱", "idx_repair_customer"),
                ("服務地址", "idx_repair_address"),
                ("聯絡電話", "idx_repair_phone"),
                ("備註", "idx_repair_remark"),
                ("客戶代碼", "idx_repair_customer_code"),
            ]
            self.create_indexes_batch(cursor, "repair_data", repair_indexes)

            conn.commit()
            conn.close()

            self.log_time(f"✅ 維修資料轉換完成：{len(df_combined)} 筆")

        except Exception as e:
            self.log_time(f"❌ 轉換維修資料時發生錯誤：{str(e)}")

    def convert_crm_notes(self):
        """轉換 CRM記事.xlsx 為 SQLite"""
        self.log_time("?? 開始轉換 CRM記事.xlsx ...")

        try:
            if not os.path.exists(self.crm_source_path):
                self.log_time(f"?? 找不到檔案：{self.crm_source_path}")
                return False

            os.makedirs(self.crm_dir, exist_ok=True)

            df = pd.read_excel(self.crm_source_path)
            self.log_time(f"?? CRM記事原始列數：{len(df)} 筆")

            normalized_columns = []
            for idx, col in enumerate(df.columns):
                if pd.isna(col) or str(col).strip() == "":
                    normalized_columns.append(f"欄位_{idx + 1}")
                else:
                    normalized_columns.append(str(col).strip())
            df.columns = normalized_columns

            before_drop = len(df)
            df = df.dropna(how="all")
            self.log_time(f"?? 清理空白列後：{before_drop} 筆 -> {len(df)} 筆")

            conn = sqlite3.connect(self.crm_db_path)
            df.to_sql('crm_notes', conn, if_exists='replace', index=False)
            conn.close()

            self.log_time(f"? CRM記事寫入 SQLite 完成，共 {len(df)} 筆")
            return True

        except Exception as e:
            self.log_time(f"? 轉換 CRM記事.xlsx 發生錯誤：{str(e)}")
            return False
    def convert_inventory_inquiry_data(self):
        """轉換正航庫存資料"""
        self.log_time("🔄 開始轉換正航庫存資料...")

        try:
            # 讀取Excel檔案（從統一資料來源目錄）
            excel_path = os.path.join(self.data_source_dir, "正航庫存資料.xlsx")
            if not os.path.exists(excel_path):
                self.log_time(f"❌ 找不到檔案：{excel_path}")
                return False

            df = pd.read_excel(excel_path)
            self.log_time(f"📊 讀取到 {len(df)} 筆庫存資料")

            # 建立資料庫連接
            conn = sqlite3.connect(self.inventory_data_db_path)
            cursor = conn.cursor()

            # 刪除舊表格並重新建立
            cursor.execute("DROP TABLE IF EXISTS inventory_data")

            # 建立庫存資料表（實際插入時會由 DataFrame 透過 to_sql 重建，此結構僅供參考）
            cursor.execute(
                """
                CREATE TABLE inventory_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_name TEXT,
                    warehouse_name TEXT,
                    warehouse_partner_name TEXT,
                    inventory_type TEXT,
                    product_code TEXT,
                    specification TEXT,
                    unit TEXT,
                    quantity REAL,
                    unit_price REAL,
                    total_amount REAL,
                    last_update_date TEXT,
                    remark TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # 處理資料欄位對應（根據實際Excel結構調整）
            # 先檢查Excel檔案的欄位結構
            self.log_time(f"📋 Excel欄位：{list(df.columns)}")

            # 如果Excel檔案有資料，進行轉換
            if len(df) > 0:
                # 建立欄位對應字典
                column_mapping = {}
                for col in df.columns:
                    if "產品名稱" in col:
                        column_mapping[col] = "product_name"
                    elif "倉庫名稱" in col:
                        column_mapping[col] = "warehouse_name"
                    elif "倉庫往來對象名稱" in col or "往來對象" in col:
                        column_mapping[col] = "warehouse_partner_name"
                    elif "存貨屬性" in col or "庫存屬性" in col or col == "屬性":
                        # 直接沿用 Excel 的存貨屬性，不做邏輯換算
                        column_mapping[col] = "inventory_type"
                    elif "產品代碼" in col or "代碼" in col:
                        column_mapping[col] = "product_code"
                    elif "規格" in col:
                        column_mapping[col] = "specification"
                    elif "單位" in col:
                        column_mapping[col] = "unit"
                    elif "數量" in col:
                        column_mapping[col] = "quantity"
                    elif "單價" in col:
                        column_mapping[col] = "unit_price"
                    elif "總金額" in col or "金額" in col:
                        column_mapping[col] = "total_amount"
                    elif "更新日期" in col or "日期" in col:
                        column_mapping[col] = "last_update_date"
                    elif "備註" in col:
                        column_mapping[col] = "remark"

                # 重新命名欄位
                df_mapped = df.rename(columns=column_mapping)

                # 確保必要欄位存在，如果不存在則填入空值
                required_columns = [
                    "product_name",
                    "warehouse_name",
                    "warehouse_partner_name",
                    "inventory_type",
                    "product_code",
                    "specification",
                    "unit",
                    "quantity",
                    "unit_price",
                    "total_amount",
                    "last_update_date",
                    "remark",
                ]

                for col in required_columns:
                    if col not in df_mapped.columns:
                        df_mapped[col] = ""

                # 只保留需要的欄位
                df_final = df_mapped[required_columns]

                df_final = self.normalize_remark_columns(df_final, ["remark"])
                # 存貨屬性欄位標準化 + 顯示用文字對應
                # 自有庫存 -> 世磊、借入庫存 -> 寄倉、借出庫存 -> 借出
                if "inventory_type" in df_final.columns:
                    inv = df_final["inventory_type"].fillna("").astype(str).str.strip()
                    mapping = {
                        "自有庫存": "世磊",
                        "借入庫存": "寄倉",
                        "借出庫存": "借出",
                        # 也支援可能的簡寫
                        "自有": "世磊",
                        "借入": "寄倉",
                        "借出": "借出",
                    }
                    df_final["inventory_type"] = inv.replace(mapping)

                # 使用分批插入方法
                self.insert_data_in_chunks(
                    df_final, conn, "inventory_data", chunk_size=1000
                )

                # 建立索引（針對查詢需求）
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_product_name ON inventory_data(product_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warehouse_name ON inventory_data(warehouse_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warehouse_partner_name ON inventory_data(warehouse_partner_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_inventory_type ON inventory_data(inventory_type)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_product_code ON inventory_data(product_code)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_quantity ON inventory_data(quantity)"
                )

                # 建立複合索引（提升多欄位查詢效能）
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_product_warehouse ON inventory_data(product_name, warehouse_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warehouse_partner ON inventory_data(warehouse_name, warehouse_partner_name)"
                )

            # 提交並關閉
            conn.commit()

            # 驗證資料
            cursor.execute("SELECT COUNT(*) FROM inventory_data")
            count = cursor.fetchone()[0]
            conn.close()

            self.log_time(f"✅ 正航庫存資料轉換完成：{count} 筆")
            return True

        except Exception as e:
            self.log_time(f"❌ 轉換正航庫存資料時發生錯誤：{str(e)}")
            return False

    def convert_procuretrack_data(self):
        """轉換採購到貨進度追蹤系統資料"""
        self.log_time("🔄 開始轉換採購到貨進度資料...")

        try:
            # 讀取 Excel 檔案
            procure_xlsx_path = os.path.join(self.data_source_dir, "採購狀況明細表.xlsx")
            if not os.path.exists(procure_xlsx_path):
                self.log_time(f"❌ 找不到檔案：{procure_xlsx_path}")
                return

            df_procure = pd.read_excel(procure_xlsx_path)
            self.log_time(f"📊 讀取採購資料：{len(df_procure)} 筆")

            # 轉換日期
            # 欄位名稱可能變動，這裡列出可能的日期欄位
            date_columns = [
                "採購日期",
                "預交日期",
                "船期",
                "進倉日期",
                "結案日期",
                "日期",
                "建立日期",
            ]
            df_procure = self.convert_datetime_optimized(df_procure, date_columns)

            # 建立資料庫連接
            conn = sqlite3.connect(self.procuretrack_db_path)

            # 使用分批插入方法
            # 假設只有一張表 procure_data
            self.insert_data_in_chunks(
                df_procure, conn, "procure_data", chunk_size=1000
            )

            # 建立索引
            # 假設常用查詢欄位
            cursor = conn.cursor()
            procure_indexes = [
                ("採購單號", "idx_procure_po"),
                ("廠商名稱", "idx_procure_vendor"),
                ("產品名稱", "idx_procure_product"),
            ]
            # 檢查這些欄位是否存在再建立索引
            valid_indexes = []
            for col, idx_name in procure_indexes:
                if col in df_procure.columns:
                    valid_indexes.append((col, idx_name))

            self.create_indexes_batch(cursor, "procure_data", valid_indexes)

            conn.commit()
            conn.close()

            self.log_time(f"✅ 採購資料轉換完成：{len(df_procure)} 筆")

        except Exception as e:
            self.log_time(f"❌ 轉換採購資料時發生錯誤：{str(e)}")

    def convert_sales_ai_data(self):
        """轉換 AI 專用銷售資料 (合併發貨狀況分析表與昇峰銷售資料)"""
        self.log_time("🔄 開始轉換 AI 專用銷售資料 (sales_ai.db)...")

        try:
            # 讀取銷售資料檔案
            df_shipment_path = os.path.join(self.data_source_dir, "發貨狀況分析表.xlsx")
            df_shengfeng_path = os.path.join(self.data_source_dir, "昇峰銷售資料.xlsx")

            df_list = []

            if os.path.exists(df_shipment_path):
                df_shipment = pd.read_excel(df_shipment_path)
                df_list.append(df_shipment)
                self.log_time(f"📊 發貨狀況分析表：{len(df_shipment)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shipment_path}")

            if os.path.exists(df_shengfeng_path):
                df_shengfeng = pd.read_excel(df_shengfeng_path)
                df_list.append(df_shengfeng)
                self.log_time(f"📊 昇峰銷售資料：{len(df_shengfeng)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shengfeng_path}")

            if not df_list:
                self.log_time("❌ 沒有找到任何銷售資料檔案")
                return

            # 轉換日期時間
            # 這裡需要處理所有可能的日期欄位，確保 AI 能正確查詢
            date_columns = ["發貨日期", "單據日期", "建立日期", "過帳日期"]
            for i, df in enumerate(df_list):
                 # 使用既有的日期轉換邏輯
                if i == 0:  # 發貨狀況分析表
                     df_list[i] = self.convert_datetime_optimized(df, date_columns)
                else: # 昇峰銷售資料 (通常有特殊日期格式)
                     df_list[i] = self.convert_shengfeng_dates(df, date_columns)

            # 合併資料 (concat 會自動處理欄位不一致，缺少的欄位補 NaN)
            df_combined = pd.concat(df_list, ignore_index=True)
            self.log_time(f"📊 合併後總筆數：{len(df_combined)}")

            # 移除備註中的電話連字號 (選用，為了保持與 sales.db 一致)
            df_combined = self.normalize_remark_columns(df_combined, ["備註"])

            # 建立資料庫連接
            conn = sqlite3.connect(self.sales_ai_db_path)
            cursor = conn.cursor()

            # 使用分批插入方法，將所有資料寫入單一資料表 'sales_data'
            self.insert_data_in_chunks(df_combined, conn, "sales_data", chunk_size=1000)

            # 建立索引 (針對可能的常用查詢欄位)
            # 先檢查欄位是否存在
            potential_indexes = [
                ("單據編號", "idx_sales_ai_doc_no"),
                ("發貨日期", "idx_sales_ai_date"),
                ("客戶名稱", "idx_sales_ai_customer"),
                ("產品名稱", "idx_sales_ai_product"),
                ("業務人員名稱", "idx_sales_ai_salesperson"),
                ("客戶代碼", "idx_sales_ai_customer_code"),
            ]
            
            valid_indexes = []
            for col, idx_name in potential_indexes:
                if col in df_combined.columns:
                    valid_indexes.append((col, idx_name))
            
            self.create_indexes_batch(cursor, "sales_data", valid_indexes)

            conn.commit()
            conn.close()

            self.log_time(f"✅ AI 銷售資料轉換完成：{len(df_combined)} 筆 -> {self.sales_ai_db_path}")

        except Exception as e:
            self.log_time(f"❌ 轉換 AI 銷售資料時發生錯誤：{str(e)}")

    def convert_custody_data(self):
        """轉換寄倉資料"""
        self.log_time("🔄 開始轉換寄倉資料...")

        try:
            custody_xlsx_1 = os.path.join(self.data_source_dir, "寄倉資料.xlsx")
            custody_xlsx_2 = os.path.join(self.data_source_dir, "昇峰寄庫資料.xlsx")

            raw_frames = []
            if os.path.exists(custody_xlsx_1):
                df1 = pd.read_excel(custody_xlsx_1)
                raw_frames.append(df1)
                self.log_time(f"📊 寄倉資料：{len(df1)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{custody_xlsx_1}")

            if os.path.exists(custody_xlsx_2):
                df2 = pd.read_excel(custody_xlsx_2)
                raw_frames.append(df2)
                self.log_time(f"📊 昇峰寄庫資料：{len(df2)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{custody_xlsx_2}")

            if not raw_frames:
                self.log_time("❌ 沒有找到任何寄倉資料檔案")
                return

            date_columns = ["單據日期"]
            processed_frames = []
            for idx, frame in enumerate(raw_frames):
                frame = self.normalize_phone_columns(frame.copy())
                if idx == 0:
                    processed_frames.append(
                        self.convert_datetime_optimized(frame, date_columns)
                    )
                else:
                    processed_frames.append(
                        self.convert_shengfeng_dates(frame, date_columns)
                    )

            df = pd.concat(processed_frames, ignore_index=True)
            df = self.normalize_remark_columns(df, ["備註"])
            self.log_time(f"📊 合併後總筆數：{len(df)}")
            # 主檔欄位對應
            main_columns_mapping = {
                "單據編號": "單據編號",
                "單據日期": "單據日期",
                "借貨對象": "客戶代碼",
                "借貨對象名稱": "客戶名稱",
                "業務人員名稱": "業務人員名稱",
                "聯絡電話": "聯絡電話",
                "送貨地址": "送貨地址",
                "備註": "備註",
            }

            available_main_columns = {
                k: v for k, v in main_columns_mapping.items() if k in df.columns
            }
            # 規整來源名稱欄位，讓映射能產出完整的「客戶名稱」
            try:
                if "客戶名稱" in df.columns:
                    df["客戶名稱"] = (
                        df["客戶名稱"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "寄貨對象名稱" in df.columns:
                    df["寄貨對象名稱"] = (
                        df["寄貨對象名稱"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "客戶" in df.columns:
                    df["客戶"] = (
                        df["客戶"].astype(str)
                        .replace("nan", "")
                        .replace("None", "")
                        .str.strip()
                    )
                if "寄貨對象名稱" not in df.columns and "客戶" in df.columns:
                    df["寄貨對象名稱"] = df["客戶"]
                elif "寄貨對象名稱" in df.columns and "客戶" in df.columns:
                    __mask_empty_cus = df["寄貨對象名稱"].isin(["", "nan", "None"]) | df["寄貨對象名稱"].isna()
                    df.loc[__mask_empty_cus, "寄貨對象名稱"] = df.loc[__mask_empty_cus, "客戶"].fillna("")
                if "寄貨對象名稱" not in df.columns and "客戶名稱" in df.columns:
                    df["寄貨對象名稱"] = df["客戶名稱"]
                elif "寄貨對象名稱" in df.columns and "客戶名稱" in df.columns:
                    __mask_empty_cus2 = df["寄貨對象名稱"].isin(["", "nan", "None"]) | df["寄貨對象名稱"].isna()
                    df.loc[__mask_empty_cus2, "寄貨對象名稱"] = df.loc[__mask_empty_cus2, "客戶名稱"].fillna("")
            except Exception:
                pass

            df_main = df[list(available_main_columns.keys())].rename(
                columns=available_main_columns
            )
            # 依單據編號排序，讓有「客戶名稱」的紀錄優先
            try:
                if "單據編號" in df_main.columns and "客戶名稱" in df_main.columns:
                    df_main["__has_name__"] = df_main["客戶名稱"].astype(str).str.strip().ne("")
                    df_main.sort_values(["單據編號", "__has_name__"], ascending=[True, False], inplace=True)
                    if "__has_name__" in df_main.columns:
                        df_main.drop(columns=["__has_name__"], inplace=True)
            except Exception:
                pass
            df_main = df_main.drop_duplicates(subset=["單據編號"])

            # 處理客戶代碼格式
            if "客戶代碼" in df_main.columns:
                df_main["客戶代碼"] = (
                    df_main["客戶代碼"]
                    .astype(str)
                    .replace("nan", "")
                    .replace("None", "")
                )

            # 明細欄位對應
            detail_columns_mapping = {
                "單據編號": "單據編號",
                "產品名稱": "產品名稱",
                "倉庫確認數量": "交易數量",
            }

            available_detail_columns = {
                k: v for k, v in detail_columns_mapping.items() if k in df.columns
            }
            df_detail = df[list(available_detail_columns.keys())].rename(
                columns=available_detail_columns
            )

            # 建立資料庫連接
            conn = sqlite3.connect(self.custody_db_path)

            # 使用分批插入方法
            self.insert_data_in_chunks(df_main, conn, "custody_main", chunk_size=1000)
            self.insert_data_in_chunks(
                df_detail, conn, "custody_detail", chunk_size=1000
            )

            # 建立索引
            cursor = conn.cursor()
            custody_main_indexes = [
                ("單據編號", "idx_custody_main_doc_no"),
                ("客戶名稱", "idx_custody_main_customer"),
                ("送貨地址", "idx_custody_main_address"),
                ("聯絡電話", "idx_custody_main_phone"),
                ("備註", "idx_custody_main_remark"),
            ]
            self.create_indexes_batch(cursor, "custody_main", custody_main_indexes)

            custody_detail_indexes = [("單據編號", "idx_custody_detail_doc_no")]
            self.create_indexes_batch(cursor, "custody_detail", custody_detail_indexes)

            conn.commit()
            conn.close()

            self.log_time(
                f"✅ 寄倉資料轉換完成：主檔 {len(df_main)} 筆，明細 {len(df_detail)} 筆"
            )

        except Exception as e:
            self.log_time(f"❌ 轉換寄倉資料時發生錯誤：{str(e)}")

    def convert_service_card_data(self):
        """轉換服務登記卡資料"""
        self.log_time("🔄 開始轉換服務登記卡資料...")

        try:
            # 讀取服務登記卡檔案（從統一資料來源目錄）
            service_card_path = os.path.join(self.data_source_dir, "服務登記卡.xlsx")

            if not os.path.exists(service_card_path):
                self.log_time(f"⚠️ 找不到檔案：{service_card_path}")
                return

            # 讀取Excel檔案，跳過前兩行（標題行）
            df = pd.read_excel(service_card_path, skiprows=2)
            self.log_time(f"📊 服務登記卡原始資料：{len(df)} 筆")

            # 過濾掉空白行
            df = df.dropna(how="all")
            self.log_time(f"📊 服務登記卡有效資料：{len(df)} 筆")

            df = self.normalize_phone_columns(df)

            # 重新命名欄位（根據實際的Excel結構）
            if len(df.columns) >= 18:
                column_mapping = {
                    df.columns[0]: "服務登記號",
                    df.columns[1]: "核算組織",
                    df.columns[2]: "客戶代碼",
                    df.columns[3]: "客戶名稱",
                    df.columns[4]: "聯絡人",
                    df.columns[5]: "聯絡電話",
                    df.columns[6]: "服務地址",
                    df.columns[7]: "產品型號",
                    df.columns[8]: "產品序號",
                    df.columns[9]: "服務項目",
                    df.columns[10]: "服務人員",
                    df.columns[11]: "登記日期",
                    df.columns[12]: "服務日期",
                    df.columns[13]: "完成日期",
                    df.columns[14]: "服務狀態",
                    df.columns[15]: "備註",
                    df.columns[16]: "保固期限",
                    df.columns[17]: "裝機位置說明",
                }
                df = df.rename(columns=column_mapping)

                df = self.normalize_remark_columns(df, ["備註"])

            # 轉換日期時間
            date_columns = ["登記日期", "服務日期", "完成日期", "保固期限"]
            df = self.convert_datetime_optimized(df, date_columns)

            # 處理客戶代碼欄位（避免浮點數顯示）
            if "客戶代碼" in df.columns:
                df["客戶代碼"] = (
                    df["客戶代碼"].astype(str).replace("nan", "").replace("None", "")
                )
                # 去除浮點數的小數部分
                df["客戶代碼"] = df["客戶代碼"].apply(
                    lambda x: x.split(".")[0] if "." in str(x) else x
                )

            # 建立資料庫連接
            conn = sqlite3.connect(self.service_card_db_path)

            # 使用分批插入方法
            self.insert_data_in_chunks(df, conn, "service_card_data", chunk_size=1000)

            # 建立索引（使用實際的欄位名稱）
            cursor = conn.cursor()
            service_card_indexes = [
                ("服務登記號", "idx_service_card_no"),
                ("客戶名稱", "idx_service_card_customer"),
                ("客戶代碼", "idx_service_card_customer_code"),
                ("服務地址", "idx_service_card_address"),
                ("聯絡電話", "idx_service_card_phone"),
                ("服務項目", "idx_service_card_service"),
                ("服務人員", "idx_service_card_staff"),
                ("登記日期", "idx_service_card_reg_date"),
                ("服務日期", "idx_service_card_service_date"),
                ("產品型號", "idx_service_card_model"),
                ("產品序號", "idx_service_card_serial"),
            ]
            self.create_indexes_batch(cursor, "service_card_data", service_card_indexes)

            # 讀取服務登記卡 第二頁（明細），匯入 service_card_detail
            try:
                df_detail_raw = pd.read_excel(service_card_path, sheet_name=1, skiprows=2)
                # 若偵測到 Unnamed 欄位，代表表頭跨行，改用 header=None 後第2列當表頭
                if any(str(c).startswith("Unnamed") for c in df_detail_raw.columns):
                    df_tmp = pd.read_excel(service_card_path, sheet_name=1, skiprows=2, header=None)
                    if len(df_tmp) >= 2:
                        header = [str(x).strip() if pd.notna(x) else "" for x in df_tmp.iloc[1].tolist()]
                        df_detail = df_tmp.iloc[2:].copy()
                        df_detail.columns = header
                    else:
                        df_detail = df_detail_raw
                else:
                    df_detail = df_detail_raw

                # 去除全空列
                before_detail = len(df_detail)
                df_detail = df_detail.dropna(how="all")
                self.log_time(f"📊 服務登記卡-明細 原始筆數: {before_detail} -> 清理後: {len(df_detail)}")

                # 欄位名稱對應：將常見異名對應到系統欄位
                wanted_map = {
                    "服務登記號": ["服務登記號", "服務登記卡號", "登記號", "A登記號"],
                    "物料代碼": ["物料代碼", "品號", "料號"],
                    "產品名稱": ["產品名稱", "品名"],
                    "標準售價": ["標準售價", "售價", "單價"],
                    "更換期限_月": ["更換期限_月", "更換期限(月)", "更換期限"],
                    "上次更換": ["上次更換"],
                    "上次通知": ["上次通知"],
                    "下次通知": ["下次通知"],
                    "數量": ["數量", "Qty"],
                }
                rename_map = {}
                for std, candidates in wanted_map.items():
                    for c in candidates:
                        if c in df_detail.columns:
                            rename_map[c] = std
                            break
                if rename_map:
                    df_detail = df_detail.rename(columns=rename_map)

                # 標準化日期欄位（若存在）
                date_cols = [c for c in ["上次更換", "上次通知", "下次通知"] if c in df_detail.columns]
                if date_cols:
                    df_detail = self.convert_datetime_optimized(df_detail, date_cols)
                    # 轉成 年/月/日（不含時分秒）
                    for _col in date_cols:
                        try:
                            s = pd.to_datetime(df_detail[_col], errors="coerce")
                            out = s.dt.strftime("%Y/%m/%d")
                            # 將 NaT 轉為空字串，避免出現 'NaT'
                            df_detail[_col] = out.where(s.notna(), "")
                        except Exception:
                            pass

                # 僅保留系統使用欄位
                keep_cols = [
                    "服務登記號",
                    "物料代碼",
                    "產品名稱",
                    "標準售價",
                    "更換期限_月",
                    "上次更換",
                    "上次通知",
                    "下次通知",
                    "數量",
                ]
                if "服務登記號" not in df_detail.columns:
                    self.log_time("❌ 無法找到明細必要欄位『服務登記號』，已略過明細匯入。")
                else:
                    existing = [c for c in keep_cols if c in df_detail.columns]
                    df_detail = df_detail[existing].copy()

                    # 重新建立 service_card_detail（固定結構，包含自增 id）
                    cursor.execute("DROP TABLE IF EXISTS service_card_detail")
                    cursor.execute(
                        """
                        CREATE TABLE service_card_detail (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          "服務登記號" TEXT,
                          "物料代碼" TEXT,
                          "產品名稱" TEXT,
                          "標準售價" TEXT,
                          "更換期限_月" TEXT,
                          "上次更換" TEXT,
                          "上次通知" TEXT,
                          "下次通知" TEXT,
                          "數量" TEXT
                        )
                        """
                    )

                    # 欄位補齊順序
                    ordered_cols = [
                        "服務登記號",
                        "物料代碼",
                        "產品名稱",
                        "標準售價",
                        "更換期限_月",
                        "上次更換",
                        "上次通知",
                        "下次通知",
                        "數量",
                    ]
                    for col in ordered_cols:
                        if col not in df_detail.columns:
                            df_detail[col] = ""
                    df_detail = df_detail[ordered_cols]

                    # 批次寫入（避免 pandas 重建表結構）
                    insert_sql = (
                        "INSERT INTO service_card_detail(\"服務登記號\",\"物料代碼\",\"產品名稱\",\"標準售價\",\"更換期限_月\",\"上次更換\",\"上次通知\",\"下次通知\",\"數量\") "
                        "VALUES (?,?,?,?,?,?,?,?,?)"
                    )
                    data = [tuple("" if x is None else x for x in row) for row in df_detail.itertuples(index=False, name=None)]
                    chunk_size = 1000
                    for i in range(0, len(data), chunk_size):
                        cursor.executemany(insert_sql, data[i : i + chunk_size])

                    # 索引
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_svc_detail_card_no ON service_card_detail(\"服務登記號\")")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_svc_detail_product ON service_card_detail(\"產品名稱\")")

                    self.log_time(f"✅ 服務登記卡-明細 已匯入筆數: {len(df_detail)}")
            except Exception as de:
                self.log_time(f"❌ 匯入服務登記卡明細時發生錯誤：{str(de)}")

            conn.commit()
            conn.close()

            self.log_time(f"✅ 服務登記卡資料轉換完成：{len(df)} 筆")

        except Exception as e:
            self.log_time(f"❌ 轉換服務登記卡資料時發生錯誤：{str(e)}")

    def convert_crm_notes(self):
        """轉換 CRM記事.xlsx 為 SQLite"""
        self.log_time("?? 開始轉換 CRM記事.xlsx ...")

        try:
            if not os.path.exists(self.crm_source_path):
                self.log_time(f"?? 找不到檔案：{self.crm_source_path}")
                return False

            os.makedirs(self.crm_dir, exist_ok=True)

            df = pd.read_excel(self.crm_source_path)
            self.log_time(f"?? CRM記事原始列數：{len(df)} 筆")

            normalized_columns = []
            for idx, col in enumerate(df.columns):
                if pd.isna(col) or str(col).strip() == "":
                    normalized_columns.append(f"欄位_{idx + 1}")
                else:
                    normalized_columns.append(str(col).strip())
            df.columns = normalized_columns

            before_drop = len(df)
            df = df.dropna(how="all")
            self.log_time(f"?? 清理空白列後：{before_drop} 筆 -> {len(df)} 筆")

            conn = sqlite3.connect(self.crm_db_path)
            df.to_sql('crm_notes', conn, if_exists='replace', index=False)
            conn.close()

            self.log_time(f"? CRM記事寫入 SQLite 完成，共 {len(df)} 筆")
            return True

        except Exception as e:
            self.log_time(f"? 轉換 CRM記事.xlsx 發生錯誤：{str(e)}")
            return False
    def convert_inventory_inquiry_data(self):
        """轉換正航庫存資料"""
        self.log_time("🔄 開始轉換正航庫存資料...")

        try:
            # 讀取Excel檔案（從統一資料來源目錄）
            excel_path = os.path.join(self.data_source_dir, "正航庫存資料.xlsx")
            if not os.path.exists(excel_path):
                self.log_time(f"❌ 找不到檔案：{excel_path}")
                return False

            df = pd.read_excel(excel_path)
            self.log_time(f"📊 讀取到 {len(df)} 筆庫存資料")

            # 建立資料庫連接
            conn = sqlite3.connect(self.inventory_data_db_path)
            cursor = conn.cursor()

            # 刪除舊表格並重新建立
            cursor.execute("DROP TABLE IF EXISTS inventory_data")

            # 建立庫存資料表（實際插入時會由 DataFrame 透過 to_sql 重建，此結構僅供參考）
            cursor.execute(
                """
                CREATE TABLE inventory_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_name TEXT,
                    warehouse_name TEXT,
                    warehouse_partner_name TEXT,
                    inventory_type TEXT,
                    product_code TEXT,
                    specification TEXT,
                    unit TEXT,
                    quantity REAL,
                    unit_price REAL,
                    total_amount REAL,
                    last_update_date TEXT,
                    remark TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # 處理資料欄位對應（根據實際Excel結構調整）
            # 先檢查Excel檔案的欄位結構
            self.log_time(f"📋 Excel欄位：{list(df.columns)}")

            # 如果Excel檔案有資料，進行轉換
            if len(df) > 0:
                # 建立欄位對應字典
                column_mapping = {}
                for col in df.columns:
                    if "產品名稱" in col:
                        column_mapping[col] = "product_name"
                    elif "倉庫名稱" in col:
                        column_mapping[col] = "warehouse_name"
                    elif "倉庫往來對象名稱" in col or "往來對象" in col:
                        column_mapping[col] = "warehouse_partner_name"
                    elif "存貨屬性" in col or "庫存屬性" in col or col == "屬性":
                        # 直接沿用 Excel 的存貨屬性，不做邏輯換算
                        column_mapping[col] = "inventory_type"
                    elif "產品代碼" in col or "代碼" in col:
                        column_mapping[col] = "product_code"
                    elif "規格" in col:
                        column_mapping[col] = "specification"
                    elif "單位" in col:
                        column_mapping[col] = "unit"
                    elif "數量" in col:
                        column_mapping[col] = "quantity"
                    elif "單價" in col:
                        column_mapping[col] = "unit_price"
                    elif "總金額" in col or "金額" in col:
                        column_mapping[col] = "total_amount"
                    elif "更新日期" in col or "日期" in col:
                        column_mapping[col] = "last_update_date"
                    elif "備註" in col:
                        column_mapping[col] = "remark"

                # 重新命名欄位
                df_mapped = df.rename(columns=column_mapping)

                # 確保必要欄位存在，如果不存在則填入空值
                required_columns = [
                    "product_name",
                    "warehouse_name",
                    "warehouse_partner_name",
                    "inventory_type",
                    "product_code",
                    "specification",
                    "unit",
                    "quantity",
                    "unit_price",
                    "total_amount",
                    "last_update_date",
                    "remark",
                ]

                for col in required_columns:
                    if col not in df_mapped.columns:
                        df_mapped[col] = ""

                # 只保留需要的欄位
                df_final = df_mapped[required_columns]

                df_final = self.normalize_remark_columns(df_final, ["remark"])
                # 存貨屬性欄位標準化 + 顯示用文字對應
                # 自有庫存 -> 世磊、借入庫存 -> 寄倉、借出庫存 -> 借出
                if "inventory_type" in df_final.columns:
                    inv = df_final["inventory_type"].fillna("").astype(str).str.strip()
                    mapping = {
                        "自有庫存": "世磊",
                        "借入庫存": "寄倉",
                        "借出庫存": "借出",
                        # 也支援可能的簡寫
                        "自有": "世磊",
                        "借入": "寄倉",
                        "借出": "借出",
                    }
                    df_final["inventory_type"] = inv.replace(mapping)

                # 使用分批插入方法
                self.insert_data_in_chunks(
                    df_final, conn, "inventory_data", chunk_size=1000
                )

                # 建立索引（針對查詢需求）
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_product_name ON inventory_data(product_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warehouse_name ON inventory_data(warehouse_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warehouse_partner_name ON inventory_data(warehouse_partner_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_inventory_type ON inventory_data(inventory_type)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_product_code ON inventory_data(product_code)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_quantity ON inventory_data(quantity)"
                )

                # 建立複合索引（提升多欄位查詢效能）
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_product_warehouse ON inventory_data(product_name, warehouse_name)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_warehouse_partner ON inventory_data(warehouse_name, warehouse_partner_name)"
                )

            # 提交並關閉
            conn.commit()

            # 驗證資料
            cursor.execute("SELECT COUNT(*) FROM inventory_data")
            count = cursor.fetchone()[0]
            conn.close()

            self.log_time(f"✅ 正航庫存資料轉換完成：{count} 筆")
            return True

        except Exception as e:
            self.log_time(f"❌ 轉換正航庫存資料時發生錯誤：{str(e)}")
            return False

    def convert_procuretrack_data(self):
        """匯入採購到貨進度追蹤資料，採更新或新增(UPSERT)邏輯，並保留手動維護欄位。"""
        self.log_time("🔄 開始匯入採購到貨進度追蹤資料...")

        try:
            excel_path = os.path.join(self.data_source_dir, "採購狀況明細表.xlsx")
            if not os.path.exists(excel_path):
                self.log_time(f"⚠️ 找不到檔案：{excel_path}")
                return False

            df = pd.read_excel(excel_path)
            if "序號" not in df.columns:
                self.log_time("⚠️ Excel 缺少「序號」欄位，無法執行新的匯入邏輯，停止匯入。")
                return False
            
            self.log_time(f"📊 讀取 Excel 資料總筆數：{len(df)}")

            os.makedirs(self.procuretrack_db_dir, exist_ok=True)
            conn = sqlite3.connect(self.procuretrack_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 檢查並更新資料庫結構
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS procure_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    po_number TEXT,
                    item_serial_number TEXT,
                    product_code TEXT,
                    product_name TEXT,
                    quantity INTEGER,
                    warehouse_qty INTEGER,
                    delivery_date TEXT,
                    dispatch_date TEXT,
                    warehouse TEXT,
                    arrival_date TEXT,
                    ship_info TEXT,
                    status TEXT,
                    goods_status TEXT,
                    UNIQUE(po_number, item_serial_number)
                )
                """
            )
            conn.commit()

            try:
                cols = {row[1] for row in cursor.execute("PRAGMA table_info(procure_items)").fetchall()}
                if "item_serial_number" not in cols:
                    cursor.execute("ALTER TABLE procure_items ADD COLUMN item_serial_number TEXT")
                if "goods_status" not in cols:
                    cursor.execute("ALTER TABLE procure_items ADD COLUMN goods_status TEXT")
                for col_to_check in ["product_code", "warehouse_qty", "warehouse", "dispatch_date"]:
                     if col_to_check not in cols:
                        cursor.execute(f"ALTER TABLE procure_items ADD COLUMN {col_to_check} TEXT")
                conn.commit()
            except Exception as e:
                self.log_time(f"⚠️ 檢查/新增欄位時發生錯誤 (可忽略): {e}")

            updated_count = 0
            inserted_count = 0
            skipped_count = 0
            errors_count = 0

            def _normalize_delivery(val):
                if pd.isna(val): return ""
                try: s = str(val).strip()
                except Exception: return ""
                if not s: return ""
                import re
                s = re.sub(r"\([^)]*\)", "", s).strip()
                try:
                    dt = pd.to_datetime(s, errors="coerce")
                    return dt.date().isoformat() if pd.notna(dt) else s
                except Exception: return s

            for index, row in df.iterrows():
                try:
                    po_number = str(row.get("單據編號") or "").strip()
                    item_serial_number = str(row.get("序號") or "").strip()

                    self.log_time(f"--- [處理中] Excel 第 {index + 2} 行: 單號={po_number}, 序號={item_serial_number} ---")

                    if not po_number or not item_serial_number:
                        self.log_time("   -> [跳過] 單號或序號為空。")
                        skipped_count += 1
                        continue
                    
                    product_code = str(row.get("產品代碼") or "").strip()
                    product_name = str(row.get("產品名稱") or "").strip()
                    quantity_raw = row.get("交易數量")
                    warehouse_qty_raw = row.get("倉庫確認數量")
                    warehouse_name = str(row.get("倉庫") or "").strip()
                    delivery_date = _normalize_delivery(row.get("交貨日期"))
                    status_val = str(row.get("狀態") or "").strip()
                    self.log_time(f"   [讀取值] Excel狀態: '{status_val}', 數量: {quantity_raw}")

                    try: quantity = int(quantity_raw) if pd.notna(quantity_raw) else 0
                    except (ValueError, TypeError): quantity = 0
                    try: warehouse_qty = int(warehouse_qty_raw) if pd.notna(warehouse_qty_raw) else 0
                    except (ValueError, TypeError): warehouse_qty = 0

                    cursor.execute(
                        "SELECT id FROM procure_items WHERE po_number = ? AND item_serial_number = ?",
                        (po_number, item_serial_number),
                    )
                    existing_record = cursor.fetchone()

                    if existing_record:
                        self.log_time(f"   -> [判斷] 找不到對應紀錄 (ID: {existing_record['id']})，準備執行【更新】。")
                        self.log_time(f"      [更新值] status='{status_val}'")
                        cursor.execute(
                            """
                            UPDATE procure_items
                            SET
                                product_code = ?, product_name = ?, quantity = ?,
                                warehouse_qty = ?, delivery_date = ?, warehouse = ?,
                                status = ?
                            WHERE
                                id = ?
                            """,
                            (
                                product_code, product_name, quantity,
                                warehouse_qty, delivery_date, warehouse_name,
                                status_val,
                                existing_record["id"],
                            ),
                        )
                        updated_count += 1
                    else:
                        self.log_time("   -> [判斷] 找不到對應紀錄，準備執行【新增】。")
                        cursor.execute(
                            """
                            INSERT INTO procure_items (
                                po_number, item_serial_number, product_code, product_name, quantity, warehouse_qty,
                                delivery_date, warehouse, status,
                                dispatch_date, arrival_date, ship_info, goods_status
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                po_number, item_serial_number, product_code, product_name, quantity, warehouse_qty,
                                delivery_date, warehouse_name, status_val,
                                "", "", "", ""
                            ),
                        )
                        inserted_count += 1

                except Exception as e:
                    errors_count += 1
                    self.log_time(f"   -> XXX [錯誤] 處理此行時發生嚴重錯誤：{e}")

            conn.commit()
            conn.close()

            self.log_time("-" * 40)
            self.log_time(
                f"📊 採購到貨進度匯入完成：新增 {inserted_count} 筆，更新 {updated_count} 筆，跳過 {skipped_count} 筆，錯誤 {errors_count} 筆"
            )
            return True

        except Exception as e:
            self.log_time(f"❌ 匯入採購到貨進度資料發生最外層嚴重錯誤：{str(e)}")
            return False

    def convert_sales_ai_data(self):
        """轉換 AI 專用銷售資料 (合併發貨狀況分析表與昇峰銷售資料)"""
        self.log_time("🔄 開始轉換 AI 專用銷售資料 (sales_ai.db)...")

        try:
            # 讀取銷售資料檔案
            df_shipment_path = os.path.join(self.data_source_dir, "發貨狀況分析表.xlsx")
            df_shengfeng_path = os.path.join(self.data_source_dir, "昇峰銷售資料.xlsx")

            df_list = []

            if os.path.exists(df_shipment_path):
                df_shipment = pd.read_excel(df_shipment_path)
                df_list.append(df_shipment)
                self.log_time(f"📊 發貨狀況分析表：{len(df_shipment)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shipment_path}")

            if os.path.exists(df_shengfeng_path):
                df_shengfeng = pd.read_excel(df_shengfeng_path)
                df_list.append(df_shengfeng)
                self.log_time(f"📊 昇峰銷售資料：{len(df_shengfeng)} 筆")
            else:
                self.log_time(f"⚠️ 找不到檔案：{df_shengfeng_path}")

            if not df_list:
                self.log_time("❌ 沒有找到任何銷售資料檔案")
                return

            # 轉換日期時間
            # 這裡需要處理所有可能的日期欄位，確保 AI 能正確查詢
            date_columns = ["發貨日期", "單據日期", "建立日期", "過帳日期"]
            for i, df in enumerate(df_list):
                 # 使用既有的日期轉換邏輯
                if i == 0:  # 發貨狀況分析表
                     df_list[i] = self.convert_datetime_optimized(df, date_columns)
                else: # 昇峰銷售資料 (通常有特殊日期格式)
                     df_list[i] = self.convert_shengfeng_dates(df, date_columns)

            # 合併資料 (concat 會自動處理欄位不一致，缺少的欄位補 NaN)
            df_combined = pd.concat(df_list, ignore_index=True)
            self.log_time(f"📊 合併後總筆數：{len(df_combined)}")

            # --- 資料清理開始 ---
            
            # 1. 欄位名稱去空白
            df_combined.columns = [str(col).strip() for col in df_combined.columns]

            # 2. 處理 '客戶' 與 '客戶名稱'
            # 如果有 '客戶' 欄位，將其內容填入 '客戶名稱' (如果 '客戶名稱' 為空或不存在)
            if '客戶' in df_combined.columns:
                if '客戶名稱' not in df_combined.columns:
                    df_combined.rename(columns={'客戶': '客戶名稱'}, inplace=True)
                else:
                    # 兩者並存時，優先保留 '客戶名稱'，若為空則用 '客戶' 填補
                    df_combined['客戶名稱'] = df_combined['客戶名稱'].fillna(df_combined['客戶'])
                    # 移除多餘的 '客戶' 欄位
                    df_combined.drop(columns=['客戶'], inplace=True)
            
            # 3. 移除 Unnamed 欄位
            cols_to_drop = [col for col in df_combined.columns if 'Unnamed' in col]
            if cols_to_drop:
                df_combined.drop(columns=cols_to_drop, inplace=True)
                self.log_time(f"🧹 已移除無效欄位: {cols_to_drop}")

            # 4. 數值欄位清理 (移除逗號並轉為數字)
            numeric_cols = ['交易數量', '倉庫確認數量', '交易價']
            for col in numeric_cols:
                if col in df_combined.columns:
                    # 先轉為字串，移除逗號，再轉為數值
                    df_combined[col] = df_combined[col].astype(str).str.replace(',', '', regex=False)
                    df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').fillna(0)
                    self.log_time(f"🔢 已清理數值欄位: {col}")

            # --- 資料清理結束 ---

            # 移除備註中的電話連字號 (選用，為了保持與 sales.db 一致)
            # df_combined = self.normalize_remark_columns(df_combined, ["備註"])

            # 建立資料庫連接
            conn = sqlite3.connect(self.sales_ai_db_path)
            cursor = conn.cursor()

            # 使用分批插入方法，將所有資料寫入單一資料表 'sales_data'
            # 刪除舊表
            cursor.execute("DROP TABLE IF EXISTS sales_data")
            
            # 因為欄位可能很多且不固定，使用 to_sql 直接建立
            self.insert_data_in_chunks(df_combined, conn, "sales_data", chunk_size=1000)

            # 建立索引 (針對可能的常用查詢欄位)
            # 先檢查欄位是否存在
            potential_indexes = [
                ("單據編號", "idx_sales_ai_doc_no"),
                ("發貨日期", "idx_sales_ai_date"),
                ("客戶名稱", "idx_sales_ai_customer"),
                ("產品名稱", "idx_sales_ai_product"),
                ("業務人員名稱", "idx_sales_ai_salesperson"),
                ("客戶代碼", "idx_sales_ai_customer_code"),
            ]
            
            valid_indexes = []
            for col, idx_name in potential_indexes:
                if col in df_combined.columns:
                    valid_indexes.append((col, idx_name))
            
            self.create_indexes_batch(cursor, "sales_data", valid_indexes)

            conn.commit()
            conn.close()

            self.log_time(f"✅ AI 銷售資料轉換完成：{len(df_combined)} 筆 -> {self.sales_ai_db_path}")

        except Exception as e:
            self.log_time(f"❌ 轉換 AI 銷售資料時發生錯誤：{str(e)}")


    def verify_data_integrity(self):
        """驗證資料完整性"""
        self.log_time("?? 驗證資料完整性...")

        # 銷售資訊查詢系統資料庫
        sales_databases = [
            (self.sales_db_path, "sales_main", "銷售主檔"),
            (self.sales_db_path, "sales_detail", "銷售明細"),
            (self.repair_db_path, "repair_data", "維修資料"),
            (self.custody_db_path, "custody_main", "寄倉主檔"),
            (self.custody_db_path, "custody_detail", "寄倉明細"),
            (self.customer_db_path, "customer_basic", "客戶基本資料"),
            (self.service_card_db_path, "service_card_data", "服務登記卡資料"),
            (self.crm_db_path, "crm_notes", "CRM記事"),
            (self.sales_ai_db_path, "sales_data", "AI銷售資料"), # Add AI sales data
        ]

        # 庫存查詢系統資料庫
        inventory_inquiry_databases = [
            (self.inventory_data_db_path, "inventory_data", "正航庫存資料")
        ]

        # 採購到貨進度追蹤系統
        procuretrack_databases = [
            (self.procuretrack_db_path, "procure_data", "採購到貨資料") # Changed table name to procure_data
        ]

        self.log_time("?? 銷售資訊查詢系統：")
        for db_path, table_name, description in sales_databases:
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    conn.close()
                    self.log_time(f"   ? {description}：{count:,} 筆")
                except Exception as e:
                    self.log_time(f"   ? {description} 驗證失敗：{e}")
            else:
                self.log_time(f"   ?? 找不到資料庫：{db_path}")

        self.log_time("?? 庫存查詢系統：")
        for db_path, table_name, description in inventory_inquiry_databases:
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    conn.close()
                    self.log_time(f"   ? {description}：{count:,} 筆")
                except Exception as e:
                    self.log_time(f"   ? {description} 驗證失敗：{e}")
            else:
                self.log_time(f"   ?? 找不到資料庫：{db_path}")

        self.log_time("?? 採購到貨進度追蹤系統：")
        for db_path, table_name, description in procuretrack_databases:
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    conn.close()
                    self.log_time(f"   ? {description}：{count:,} 筆")
                except Exception as e:
                    self.log_time(f"   ? {description} 驗證失敗：{e}")
            else:
                self.log_time(f"   ?? 找不到資料庫：{db_path}")

    def run_bi_full_conversion(self):
        """執行BI系統完整的資料轉換流程（包含所有分支系統）"""
        start_time = time.time()
        self.log_time("🚀 開始BI系統整合資料轉換流程...")

        try:
            # 1. 轉換銷售資訊查詢系統資料
            self.log_time("📊 === 銷售資訊查詢系統 ===")
            self.convert_customer_data()
            self.convert_sales_data()
            self.convert_repair_data()
            self.convert_custody_data()
            self.convert_crm_notes()
            self.convert_service_card_data()
            self.convert_sales_ai_data() # Add AI sales data conversion

            # 2. 採購到貨進度追蹤系統
            self.log_time("🛥 === 採購到貨進度追蹤系統 ===")
            self.convert_procuretrack_data()

            # 3. 轉換庫存查詢系統資料
            self.log_time("📋 === 庫存查詢系統 ===")
            self.convert_inventory_inquiry_data()

            # 4. 驗證資料完整性
            self.verify_data_integrity()

            # 計算總耗時
            end_time = time.time()
            total_time = end_time - start_time
            minutes = int(total_time // 60)
            seconds = int(total_time % 60)

            self.log_time(f"🎉 BI系統資料轉換完成！總耗時：{minutes}分{seconds}秒")

        except Exception as e:
            self.log_time(f"❌ BI系統資料轉換過程發生錯誤：{str(e)}")
            raise

    def run_bi_daily_update(self):
        """BI系統日常更新模式"""
        start_time = time.time()
        self.log_time("📅 開始BI系統日常更新模式...")

        try:
            # 1. 更新銷售資訊查詢系統
            self.log_time("📊 === 更新銷售資訊查詢系統 ===")
            self.convert_customer_data()
            self.convert_sales_data()
            self.convert_repair_data()
            self.convert_custody_data()
            self.convert_service_card_data()
            self.convert_sales_ai_data() # Add AI sales data conversion

            # 2. 更新採購到貨進度追蹤資料
            self.log_time("🛥 === 更新採購到貨進度追蹤資料 ===")
            self.convert_procuretrack_data()

            # 3. 更新庫存查詢系統資料
            self.log_time("📋 === 更新庫存查詢系統資料 ===")
            self.convert_inventory_inquiry_data()

            # 4. 驗證更新結果
            self.verify_data_integrity()

            # 計算總耗時
            end_time = time.time()
            total_time = end_time - start_time
            minutes = int(total_time // 60)
            seconds = int(total_time % 60)

            self.log_time(f"📅 BI系統日常更新完成！耗時：{minutes}分{seconds}秒")

        except Exception as e:
            self.log_time(f"❌ BI系統日常更新過程發生錯誤：{str(e)}")
            raise

    def run_sales_only_conversion(self):
        """僅轉換銷售資訊查詢系統資料"""
        start_time = time.time()
        self.log_time("📊 開始銷售資訊查詢系統資料轉換...")

        try:
            # 轉換銷售資訊查詢系統資料
            self.convert_customer_data()
            self.convert_sales_data()
            self.convert_repair_data()
            self.convert_crm_notes()
            self.convert_custody_data()
            self.convert_service_card_data()
            self.convert_sales_ai_data() # Add AI sales data conversion

            # 驗證結果
            self.log_time("📊 銷售資訊查詢系統：")
            sales_databases = [
                (self.sales_db_path, "sales_main", "銷售主檔"),
                (self.sales_db_path, "sales_detail", "銷售明細"),
                (self.repair_db_path, "repair_data", "維修資料"),
                (self.custody_db_path, "custody_main", "寄倉主檔"),
                (self.custody_db_path, "custody_detail", "寄倉明細"),
                (self.customer_db_path, "customer_basic", "客戶基本資料"),
                (self.service_card_db_path, "service_card_data", "服務登記卡資料"),
                (self.crm_db_path, "crm_notes", "CRM記事"),
                (self.sales_ai_db_path, "sales_data", "AI銷售資料"), # Add AI sales data
            ]

            for db_path, table_name, description in sales_databases:
                if os.path.exists(db_path):
                    try:
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        conn.close()
                        self.log_time(f"   ✅ {description}：{count:,} 筆")
                    except Exception as e:
                        self.log_time(f"   ❌ {description} 驗證失敗：{e}")
                else:
                    self.log_time(f"   ⚠️ 找不到資料庫：{db_path}")

            # 計算總耗時
            end_time = time.time()
            total_time = end_time - start_time
            minutes = int(total_time // 60)
            seconds = int(total_time % 60)
            self.log_time(f"📊 銷售資訊查詢系統轉換完成！耗時：{minutes}分{seconds}秒")

        except Exception as e:
            self.log_time(f"❌ 銷售資訊查詢系統轉換過程發生錯誤：{str(e)}")
            raise



def main():
    """主程式入口"""
    import sys

    # 檢查命令列參數
    if len(sys.argv) > 1 and sys.argv[1] == "--daily":
        # 直接執行日常更新，不需要用戶輸入
        print("=" * 60)
        print("🔧 BI系統整合資料匯入工具 - 日常更新模式")
        print("=" * 60)

        try:
            converter = BIIntegratedDataConverter()
            converter.run_bi_daily_update()
            print("\n" + "=" * 60)
            print("✨ BI系統日常更新完成！")
        except Exception as e:
            print(f"\n❌ 發生錯誤：{str(e)}")
        return

    # 互動模式
    print("=" * 60)
    print("🔧 BI系統整合資料匯入工具")
    print("1. BI整合一次轉換（含銷售/採購/庫存查詢/CRM）——建議初次使用")
    print("2. BI每日更新（適用每日例行更新）")
    print("3. 僅轉換銷售查詢系統（客戶/銷售/維修/寄倉/服務卡/CRM記事）")
    print("4. 僅轉換庫存查詢系統（正航庫存資料）")
    print("5. 轉換客戶資料")
    print("6. 轉換銷售資料")
    print("7. 轉換維修資料")
    print("8. 轉換寄倉資料")
    print("9. 轉換服務卡資料")
    print("10. 匯入 CRM 記事資料 (CRM記事.db)")
    print("11. 匯入 採購到貨進度資料 (procure.db)")
    print("12. 檢查資料完整性")
    print("13. 匯入 AI 專用銷售資料 (sales_ai.db) [含發貨狀況與昇峰銷售]")
    print("Q. 離開")
    print("=" * 60)
    print("📌 使用建議")
    print("   選項1：建議首次使用或資料異動後完整重建")
    print("   選項2：每日例行更新（避免重新匯入所有資料）")
    print("   選項3-4：針對特定系統快速更新")
    print("   選項5-11：針對單一主檔重新匯入")
    print("   選項12：檢查各系統資料筆數是否正常")
    print("   選項13：為 AI 應用準備的銷售資料，可獨立更新")
    print("=" * 60)
    print("📁 資料來源目錄：D:\\WEB\\BI\\資料來源")
    print("=" * 60)

    try:
        choice = input("請選擇操作模式 (1-12): ").strip()

        converter = BIIntegratedDataConverter()

        if choice == "1":
            converter.run_bi_full_conversion()
        elif choice == "2":
            converter.run_bi_daily_update()
        elif choice == "3":
            converter.run_sales_only_conversion()
        elif choice == "4":
            converter.convert_inventory_inquiry_data()
        elif choice == "5":
            converter.convert_customer_data()
        elif choice == "6":
            converter.convert_sales_data()
        elif choice == "7":
            converter.convert_repair_data()
        elif choice == "8":
            converter.convert_custody_data()
        elif choice == "9":
            converter.convert_service_card_data()
        elif choice == "10":
            converter.convert_crm_notes()
        elif choice == "11":
            converter.convert_procuretrack_data()
        elif choice == "12":
            converter.verify_data_integrity()
        elif choice == "13":
            converter.convert_sales_ai_data()
            return

        print("\n" + "=" * 60)
        print("✨ 操作完成！")

    except KeyboardInterrupt:
        print("\n❌ 操作被使用者中斷")
    except Exception as e:
        print(f"\n❌ 發生錯誤：{str(e)}")
    finally:
        try:
            input("\n按 Enter 鍵結束...")
        except EOFError:
            # 處理批次檔執行時的 EOF 錯誤
            pass



if __name__ == "__main__":
    main()
