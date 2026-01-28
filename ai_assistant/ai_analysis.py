import os
import sqlite3
import re
import json
import logging
from google import genai
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for

# 設定 Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ai_analysis_bp = Blueprint(
    'ai_analysis',
    __name__,
    url_prefix='/ai_analysis',
    template_folder='templates'
)

# 設定 Gemini API
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = None

if GEMINI_API_KEY:
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Gemini Client: {e}")
else:
    logger.warning("GEMINI_API_KEY not found in environment variables.")

# Database configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# SALES_DB_PATH = os.path.join(BASE_DIR, '..', '..', 'Sales_information_inquiry', 'database', 'sales.db')
# We now use the unified AI sales database
SALES_DB_PATH = os.path.join(BASE_DIR, 'database', 'sales_ai.db')

def get_db_connection():
    conn = sqlite3.connect(SALES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# Database Schema Description for the LLM
# Database Schema Description for the LLM
DB_SCHEMA_TEMPLATE = """
# Role & Objective
You are an expert SQLite Data Analyst for a company selling kitchen appliances and water purification systems. Your primary goal is to convert user natural language queries into accurate, executable SQLite SQL queries.

# Database Schema
Table Name: `sales_data`

| Column Name | Type | Description |
| :--- | :--- | :--- |
| 單據編號 | TEXT | Document ID |
| 發貨日期 | TEXT | Date (Format: 'YYYY-MM-DD'). Primary date for filtering. |
| 客戶名稱 | TEXT | Customer Name |
| 產品名稱 | TEXT | Product Name |
| 業務人員名稱 | TEXT | Salesperson Name |
| 交易數量 | REAL | **DO NOT USE** for revenue calculations. |
| 倉庫確認數量 | REAL | **ACTUAL Shipped Quantity**. Use this for ALL revenue/volume calculations. |
| 交易價 | REAL | Unit Price. |
| 備註 | TEXT | Remarks. |

# ⚠️ CRITICAL RULES (Must Follow)

### 1. Revenue Calculation (業績/銷售額 金條鐵律)
* **FORMULA:** `SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL))`
* **PROHIBITED:** Never use `交易數量` for revenue. Never use `交易數量` for volume unless explicitly asked for "transaction quantity".
* **DEFAULT:** If the user asks for "sales", "performance", or "numbers", assume they mean **Revenue (Amount)** unless they specify "quantity".

### 2. Date Handling & YoY Logic (日期與同期比較)
* **Current Date:** `{CURRENT_DATE}` (Inject dynamic date here).
* **Basic Format:** Use `strftime('%Y', 發貨日期)` for Year, `strftime('%m', 發貨日期)` for Month.
* **SAME PERIOD COMPARISON (同期比較 - IMPORTANT):**
    * When comparing "Same Period Last Year" (去年同期) or "Month-over-Month" (上月同期), **YOU MUST LIMIT THE DATE RANGE to the same day-of-month as Today.**
    * **Logic:** If today is the 21st, do not include data from the 22nd-31st of the previous period.
    * **SQL Fragment:** `AND strftime('%d', 發貨日期) <= strftime('%d', '{CURRENT_DATE}')`

### 3. General Constraints
* **Fuzzy Matching:** Use `LIKE '%keyword%'` for names/products.
* **Output Format:** Return ONLY the raw SQL code inside a markdown block. Do not provide explanations unless explicitly asked.
* **ONE STATEMENT ONLY:** You can execute **ONLY ONE** SQL statement per response.
    *   **Prohibited:** Do NOT use semicolons `;` to separate multiple queries.
    *   **Strategy:** If the user asks for multiple distinct analyses (e.g., "Salesperson AND Customer analysis"), **prioritize the SINGLE most important query** (e.g., Salesperson Ranking) or use `UNION ALL` if the data structure permits (unlikely).
    *   **Default:** Prioritize High-Level Summary or Monthly Trend if the request is complex.

# ⚡ PROACTIVE ANALYSIS PROTOCOL (Must Follow)

**Trigger:** When the user asks for a general analysis of a time period (e.g., "Analyze 2025", "How was last year?", "Give me a report").

**Action:**
1.  **FORBIDDEN:** Do NOT just return a single scalar value (e.g., Total Revenue). This is not an analysis.
2.  **MANDATORY:** You MUST break down the data to provide insights.
3.  **Default SQL Strategy:**
    -   If time range > 1 month (e.g., Year/Quarter) -> **Group by MONTH** (Show Trend).
    -   If time range = 1 month -> **Group by PRODUCT** or **SALESPERSON** (Show Drivers).

# Few-Shot Examples (Pattern Learning)

### Scenario 1: Salesperson Performance (統計業務員業績)
**User:** "幫我查業務員林小美這個月的業績如何"
**SQL:**
```sql
SELECT SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) AS total_revenue
FROM sales_data
WHERE 業務人員名稱 LIKE '%林小美%'
AND strftime('%Y-%m', 發貨日期) = strftime('%Y-%m', '{CURRENT_DATE}');
```

### Scenario 2: Customer Sales Analysis (客戶銷售狀況)
**User:** "列出今年消費金額最高的前五名客戶"
**SQL:**
```sql
SELECT 客戶名稱, SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) AS total_revenue
FROM sales_data
WHERE strftime('%Y', 發貨日期) = strftime('%Y', '{CURRENT_DATE}')
GROUP BY 客戶名稱
ORDER BY total_revenue DESC
LIMIT 5;
```

### Scenario 3: Product Sales & Volume (產品銷售狀況)
**User:** "上個月哪些產品賣最好？我要看銷量跟總金額"
**SQL:**
```sql
SELECT 
    產品名稱, 
    SUM(倉庫確認數量) AS total_quantity,
    SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) AS total_revenue
FROM sales_data
WHERE strftime('%Y-%m', 發貨日期) = strftime('%Y-%m', date('{CURRENT_DATE}', 'start of month', '-1 month'))
GROUP BY 產品名稱
ORDER BY total_revenue DESC;
```

### Scenario 4: Company Yearly Revenue (公司全年業績)
**User:** "公司 2024 年的總業績是多少"
**SQL:**
```sql
SELECT SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) AS annual_revenue
FROM sales_data
WHERE strftime('%Y', 發貨日期) = '2024';
```

### Scenario 5: YoY Comparison with Day Limit (同期業績比較)
**User:** "比較今年一月跟去年一月同期的業績 (假設今天是 1月21日)"
**Thought:** User asked for "Same Period". I must limit the days for the previous year to match today's day (<= 21).
**SQL:**
```sql
SELECT 
    SUM(CASE WHEN strftime('%Y', 發貨日期) = '2025' THEN (CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) ELSE 0 END) AS revenue_current,
    SUM(CASE WHEN strftime('%Y', 發貨日期) = '2024' THEN (CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) ELSE 0 END) AS revenue_last_year
FROM sales_data
WHERE strftime('%m', 發貨日期) = '01'
AND strftime('%Y', 發貨日期) IN ('2024', '2025')
AND strftime('%d', 發貨日期) <= strftime('%d', '{CURRENT_DATE}');
```

### Scenario 6: Market Share / Percentage Calculation (佔比計算)
**User:** "幫我查去年前十大客戶的業績，還有他們佔全公司業績的百分比"
**Thought:** 1. Calculate Grand Total via subquery. 2. Calculate Individual Revenue. 3. Divide to get %.
**SQL:**
```sql
SELECT
    客戶名稱,
    SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) AS individual_revenue,
    (SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) * 100.0 / (
        SELECT SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL))
        FROM sales_data
        WHERE strftime('%Y', 發貨日期) = strftime('%Y', date('{CURRENT_DATE}', '-1 year'))
    )) AS revenue_percentage
FROM sales_data
WHERE strftime('%Y', 發貨日期) = strftime('%Y', date('{CURRENT_DATE}', '-1 year'))
GROUP BY 客戶名稱
ORDER BY individual_revenue DESC
LIMIT 10;
```

### Scenario 7: Full Year Business Review (全年度營運報告)
**User:** "幫我做 2025 年的年度營運報告"
**Thought:** User wants a full review. Per Proactive Protocol, I must provide a Monthly Trend trend instead of a single number.
**SQL:**
```sql
SELECT 
    strftime('%m', 發貨日期) AS month, 
    SUM(CAST(倉庫確認數量 AS REAL) * CAST(交易價 AS REAL)) AS monthly_revenue
FROM sales_data
WHERE strftime('%Y', 發貨日期) = '2025'
GROUP BY month
ORDER BY month;
```
"""

@ai_analysis_bp.before_request
def require_login():
    if 'logged_in' not in session:
        return redirect(url_for('login'))

@ai_analysis_bp.route('/')
def index():
    return render_template('ai_analysis.html')

@ai_analysis_bp.route('/ask', methods=['POST'])
def ask():
    if not client:
        return jsonify({'error': '系統未設定 Gemini API Key 或 Client 初始化失敗，請聯絡管理員。'}), 500

    user_question = request.json.get('question', '')
    if not user_question:
        return jsonify({'error': '請輸入問題'}), 400

    current_user = session.get('name', 'Unknown')
    user_dept = session.get('department', '')
    is_supervisor = session.get('is_supervisor', '')

    logger.info(f"User: {current_user}, Question: {user_question}")

    # 1. 第一階段：Text-to-SQL
    sql_query = generate_sql(user_question, current_user, is_supervisor)
    if not sql_query:
        return jsonify({'answer': '抱歉，我不確定如何將您的問題轉換為查詢。請嘗試換個說法。', 'sql': None})

    if "ERROR:" in sql_query:
         return jsonify({'answer': sql_query.replace("ERROR:", ""), 'sql': None})

    # 2. 執行 SQL
    try:
        results, columns = execute_sql_safely(sql_query)
    except Exception as e:
        logger.error(f"SQL Execution Error: {e}")
        return jsonify({'answer': '查詢執行失敗，請稍後再試。', 'sql': sql_query, 'error': str(e)})

    # 如果沒資料
    if not results:
        return jsonify({'answer': '根據您的查詢，沒有找到相關資料。', 'sql': sql_query, 'data': []})

    # 3. 第二階段：分析與圖表建議
    # 限制傳送給 AI 的資料量，避免 Token 爆量。只傳前 20 筆或摘要。
    data_summary = str(results[:20]) 
    if len(results) > 20:
        data_summary += f"\n... (and {len(results)-20} more rows)"

    analysis_response = generate_analysis(user_question, sql_query, data_summary, columns)
    
    return jsonify({
        'answer': analysis_response.get('text', ''),
        'sql': sql_query,
        'data': results[:100], # 前端顯示限制
        'chart': analysis_response.get('chart', None)
    })

def generate_sql(question, user_name, is_supervisor):
    """
    使用 Gemini 將自然語言轉換為 SQL。
    """
    from datetime import datetime
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # 填入日期
    system_prompt = DB_SCHEMA_TEMPLATE.format(CURRENT_DATE=current_date)
    
    prompt = f"""
    {system_prompt}

    ---
    **Current Request:**
    **User:** "{question}"
    **User Name:** "{user_name}"
    
    Generate the SQL below:
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-3-flash',
            contents=prompt
        )
        sql = response.text.strip()
        # 清理可能的回傳格式
        sql = sql.replace("```sql", "").replace("```", "").strip()

        # 🛑 FIX: 強制只取第一個 SQL 語句，避免 SQLite "You can only execute one statement at a time" 錯誤
        if ';' in sql:
            stmts = [s.strip() for s in sql.split(';') if s.strip()]
            if len(stmts) > 1:
                logger.warning(f"⚠️ Detected multiple SQL statements. Executing only the first one: {stmts[0]}")
                sql = stmts[0]
        
        # 簡單的安全性檢查
        if not sql.upper().startswith("SELECT"):
            return "ERROR: 為了安全起見，我只能執行查詢功能 (SELECT)。"
            
        return sql
    except Exception as e:
        logger.error(f"Gemini SQL Gen Error: {e}")
        return None

def execute_sql_safely(sql):
    """
    執行 SQL 並回傳 list of dict
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 再次確認是 SELECT
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed.")
        
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    # 取得欄位名稱
    columns = [description[0] for description in cursor.description]
    
    results = []
    for row in rows:
        results.append(dict(zip(columns, row)))
        
    conn.close()
    return results, columns

def generate_analysis(question, sql, data_summary, columns):
    """
    根據查詢結果生成分析文字與圖表設定JSON
    """
    prompt = f"""
    使用者的問題: "{question}"
    執行的 SQL: "{sql}"
    查詢結果 (部分): {data_summary}
    欄位清單: {columns}

    任務:
    1. 請用繁體中文回答使用者的問題，針對數據做簡短分析 (Insights)。
    2. 判斷是否適合繪製圖表 (Bar, Line, Pie, Doughnut)。
       - 適合: 產生一段 Chart.js 相容的 JSON 設定 (type, data: {{labels, datasets}})。
       - 不適合: chart 欄位回傳 null。
    
    請回傳一個 JSON 物件，格式如下:
    {{
        "text": "你的回答文字...",
        "chart": {{
            "type": "bar", 
            "data": {{ "labels": ["A", "B"], "datasets": [{{ "label": "Sales", "data": [100, 200] }}] }},
            "options": {{ ... }}
        }} OR null
    }}
    
    注意:
    - JSON 必須是標準格式，不要用 Markdown 包裹。
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-3-flash',
            contents=prompt
        )
        text = response.text.strip()
        # 嘗試清理 Markdown JSON
        if text.startswith("```json"):
            text = text[7:]
        if text.endswith("```"):
            text = text[:-3]
        
        return json.loads(text)
    except json.JSONDecodeError:
        # Fallback if generated text is not valid JSON
        logger.error("Gemini returned invalid JSON for analysis.")
        return {"text": response.text, "chart": None}
    except Exception as e:
        logger.error(f"Gemini Analysis Error: {e}")
        return {"text": "分析時發生錯誤，但資料已撈出。", "chart": None}

