"""
ADOPT AI Database Assistant v4
- SWITCHED: Groq → Cohere API (handles large payloads)
- FIXED: Table horizontal scroll - columns no longer cut off
- NEW: Clear chat button
- NEW: Smart input suggestions (autocomplete in AI mode too)
- FIXED: AI always executes SQL and shows results
- FIXED: Pagination in AI responses to avoid payload errors
- DB list sidebar with table explorer
- Manual SQL + AI Chat toggle
- AI output table names are clickable
- Pagination on table data
- Clean dark UI
"""

from flask import Flask, request, jsonify, render_template_string
import pymysql
import pymysql.cursors
import requests
import cohere
import json
import re
from datetime import datetime

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
MYSQL_SETTINGS = {
    "host":    "102.209.31.227",
    "port":    3306,
    "user":    "clusteradmin",
    "passwd":  "ADOPT@2024#WIOCC@2023",
    "charset": "utf8mb4",
}

# ── COHERE CONFIG (using SDK — same method as working projects) ──
COHERE_API_KEY = "hB0fPGMnWw9sugm3BxjROJsnpOMs51aqD9Zd5mBZ"
COHERE_MODEL   = "command-a-03-2025"

# Init Cohere SDK client
co = cohere.ClientV2(api_key=COHERE_API_KEY)

WATCH_DATABASES = [
    "adoptconvergebss", "adoptnotification", "adoptcommonapigateway",
    "adoptintegrationsystem", "adoptinventorymanagement", "adoptrevenuemanagement",
    "adoptsalesscrms", "adopttaskmanagement", "adoptticketmanagement", "adoptradiusbss",
]

SCHEMA_CACHE = {}  # { db: { table: [cols] } }

# ─────────────────────────────────────────────────────────────
#  DB HELPERS
# ─────────────────────────────────────────────────────────────
def get_conn(db=None):
    cfg = {**MYSQL_SETTINGS, "cursorclass": pymysql.cursors.DictCursor}
    if db:
        cfg["db"] = db
    return pymysql.connect(**cfg)

def safe_val(v):
    if v is None: return None
    if isinstance(v, bytes):
        try: return v.decode("utf-8")
        except: return v.hex()
    if isinstance(v, (int, float, bool, str)): return v
    return str(v)

DB_SHORT_MAP = {
    "convergebss":         "adoptconvergebss",
    "notification":        "adoptnotification",
    "commonapigateway":    "adoptcommonapigateway",
    "integrationsystem":   "adoptintegrationsystem",
    "inventorymanagement": "adoptinventorymanagement",
    "revenuemanagement":   "adoptrevenuemanagement",
    "salesscrms":          "adoptsalesscrms",
    "taskmanagement":      "adopttaskmanagement",
    "ticketmanagement":    "adoptticketmanagement",
    "radiusbss":           "adoptradiusbss",
}

def normalize_sql(sql: str) -> str:
    sql = sql.strip().rstrip("\\").strip()
    for short, full in DB_SHORT_MAP.items():
        sql = re.sub(
            rf"(?<![a-zA-Z`]){re.escape(short)}\.",
            f"{full}.",
            sql,
            flags=re.IGNORECASE
        )
    return sql

def execute_sql(sql, db=None):
    conn = None
    try:
        sql = normalize_sql(sql)
        if not db:
            m = re.search(r'`(adopt\w+)`\.', sql, re.IGNORECASE)
            if not m:
                m = re.search(r'(adopt\w+)\.', sql, re.IGNORECASE)
            if m:
                db = m.group(1)
        conn = get_conn(db)
        cur  = conn.cursor()
        if db:
            cur.execute(f"USE `{db}`")
        stmts   = [s.strip() for s in sql.split(";") if s.strip()]
        results = []
        for stmt in stmts:
            cur.execute(stmt)
            up = stmt.strip().upper()
            if up.startswith(("SELECT","SHOW","DESCRIBE","EXPLAIN","DESC")):
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                results.append({
                    "type":    "select",
                    "columns": cols,
                    "rows":    [{k: safe_val(v) for k,v in r.items()} for r in rows],
                    "count":   len(rows),
                })
            else:
                conn.commit()
                op = ("INSERT" if "INSERT" in up else
                      "UPDATE" if "UPDATE" in up else
                      "DELETE" if "DELETE" in up else "EXECUTE")
                results.append({"type": "write", "operation": op, "affected": cur.rowcount})
        cur.close(); conn.close()
        return {"success": True, "results": results}
    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
            try: conn.close()
            except: pass
        return {"success": False, "error": str(e)}

def load_schema():
    print("  Loading schema...")
    for db in WATCH_DATABASES:
        try:
            conn = get_conn()
            cur  = conn.cursor()
            cur.execute(f"SHOW TABLES FROM `{db}`")
            tables = [list(r.values())[0] for r in cur.fetchall()]
            SCHEMA_CACHE[db] = {}
            for t in tables:
                try:
                    cur.execute(f"DESCRIBE `{db}`.`{t}`")
                    SCHEMA_CACHE[db][t] = [r["Field"] for r in cur.fetchall()]
                except: pass
            cur.close(); conn.close()
            print(f"  ✔ {db}: {len(SCHEMA_CACHE[db])} tables")
        except Exception as e:
            print(f"  ✗ {db}: {e}")
    print(f"  Done.\n")

def find_relevant_tables(user_msg: str):
    """Find tables relevant to the user message — checks actual table names mentioned."""
    msg = user_msg.lower()
    relevant = []

    # 1. Direct table name mentioned in message (e.g. "tblcustomers", "tblcity")
    for db, tables in SCHEMA_CACHE.items():
        for tbl, cols in tables.items():
            if tbl.lower() in msg:
                relevant.append((db, tbl, cols))

    # 2. Keyword → table fragment mapping
    if not relevant:
        keywords_map = [
            (["customer", "customers", "client", "subscriber"], ["tblcustomer"]),
            (["staff", "employee", "staffuser"], ["tblstaff", "tbluser"]),
            (["invoice", "bill", "billing"], ["tblinvoice", "tblbill"]),
            (["payment", "transaction"], ["tblpayment", "tbltransaction"]),
            (["plan", "package", "tariff"], ["tblplan", "tblpackage"]),
            (["ticket", "issue", "complaint", "support"], ["tblticket", "tblissue"]),
            (["task", "todo"], ["tbltask"]),
            (["inventory", "stock", "device"], ["tblinventory", "tblstock"]),
            (["revenue", "earning"], ["tblrevenue"]),
            (["notification", "alert", "sms", "email"], ["tblnotification", "tblalert"]),
            (["city", "cities", "location", "area"], ["tblcity", "tblmcity", "tbllocation"]),
            (["country", "countries", "nation"], ["tblcountry", "tblmcountry"]),
            (["state", "province", "region"], ["tblstate", "tblmstate"]),
        ]
        for kws, tbl_fragments in keywords_map:
            if any(kw in msg for kw in kws):
                for db, tables in SCHEMA_CACHE.items():
                    for tbl, cols in tables.items():
                        tbl_lower = tbl.lower()
                        if any(frag in tbl_lower for frag in tbl_fragments):
                            if (db, tbl, cols) not in relevant:
                                relevant.append((db, tbl, cols))

    # Deduplicate by (db, tbl)
    seen = set()
    deduped = []
    for item in relevant:
        key = (item[0], item[1])
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    return deduped[:8]


def parse_complex_intent(message: str):
    """
    Parse complex user queries that need special handling:
    - multi-db search ("all db", "all databases", "all tblcustomers")
    - specific value search ("username is Kukoyi", "named Ahmed")
    - status filter ("terminate", "active", "inactive")
    Returns dict with intent info, or None if normal query.
    """
    msg = message.lower().strip()

    intent = {
        "multi_db": False,       # search across multiple databases
        "table_name": None,      # specific table to search
        "search_value": None,    # value to search for
        "search_type": None,     # "username", "name", "email", "status", "any"
        "status_filter": None,   # status value like "Terminate", "Active"
        "limit": 50,
        "dbs": [],               # which databases to search
    }

    # Detect multi-db intent
    multi_db_patterns = [
        r"all\s+(db|database|databases)",
        r"all\s+tbl\w+",
        r"across\s+all",
        r"from\s+all",
        r"every\s+(db|database)",
        r"in\s+all\s+(db|database|tbl)",
    ]
    import re as _re
    for pat in multi_db_patterns:
        if _re.search(pat, msg):
            intent["multi_db"] = True
            intent["dbs"] = list(SCHEMA_CACHE.keys())
            break

    # Detect specific table name
    for db, tables in SCHEMA_CACHE.items():
        for tbl in tables:
            if tbl.lower() in msg:
                intent["table_name"] = tbl
                if not intent["multi_db"]:
                    intent["dbs"] = [db]
                break

    # Detect search value patterns
    # "username is/= Kukoyi", "username: Kukoyi"
    username_match = _re.search(r"username\s+(?:is|=|:)?\s*(\w+)", msg)
    if username_match:
        intent["search_value"] = username_match.group(1)
        intent["search_type"] = "username"

    # "named X", "name is X", "name = X", "name: X"
    name_match = _re.search(r"(?:named?|name\s+(?:is|=|:)?)\s+([A-Za-z0-9]+)", message, _re.IGNORECASE)
    if name_match and not intent["search_value"]:
        intent["search_value"] = name_match.group(1)
        intent["search_type"] = "name"

    # "email is X"
    email_match = _re.search(r"email\s+(?:is|=|:)?\s*(\S+)", msg)
    if email_match and not intent["search_value"]:
        intent["search_value"] = email_match.group(1)
        intent["search_type"] = "email"

    # Status filter: "terminate", "active", "inactive", "suspended"
    status_map = {
        "terminat": "Terminate",
        "active": "Active",
        "inactive": "Inactive",
        "suspend": "Suspended",
        "block": "Blocked",
        "pending": "Pending",
    }
    for kw, status_val in status_map.items():
        if kw in msg:
            intent["status_filter"] = status_val
            break

    # Limit detection: "10 customers", "show 5"
    limit_match = _re.search(r"(\d+)", msg)
    if limit_match:
        n = int(limit_match.group(1))
        if 1 <= n <= 1000:
            intent["limit"] = n

    # Only return intent if something interesting was found
    if intent["multi_db"] or intent["search_value"] or intent["status_filter"]:
        return intent
    return None


def execute_complex_query(intent: dict) -> dict:
    """
    Execute complex multi-db or smart search queries directly in Python
    without relying on AI to generate correct SQL.
    """
    results_combined = []
    errors = []
    table_name = intent.get("table_name")
    search_value = intent.get("search_value")
    search_type = intent.get("search_type")
    status_filter = intent.get("status_filter")
    limit = intent.get("limit", 50)
    dbs = intent.get("dbs", list(SCHEMA_CACHE.keys()))

    for db in dbs:
        tables_in_db = SCHEMA_CACHE.get(db, {})

        # Which tables to search in this db
        if table_name:
            # exact table specified
            tbls_to_search = [table_name] if table_name in tables_in_db else []
        else:
            # default to tblcustomers
            tbls_to_search = [t for t in tables_in_db if "customer" in t.lower()]

        for tbl in tbls_to_search:
            cols = tables_in_db.get(tbl, [])
            if not cols:
                continue

            # Build WHERE clause using REAL columns
            where_parts = []

            if search_value and search_type == "username":
                # Find username-like column
                ucol = next((c for c in cols if "username" in c.lower() or c.lower() == "uname"), None)
                if ucol:
                    where_parts.append(f"`{ucol}` LIKE '%{search_value}%'")
                else:
                    # fallback: search in any text-like column
                    for c in cols[:5]:
                        where_parts.append(f"`{c}` LIKE '%{search_value}%'")

            elif search_value and search_type == "name":
                # Search firstname, lastname, username, name columns
                name_cols = [c for c in cols if any(x in c.lower() for x in ["firstname","lastname","username","name","fullname","custname"])]
                if name_cols:
                    where_parts = [f"`{c}` LIKE '%{search_value}%'" for c in name_cols[:4]]
                else:
                    where_parts = [f"`{cols[0]}` LIKE '%{search_value}%'"]

            elif search_value and search_type == "email":
                email_col = next((c for c in cols if "email" in c.lower()), None)
                if email_col:
                    where_parts.append(f"`{email_col}` LIKE '%{search_value}%'")

            if status_filter:
                status_col = next((c for c in cols if c.lower() in ["cstatus","status","cust_status","userstatus","state"]), None)
                if status_col:
                    where_parts.append(f"`{status_col}` = '{status_filter}'")

            where_clause = ""
            if where_parts:
                where_clause = "WHERE (" + " OR ".join(where_parts) + ")"

            sql = f"SELECT * FROM `{db}`.`{tbl}` {where_clause} LIMIT {limit}"
            res = execute_sql(sql, db)

            if res.get("success") and res.get("results"):
                for r in res["results"]:
                    if r.get("type") == "select" and r.get("rows"):
                        results_combined.append({
                            "db": db,
                            "table": tbl,
                            "sql": sql,
                            "columns": r["columns"],
                            "rows": r["rows"],
                            "count": r["count"],
                        })
            elif not res.get("success"):
                errors.append(f"{db}.{tbl}: {res.get('error','')}")

    return {
        "success": True,
        "multi_results": results_combined,
        "errors": errors,
        "total_found": sum(r["count"] for r in results_combined),
    }

def schema_to_text(user_msg=""):
    user_lower = user_msg.lower()
    focused_db = None
    for db in WATCH_DATABASES:
        short = db.replace("adopt","")
        if db in user_lower or short in user_lower:
            focused_db = db
            break

    # Find relevant tables for this query (full column list)
    relevant_tables = find_relevant_tables(user_msg)

    lines = []
    lines.append("DATABASES AND TABLES:")
    lines.append("Always use FULL name: `adoptconvergebss`.`tblcustomers` — NEVER short names.")
    lines.append("")

    # Show FULL columns for relevant tables first
    if relevant_tables:
        lines.append("=== RELEVANT TABLES WITH FULL COLUMN DETAILS ===")
        lines.append("USE ONLY THESE EXACT COLUMN NAMES — do not guess or invent column names!")
        lines.append("")
        for db, tbl, cols in relevant_tables:
            lines.append(f"TABLE: `{db}`.`{tbl}`")
            lines.append(f"  ALL COLUMNS: {', '.join(cols)}")
            lines.append("")
        lines.append("=================================================")
        lines.append("")

    for db, tables in SCHEMA_CACHE.items():
        tbl_names = list(tables.keys())
        if db == focused_db:
            lines.append(f"DB: `{db}` ({len(tbl_names)} tables) [FOCUSED]")
            for t in tbl_names:
                cols = tables[t]
                lines.append(f"  `{t}`: {', '.join(cols[:15])}{'...' if len(cols)>15 else ''}")
        else:
            lines.append(f"DB: `{db}` ({len(tbl_names)} tables)")
            chunk = ", ".join(f"`{t}`" for t in tbl_names[:20])
            lines.append(f"  Tables: {chunk}{'...' if len(tbl_names)>20 else ''}")
        lines.append("")

    return "\n".join(lines)

def is_db_related_question(message: str) -> bool:
    msg_lower = message.lower().strip()

    non_db_topics = [
        "prime minister", "president", "politician", "actor", "actress",
        "movie", "film", "song", "music", "famous", "food", "cuisine", "recipe",
        "weather", "sport", "cricket", "football",
        "who invented", "where is", "geography", "biology",
        "chemistry", "physics", "literature", "art",
        "what is your name", "your name", "who are you", "how are you",
        "hello", "hey ", "good morning", "good evening", "good night",
        "thank", "thanks", "bye", "goodbye",
        "gujrat", "gujarat", "mumbai", "delhi", "india", "pakistan", "usa",
        "america", "england", "london", "paris", "china", "japan",
    ]
    # Named people (except Modi is in db topics for some reason - keeping general)
    if re.search(r"^who is\s+(?!the\s+(table|column|schema|database))", msg_lower):
        return False
    if re.search(r"^who was\s+", msg_lower):
        return False

    db_keywords = [
        "select", "insert", "update", "delete", "drop", "create", "alter",
        "table", "column", "database", "schema", "query", "sql", "record",
        "row", "count", "tbl", "adopt", "convergebss", "notification",
        "inventory", "revenue", "radius", "ticket", "task", "show tables",
        "describe", "limit", "where clause", "join", "index", "trigger",
        "view", "procedure", "function", "primary key", "foreign key",
        "customer", "staff", "invoice", "bill", "plan", "subscriber",
        "list tables", "list columns", "show records", "fetch data",
        "find customer", "search customer", "get data", "retrieve",
        "give me", "show me", "list", "fetch", "get",
    ]

    for kw in db_keywords:
        if kw in msg_lower:
            return True

    for topic in non_db_topics:
        if topic in msg_lower:
            return False

    return True

# ─────────────────────────────────────────────────────────────
#  COHERE AI
# ─────────────────────────────────────────────────────────────
def call_cohere(system_prompt: str, user_message: str, chat_history: list, temperature=0.1, max_tokens=1000) -> str:
    """
    Call Cohere using the official Python SDK (ClientV2) — same as friend's working project.
    """
    messages = [{"role": "system", "content": system_prompt}]
    for h in chat_history[-6:]:
        role = "user" if h["role"] == "user" else "assistant"
        content = h["content"][:500] if len(h["content"]) > 500 else h["content"]
        messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_message})

    res = co.chat(
        model=COHERE_MODEL,
        messages=messages,
        max_tokens=max_tokens,
    )
    # Extract text from SDK response (same as friend's code)
    parts = res.message.content
    text = "".join(
        getattr(part, "text", "") for part in parts
        if getattr(part, "type", "text") == "text"
    ).strip()
    return text


def ask_cohere_general(message, history):
    system = """You are ADOPT AI, a helpful and smart assistant.
Answer the user's question naturally, informatively, and in detail.
Do NOT generate any SQL queries.
Do NOT reference any database tables unless directly asked.
Be conversational, helpful, and accurate."""

    return call_cohere(system, message, history, temperature=0.7, max_tokens=800)


def ask_cohere(message, history):
    schema = schema_to_text(message)

    system = f"""You are a MySQL expert assistant for the ADOPT telecom BSS system.

{schema}

═══════════════════════════════════════════════
CRITICAL RULES
═══════════════════════════════════════════════

RULE 1 — ONLY USE TABLES THAT EXIST IN THE SCHEMA ABOVE.
  - NEVER invent table names. If unsure, list available tables and ask.

RULE 2 — SQL FORMAT:
  - Use backticks: `adoptconvergebss`.`tblcustomers`
  - ALWAYS add LIMIT 50 to SELECT queries (unless user asks for specific count)
  - NEVER use USE statement
  - Write clean, readable SQL

RULE 3 — ALWAYS CHECK COLUMN NAMES from schema before using in JOINs or WHERE.

RULE 4 — RESPONSE FORMAT:
  - First: ONE short sentence what the query does
  - Then: the SQL block in ```sql ... ``` fences
  - Nothing else — no long explanations

RULE 5 — CLICKABLE TABLE FORMAT when listing tables:
TABLE:adoptconvergebss.tblcustomers
(exactly this format, one per line)

SMART DB MATCHING:
- "convergebss" → adoptconvergebss
- "notification" → adoptnotification
- "inventory"   → adoptinventorymanagement
- "revenue"     → adoptrevenuemanagement
- "sales/crm"   → adoptsalesscrms
- "task"        → adopttaskmanagement
- "ticket"      → adoptticketmanagement
- "radius"      → adoptradiusbss
- "gateway/api" → adoptcommonapigateway
- "integration" → adoptintegrationsystem

RULE 6 — COLUMN NAMES:
  - ALWAYS use the EXACT column names shown in "RELEVANT TABLES WITH FULL COLUMN DETAILS" above
  - tblcustomers uses: custid, username, firstname, lastname, email, cstatus, accountnumber, etc.
  - NEVER use made-up names like: customer_name, status, name, user_name
  - For customer status → use `cstatus` column
  - For customer name search → use `firstname` LIKE or `lastname` LIKE or `username` LIKE

RULE 7 — ALWAYS WRITE ONE CLEAN SQL BLOCK ONLY:
  - Write exactly ONE ```sql ... ``` block
  - Do NOT write multiple SQL blocks
  - Do NOT write placeholder queries like WHERE name = '[customer_name]'
  - If user says "find customer named Ahmed" → write: WHERE `firstname` LIKE '%Ahmed%' OR `lastname` LIKE '%Ahmed%'
  - If user says "terminate customers" → use the real status column e.g. WHERE `cstatus` = 'Terminate'

EXAMPLES:
User: "show customers"
Response: Fetches first 50 customer records.
```sql
SELECT *
FROM `adoptconvergebss`.`tblcustomers`
LIMIT 50
```

User: "give me 10 customers"
Response: Fetches 10 customer records.
```sql
SELECT *
FROM `adoptconvergebss`.`tblcustomers`
LIMIT 10
```

User: "show 10 terminate customers"
Response: Fetches 10 customers with Terminate status.
```sql
SELECT *
FROM `adoptconvergebss`.`tblcustomers`
WHERE `cstatus` = 'Terminate'
LIMIT 10
```

User: "find customer named Ahmed"
Response: Searches customers by name Ahmed.
```sql
SELECT *
FROM `adoptconvergebss`.`tblcustomers`
WHERE `firstname` LIKE '%Ahmed%' OR `lastname` LIKE '%Ahmed%' OR `username` LIKE '%Ahmed%'
LIMIT 50
```"""

    return call_cohere(system, message, history, temperature=0.1, max_tokens=600)


def extract_sql(text):
    m = re.findall(r"```(?:sql|SQL)?\s*(.*?)```", text, re.DOTALL)
    # Only return the FIRST sql block to avoid multiple broken queries
    return m[0].strip() if m else ""

def extract_tables(text):
    return re.findall(r"TABLE:(\w+)\.(\w+)", text)

# ─────────────────────────────────────────────────────────────
#  API ROUTES
# ─────────────────────────────────────────────────────────────
@app.route("/api/schema")
def api_schema():
    return jsonify({
        db: list(tables.keys())
        for db, tables in SCHEMA_CACHE.items()
    })

@app.route("/api/schema_full")
def api_schema_full():
    return jsonify(SCHEMA_CACHE)

@app.route("/api/table_columns")
def api_table_columns():
    db    = request.args.get("db")
    table = request.args.get("table")
    cols  = SCHEMA_CACHE.get(db, {}).get(table, [])
    return jsonify({"columns": cols})

@app.route("/api/table_data")
def api_table_data():
    db     = request.args.get("db")
    table  = request.args.get("table")
    page   = int(request.args.get("page", 1))
    limit  = int(request.args.get("limit", 50))
    offset = (page - 1) * limit
    if not db or not table:
        return jsonify({"error": "db and table required"}), 400
    cnt_res = execute_sql(f"SELECT COUNT(*) as total FROM `{db}`.`{table}`", db)
    total = 0
    if cnt_res["success"] and cnt_res["results"]:
        total = cnt_res["results"][0]["rows"][0].get("total", 0)
    res = execute_sql(f"SELECT * FROM `{db}`.`{table}` LIMIT {limit} OFFSET {offset}", db)
    return jsonify({**res, "total": total, "page": page, "limit": limit,
                    "pages": max(1, -(-total // limit))})

@app.route("/api/execute", methods=["POST"])
def api_execute():
    data = request.json
    sql  = data.get("sql","").strip()
    db   = data.get("db") or None
    if not sql:
        return jsonify({"error": "No SQL"}), 400
    return jsonify(execute_sql(sql, db))

@app.route("/api/chat", methods=["POST"])
def api_chat():
    data    = request.json
    message = data.get("message","").strip()
    history = data.get("history", [])
    if not message:
        return jsonify({"error": "empty"}), 400
    try:
        if not is_db_related_question(message):
            ai_resp = ask_cohere_general(message, history)
            return jsonify({
                "ai_response": ai_resp,
                "sql":         "",
                "tables":      [],
                "db_result":   None,
                "is_general":  True,
            })

        # ── STEP 1: Try to parse complex intent directly in Python ──
        intent = parse_complex_intent(message)

        if intent:
            # Handle complex query directly — no AI SQL generation needed
            complex_result = execute_complex_query(intent)

            # Build a nice AI description of what we did
            desc_parts = []
            if intent.get("search_value"):
                desc_parts.append(f"searching for '{intent['search_value']}'")
            if intent.get("status_filter"):
                desc_parts.append(f"with status '{intent['status_filter']}'")
            if intent.get("multi_db"):
                desc_parts.append(f"across all {len(intent['dbs'])} databases")
            if intent.get("table_name"):
                desc_parts.append(f"in {intent['table_name']}")

            desc = "Smart search: " + (", ".join(desc_parts) if desc_parts else "query executed")
            found = complex_result.get("total_found", 0)
            if found:
                desc += f" — found {found} record(s)"
            else:
                desc += " — no records found"

            # Show first SQL used
            first_sql = ""
            if complex_result.get("multi_results"):
                first_sql = complex_result["multi_results"][0].get("sql", "")

            return jsonify({
                "ai_response": desc,
                "sql":         first_sql,
                "tables":      [],
                "db_result":   None,
                "multi_result": complex_result,
                "is_general":  False,
                "is_complex":  True,
            })

        # ── STEP 2: Normal AI-generated SQL ──
        ai_resp = ask_cohere(message, history)
        # Only take the FIRST sql block — ignore duplicates
        all_sqls = re.findall(r"```(?:sql|SQL)?\s*(.*?)```", ai_resp, re.DOTALL)
        sql = all_sqls[0].strip() if all_sqls else ""
        tables  = extract_tables(ai_resp)
        db_result = None
        if sql:
            up = sql.strip().upper()
            if up.startswith(("SELECT","SHOW","DESCRIBE","EXPLAIN","DESC")):
                db_result = execute_sql(sql)
                # If query failed — show the error clearly, NO silent fallback
                if db_result and not db_result.get("success"):
                    err = db_result.get("error", "Unknown error")
                    # Try to fix: wrong column name → describe table first, retry
                    tbl_match = re.search(r"FROM\s+`?(\w+)`?\.`?(\w+)`?", sql, re.IGNORECASE)
                    if tbl_match:
                        fix_db  = tbl_match.group(1)
                        fix_tbl = tbl_match.group(2)
                        real_cols = SCHEMA_CACHE.get(fix_db, {}).get(fix_tbl, [])
                        if real_cols:
                            # Retry with AI knowing the exact columns
                            retry_msg = (
                                f"{message}\n\n"
                                f"[SYSTEM NOTE: Previous query failed: {err}\n"
                                f"Real columns of `{fix_db}`.`{fix_tbl}` are: {', '.join(real_cols)}\n"
                                f"Write a corrected single SQL query using ONLY these column names.]"
                            )
                            retry_resp = ask_cohere(retry_msg, [])
                            retry_sqls = re.findall(r"```(?:sql|SQL)?\s*(.*?)```", retry_resp, re.DOTALL)
                            if retry_sqls:
                                retry_sql = retry_sqls[0].strip()
                                retry_result = execute_sql(retry_sql)
                                if retry_result.get("success"):
                                    db_result = retry_result
                                    sql = retry_sql
                                    ai_resp = retry_resp

        return jsonify({
            "ai_response": ai_resp,
            "sql":         sql,
            "tables":      tables,
            "db_result":   db_result,
            "is_general":  False,
            "is_complex":  False,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ai_status")
def api_ai_status():
    try:
        # Quick test call using SDK
        test = co.chat(
            model=COHERE_MODEL,
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=5,
        )
        return jsonify({"ok": True, "model": COHERE_MODEL})
    except Exception as e:
        return jsonify({"ok": False, "model": COHERE_MODEL, "error": str(e)})

@app.route("/")
def index():
    return render_template_string(HTML)

# ─────────────────────────────────────────────────────────────
#  HTML
# ─────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ADOPT AI DB</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg:     #0a0e17;
  --panel:  #0e1421;
  --surf:   #131c2e;
  --surf2:  #182035;
  --border: #1e2d45;
  --text:   #8ba3c4;
  --bright: #dce8f8;
  --muted:  #3a5070;
  --green:  #10d98a;
  --blue:   #4d9fff;
  --red:    #f05252;
  --yellow: #f0b429;
  --acc:    #10d98a;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: 'IBM Plex Sans', sans-serif; height: 100vh; overflow: hidden; display: flex; flex-direction: column; }
code, .mono { font-family: 'IBM Plex Mono', monospace; }

/* HEADER */
header { height: 48px; background: var(--panel); border-bottom: 1px solid var(--border); display: flex; align-items: center; padding: 0 16px; gap: 12px; flex-shrink: 0; }
.logo { font-weight: 700; font-size: 15px; color: var(--bright); display: flex; align-items: center; gap: 8px; }
.logo-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 8px var(--green); }
.hdr-sep { flex: 1; }
.ai-pill { font-size: 10px; padding: 3px 10px; border-radius: 20px; border: 1px solid #10d98a40; background: #10d98a10; color: var(--green); font-family: 'IBM Plex Mono'; }

/* LAYOUT */
.layout { display: flex; flex: 1; overflow: hidden; }

/* SIDEBAR */
.sidebar { width: 240px; background: var(--panel); border-right: 1px solid var(--border); display: flex; flex-direction: column; overflow: hidden; flex-shrink: 0; }
.sidebar-hdr { padding: 10px 12px; font-size: 10px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; border-bottom: 1px solid var(--border); }
.sidebar-body { flex: 1; overflow-y: auto; }
.sidebar-body::-webkit-scrollbar { width: 3px; }
.sidebar-body::-webkit-scrollbar-thumb { background: var(--border); }

.db-item { cursor: pointer; }
.db-row { display: flex; align-items: center; gap: 7px; padding: 8px 12px; font-size: 12px; color: var(--text); transition: background .15s; border-bottom: 1px solid #ffffff05; }
.db-row:hover { background: var(--surf); color: var(--bright); }
.db-row.open { color: var(--blue); background: var(--surf); }
.db-icon { font-size: 11px; transition: transform .2s; }
.db-icon.rotated { transform: rotate(90deg); }
.db-name { flex: 1; font-family: 'IBM Plex Mono'; font-size: 11px; }
.db-count { font-size: 9px; color: var(--muted); }

.tables-list { display: none; padding: 4px 0; background: #0c1220; }
.tables-list.open { display: block; }
.tbl-row { display: flex; align-items: center; gap: 6px; padding: 5px 12px 5px 26px; font-size: 11px; font-family: 'IBM Plex Mono'; color: var(--muted); cursor: pointer; transition: all .15s; }
.tbl-row:hover { color: var(--bright); background: #ffffff06; }
.tbl-row.active { color: var(--green); background: #10d98a0a; }
.tbl-dot { width: 4px; height: 4px; border-radius: 50%; background: var(--border); flex-shrink: 0; }
.tbl-row:hover .tbl-dot { background: var(--blue); }
.tbl-row.active .tbl-dot { background: var(--green); }
.tbl-name { flex: 1; }
.tbl-open-btn { font-size: 12px; color: var(--muted); padding: 0 4px; border-radius: 3px; }
.tbl-open-btn:hover { color: var(--green); background: #10d98a15; }
.cols-list { display: none; background: #080d18; }
.cols-list.open { display: block; }
.col-row { display: flex; align-items: center; gap: 5px; padding: 3px 12px 3px 32px; font-size: 10px; font-family: 'IBM Plex Mono'; color: var(--muted); }
.col-row:hover { color: var(--text); }
.col-dot { color: var(--border); font-size: 9px; }
.col-name { flex: 1; }
.col-open-data { margin: 4px 12px 6px 32px; font-size: 10px; color: var(--blue); cursor: pointer; font-family: 'IBM Plex Mono'; padding: 3px 6px; border: 1px solid #4d9fff30; border-radius: 4px; text-align: center; }
.col-open-data:hover { background: #4d9fff15; }

/* MAIN */
.main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }

/* TABLE VIEW */
#table-view { display: none; flex-direction: column; flex: 1; overflow: hidden; }
.tv-header { padding: 10px 16px; background: var(--panel); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 10px; flex-shrink: 0; }
.tv-title { font-size: 13px; font-weight: 600; color: var(--bright); font-family: 'IBM Plex Mono'; }
.tv-back { background: none; border: 1px solid var(--border); color: var(--muted); font-size: 11px; padding: 3px 10px; border-radius: 5px; cursor: pointer; font-family: 'IBM Plex Mono'; }
.tv-back:hover { color: var(--bright); border-color: var(--text); }
.tv-info { font-size: 10px; color: var(--muted); margin-left: auto; }

/* ─── TABLE BODY: key fix — both axes scroll, no cut-off ─── */
.tv-body {
  flex: 1;
  overflow: auto;          /* both X and Y scroll */
  min-height: 0;           /* flex child shrink fix */
}
.tv-body::-webkit-scrollbar { width: 6px; height: 6px; }
.tv-body::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
.tv-body::-webkit-scrollbar-corner { background: var(--panel); }

/* Table: do NOT set width:100% — let it grow naturally */
.data-table {
  border-collapse: collapse;
  font-size: 11px;
  font-family: 'IBM Plex Mono';
  min-width: 100%;         /* at least full width but can be wider */
  width: max-content;      /* grow to fit all columns */
}
.data-table th {
  background: var(--surf2);
  padding: 7px 14px;
  text-align: left;
  color: var(--muted);
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: .6px;
  white-space: nowrap;
  position: sticky;
  top: 0;
  z-index: 2;
  border-bottom: 1px solid var(--border);
  border-right: 1px solid #ffffff08;
}
.data-table td {
  padding: 6px 14px;
  border-bottom: 1px solid #ffffff04;
  border-right: 1px solid #ffffff04;
  color: var(--text);
  white-space: nowrap;
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
}
.data-table tr:hover td { background: #ffffff05; color: var(--bright); }

.tv-pager { padding: 8px 16px; background: var(--panel); border-top: 1px solid var(--border); display: flex; align-items: center; gap: 8px; font-size: 11px; flex-shrink: 0; }
.pg-btn { background: var(--surf); border: 1px solid var(--border); color: var(--text); font-family: 'IBM Plex Mono'; font-size: 10px; padding: 4px 10px; border-radius: 4px; cursor: pointer; }
.pg-btn:hover:not(:disabled) { border-color: var(--blue); color: var(--blue); }
.pg-btn:disabled { opacity: .3; cursor: default; }
.pg-info { color: var(--muted); font-family: 'IBM Plex Mono'; }

/* CHAT VIEW */
#chat-view { display: flex; flex-direction: column; flex: 1; overflow: hidden; }

/* MODE TOGGLE */
.mode-bar { padding: 8px 16px; background: var(--panel); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
.mode-btn { font-size: 11px; font-family: 'IBM Plex Mono'; padding: 4px 14px; border-radius: 5px; cursor: pointer; border: 1px solid var(--border); background: transparent; color: var(--muted); transition: all .15s; }
.mode-btn.active { background: var(--acc); color: #000; border-color: var(--acc); font-weight: 600; }
.mode-sep { width: 1px; height: 16px; background: var(--border); margin: 0 2px; }
.mode-label { font-size: 10px; color: var(--muted); }
.clear-btn { margin-left: auto; font-size: 10px; font-family: 'IBM Plex Mono'; padding: 3px 10px; border-radius: 5px; cursor: pointer; border: 1px solid var(--border); background: transparent; color: var(--muted); transition: all .15s; }
.clear-btn:hover { border-color: var(--red); color: var(--red); background: #f0525210; }

/* MESSAGES */
.messages { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; }
.messages::-webkit-scrollbar { width: 3px; }
.messages::-webkit-scrollbar-thumb { background: var(--border); }

.msg { display: flex; gap: 8px; animation: fadeUp .25s ease; }
@keyframes fadeUp { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }
.msg.user { flex-direction: row-reverse; }
.av { width: 26px; height: 26px; border-radius: 6px; display: flex; align-items: center; justify-content: center; font-size: 11px; flex-shrink: 0; border: 1px solid var(--border); }
.av.ai { background: #10d98a12; }
.av.user { background: #4d9fff12; }
.bubble { max-width: 84%; padding: 9px 13px; border-radius: 8px; font-size: 12px; line-height: 1.65; }
.bubble.ai { background: var(--surf); border: 1px solid var(--border); color: var(--text); border-radius: 2px 8px 8px 8px; }
.bubble.user { background: var(--surf2); border: 1px solid #4d9fff30; color: var(--bright); border-radius: 8px 2px 8px 8px; }
.sql-blk { margin-top: 8px; background: #060b14; border: 1px solid #10d98a25; border-left: 3px solid var(--green); border-radius: 5px; padding: 8px 10px; font-family: 'IBM Plex Mono'; font-size: 10px; color: #4dffa8; white-space: pre-wrap; word-break: break-word; overflow-x: auto; }
.tbl-link { display: inline-flex; align-items: center; gap: 5px; margin: 2px 0; padding: 3px 8px; background: #4d9fff12; border: 1px solid #4d9fff30; border-radius: 4px; color: var(--blue); font-family: 'IBM Plex Mono'; font-size: 10px; cursor: pointer; transition: all .15s; }
.tbl-link:hover { background: #4d9fff22; border-color: var(--blue); }
.result-ok { display: inline-block; margin-top: 6px; font-size: 10px; padding: 2px 8px; border-radius: 4px; background: #10d98a12; color: var(--green); border: 1px solid #10d98a30; font-family: 'IBM Plex Mono'; }
.result-err { display: inline-block; margin-top: 6px; font-size: 10px; padding: 2px 8px; border-radius: 4px; background: #f0525212; color: var(--red); border: 1px solid #f0525230; font-family: 'IBM Plex Mono'; }
.thinking-dots span { display: inline-block; width: 4px; height: 4px; border-radius: 50%; background: var(--green); margin: 0 2px; animation: blink 1.2s infinite; }
.thinking-dots span:nth-child(2){animation-delay:.2s}
.thinking-dots span:nth-child(3){animation-delay:.4s}
@keyframes blink{0%,80%,100%{opacity:.2;transform:scale(.6)}40%{opacity:1;transform:scale(1)}}

/* INLINE RESULT TABLE — also scrollable both ways */
.inline-result {
  margin-top: 8px;
  border: 1px solid var(--border);
  border-radius: 5px;
  overflow: auto;
  max-height: 280px;
}
.inline-result::-webkit-scrollbar { width: 4px; height: 4px; }
.inline-result::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
.inline-result table {
  border-collapse: collapse;
  font-size: 10px;
  font-family: 'IBM Plex Mono';
  width: max-content;
  min-width: 100%;
}
.inline-result th { background: var(--surf2); padding: 5px 10px; text-align: left; color: var(--muted); font-size: 9px; text-transform: uppercase; white-space: nowrap; position: sticky; top: 0; border-right: 1px solid #ffffff08; }
.inline-result td { padding: 4px 10px; border-top: 1px solid #ffffff05; border-right: 1px solid #ffffff04; color: var(--text); white-space: nowrap; max-width: 200px; overflow: hidden; text-overflow: ellipsis; }
.inline-result tr:hover td { background: #ffffff04; }
.result-pager { margin-top: 6px; display: flex; align-items: center; gap: 6px; font-size: 10px; font-family: 'IBM Plex Mono'; }
.result-pg-btn { background: var(--surf2); border: 1px solid var(--border); color: var(--text); font-family: 'IBM Plex Mono'; font-size: 9px; padding: 2px 8px; border-radius: 3px; cursor: pointer; }
.result-pg-btn:hover:not(:disabled) { border-color: var(--blue); color: var(--blue); }
.result-pg-btn:disabled { opacity: .3; cursor: default; }
.result-pg-info { color: var(--muted); }

/* INPUT AREA */
.input-area { padding: 10px 16px; background: var(--panel); border-top: 1px solid var(--border); flex-shrink: 0; position: relative; }
.input-row { display: flex; gap: 8px; align-items: flex-end; }
.textarea-wrap { flex: 1; position: relative; }
textarea { width: 100%; background: var(--surf); border: 1px solid var(--border); color: var(--bright); font-family: 'IBM Plex Mono'; font-size: 12px; padding: 9px 12px; border-radius: 7px; resize: none; outline: none; min-height: 40px; max-height: 110px; line-height: 1.5; transition: border-color .15s; }
textarea::placeholder { color: var(--muted); }
textarea:focus { border-color: var(--acc); }
.send-btn { background: var(--acc); border: none; color: #000; font-family: 'IBM Plex Mono'; font-size: 11px; font-weight: 600; padding: 9px 16px; border-radius: 7px; cursor: pointer; height: 40px; white-space: nowrap; transition: opacity .15s; }
.send-btn:hover { opacity: .85; }
.send-btn:disabled { opacity: .4; cursor: default; }
.hint { font-size: 9px; color: var(--muted); margin-top: 5px; font-family: 'IBM Plex Mono'; }

/* ── SMART SUGGESTIONS (AI mode) ── */
#ai-suggestions {
  display: none;
  margin-bottom: 6px;
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}
.sugg-chip {
  font-size: 10px;
  font-family: 'IBM Plex Mono';
  padding: 3px 9px;
  border-radius: 12px;
  border: 1px solid var(--border);
  background: var(--surf);
  color: var(--muted);
  cursor: pointer;
  transition: all .15s;
  white-space: nowrap;
}
.sugg-chip:hover { border-color: var(--acc); color: var(--green); background: #10d98a0a; }

/* ── AUTOCOMPLETE DROPDOWN ── */
#autocomplete-box {
  display: none;
  position: absolute;
  bottom: 100%;
  left: 0;
  right: 60px;
  margin-bottom: 4px;
  background: #0d1a2e;
  border: 1px solid var(--border);
  border-radius: 7px;
  max-height: 220px;
  overflow-y: auto;
  z-index: 999;
  box-shadow: 0 -8px 32px rgba(0,0,0,0.6);
  font-family: 'IBM Plex Mono';
  font-size: 11px;
}
#autocomplete-box::-webkit-scrollbar { width: 3px; }
#autocomplete-box::-webkit-scrollbar-thumb { background: var(--border); }
.ac-header { padding: 5px 10px 3px; font-size: 9px; color: var(--muted); text-transform: uppercase; letter-spacing: .8px; border-bottom: 1px solid #1e2d45; }
.ac-item { display: flex; align-items: center; gap: 8px; padding: 6px 10px; cursor: pointer; transition: background .1s; border-bottom: 1px solid #ffffff03; }
.ac-item:hover, .ac-item.selected { background: #10d98a12; color: var(--bright); }
.ac-icon { font-size: 10px; width: 16px; text-align: center; flex-shrink: 0; }
.ac-main { flex: 1; color: var(--bright); }
.ac-sub { font-size: 9px; color: var(--muted); }
.ac-type { font-size: 8px; padding: 1px 5px; border-radius: 3px; font-weight: 600; }
.ac-type.db    { background: #4d9fff20; color: var(--blue); }
.ac-type.table { background: #10d98a15; color: var(--green); }
.ac-type.col   { background: #f0b42915; color: var(--yellow); }
.ac-type.kw    { background: #f0525215; color: var(--red); }
.ac-type.snip  { background: #a855f715; color: #a855f7; }

/* CONFIRM MODAL */
.overlay { display: none; position: fixed; inset: 0; background: #000a; z-index: 999; align-items: center; justify-content: center; }
.overlay.open { display: flex; }
.modal { background: var(--surf); border: 1px solid var(--border); border-radius: 10px; padding: 24px; max-width: 460px; width: 94%; }
.modal h3 { font-size: 15px; color: var(--bright); margin-bottom: 8px; }
.modal p { font-size: 11px; color: var(--muted); margin-bottom: 12px; line-height: 1.6; }
.modal .sql-blk { margin-bottom: 14px; }
.modal-btns { display: flex; gap: 8px; }
.btn-cancel { flex: 1; background: var(--surf2); border: 1px solid var(--border); color: var(--text); font-family: 'IBM Plex Mono'; font-size: 11px; padding: 9px; border-radius: 6px; cursor: pointer; }
.btn-cancel:hover { border-color: var(--text); color: var(--bright); }
.btn-exec { flex: 1; background: var(--red); border: none; color: #fff; font-family: 'IBM Plex Mono'; font-size: 11px; font-weight: 700; padding: 9px; border-radius: 6px; cursor: pointer; }
.btn-exec:hover { opacity: .85; }

/* EMPTY STATE */
.empty-state { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; color: var(--muted); text-align: center; padding: 32px; }
.empty-state .icon { font-size: 40px; margin-bottom: 12px; opacity: .4; }
.empty-state h4 { font-size: 14px; color: var(--surf2); margin-bottom: 6px; }
.empty-state p { font-size: 11px; line-height: 1.7; }
.suggest { margin-top: 14px; display: flex; flex-direction: column; gap: 6px; text-align: left; width: 100%; max-width: 360px; }
.sugg-btn { background: var(--surf); border: 1px solid var(--border); color: var(--text); font-family: 'IBM Plex Mono'; font-size: 10px; padding: 7px 10px; border-radius: 5px; cursor: pointer; text-align: left; transition: all .15s; }
.sugg-btn:hover { border-color: var(--acc); color: var(--green); }

.loading { text-align: center; padding: 40px; color: var(--muted); font-size: 12px; }
</style>
</head>
<body>

<header>
  <div class="logo"><div class="logo-dot"></div>ADOPT AI Database</div>
  <div class="hdr-sep"></div>
  <span class="ai-pill" id="ai-pill">⏳ Checking AI...</span>
</header>

<div class="layout">
  <!-- SIDEBAR -->
  <div class="sidebar">
    <div class="sidebar-hdr">Databases</div>
    <div class="sidebar-body" id="sidebar-body">
      <div class="loading">Loading...</div>
    </div>
  </div>

  <!-- MAIN -->
  <div class="main">

    <!-- TABLE VIEW -->
    <div id="table-view">
      <div class="tv-header">
        <button class="tv-back" onclick="closeTableView()">← Back</button>
        <span class="tv-title mono" id="tv-title">—</span>
        <span class="tv-info" id="tv-info"></span>
      </div>
      <div class="tv-body" id="tv-body"><div class="loading">Loading...</div></div>
      <div class="tv-pager" id="tv-pager"></div>
    </div>

    <!-- CHAT VIEW -->
    <div id="chat-view">
      <div class="mode-bar">
        <button class="mode-btn active" id="btn-ai" onclick="setMode('ai')">🤖 AI</button>
        <div class="mode-sep"></div>
        <button class="mode-btn" id="btn-manual" onclick="setMode('manual')">⌨️ Manual SQL</button>
        <span class="mode-label" id="mode-label">Ask anything in natural language</span>
        <button class="clear-btn" onclick="clearChat()" title="Clear all messages">🗑 Clear Chat</button>
      </div>

      <div class="messages" id="messages">
        <div class="empty-state" id="empty-state">
          <div class="icon">🗄️</div>
          <h4>ADOPT AI Database</h4>
          <p>Ask in plain language or write SQL directly.<br>Click any table in sidebar to browse data.</p>
          <div class="suggest">
            <button class="sugg-btn" onclick="useSuggestion(this)">Show all customers from convergebss database</button>
            <button class="sugg-btn" onclick="useSuggestion(this)">Count records in tblcustomers in adoptconvergebss</button>
            <button class="sugg-btn" onclick="useSuggestion(this)">Show recent 10 rows from adoptconvergebss.tblstaffuser</button>
            <button class="sugg-btn" onclick="useSuggestion(this)">List all tables in adoptinventorymanagement</button>
            <button class="sugg-btn" onclick="useSuggestion(this)">Find customers whose name contains Ahmed</button>
            <button class="sugg-btn" onclick="useSuggestion(this)">Show all databases and their table counts</button>
          </div>
        </div>
      </div>

      <div class="input-area">
        <!-- Autocomplete box (manual mode) -->
        <div id="autocomplete-box"></div>

        <!-- Smart suggestions (AI mode) -->
        <div id="ai-suggestions"></div>

        <div class="input-row">
          <div class="textarea-wrap">
            <textarea id="inp" placeholder="Ask AI e.g. 'show me all customers' or write SQL..." rows="1"></textarea>
          </div>
          <button class="send-btn" id="send-btn" onclick="send()">Send ▶</button>
        </div>
        <div class="hint" id="hint">Enter to send · Shift+Enter new line · Tab to autocomplete</div>
      </div>
    </div>

  </div>
</div>

<!-- CONFIRM MODAL -->
<div class="overlay" id="overlay">
  <div class="modal">
    <h3>⚠️ Confirm Execution</h3>
    <p>This SQL will modify the database. This cannot be undone. Are you sure?</p>
    <div class="sql-blk" id="modal-sql"></div>
    <div class="modal-btns">
      <button class="btn-cancel" onclick="closeModal()">Cancel</button>
      <button class="btn-exec" onclick="confirmExec()">Execute</button>
    </div>
  </div>
</div>

<script>
// ── State ──────────────────────────────────────────
let mode        = 'ai';
let history     = [];
let currentDB   = null;
let currentTbl  = null;
let tvPage      = 1;
let tvTotal     = 0;
let tvPages     = 1;
let pendingSQL  = null;
let pendingDB   = null;

// ── Autocomplete State ─────────────────────────────
let fullSchema  = {};
let acItems     = [];
let acIdx       = -1;

const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
  'ORDER BY', 'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET', 'JOIN', 'LEFT JOIN',
  'RIGHT JOIN', 'INNER JOIN', 'ON', 'AS', 'DISTINCT', 'COUNT', 'SUM', 'AVG',
  'MAX', 'MIN', 'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM',
  'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE', 'ADD COLUMN', 'DESCRIBE',
  'SHOW TABLES', 'SHOW DATABASES', 'USE', 'NULL', 'IS NULL', 'IS NOT NULL',
  'ASC', 'DESC', 'UNION', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
];

const SQL_SNIPPETS = [
  { label: 'SELECT * FROM', insert: 'SELECT * FROM ', desc: 'Select all columns' },
  { label: 'SELECT COUNT(*)', insert: 'SELECT COUNT(*) FROM ', desc: 'Count rows' },
  { label: 'WHERE ... LIKE', insert: "WHERE  LIKE '%'", desc: 'Pattern match' },
  { label: 'ORDER BY ... LIMIT', insert: 'ORDER BY  DESC LIMIT 50', desc: 'Sort and limit' },
  { label: 'GROUP BY ... HAVING', insert: 'GROUP BY  HAVING COUNT(*) > 1', desc: 'Group and filter' },
];

// Smart suggestions for AI mode — rotate contextually
const AI_SUGGESTIONS = [
  "show me 10 customers",
  "list all tables in convergebss",
  "count records in tblcustomers",
  "show recent staff users",
  "find customer by name",
  "show all databases",
  "list tables in adoptrevenuemanagement",
  "show invoice records",
  "count all customers",
  "show subscriber data",
];

// ── Init ───────────────────────────────────────────
async function init() {
  try {
    const r = await fetch('/api/ai_status').then(x=>x.json());
    const pill = document.getElementById('ai-pill');
    pill.textContent = r.ok ? `✅ AI Ready (${r.model})` : `⚠️ ${r.model}`;
    pill.style.borderColor = r.ok ? '#10d98a40' : '#f0b42940';
    pill.style.color = r.ok ? 'var(--green)' : 'var(--yellow)';
    pill.style.background = r.ok ? '#10d98a10' : '#f0b42910';
  } catch(e) {}

  try {
    const schema = await fetch('/api/schema').then(x=>x.json());
    renderSidebar(schema);
  } catch(e) {
    document.getElementById('sidebar-body').innerHTML = '<div style="padding:12px;font-size:11px;color:var(--red)">Failed to load</div>';
  }

  try {
    fullSchema = await fetch('/api/schema_full').then(x=>x.json());
  } catch(e) {}

  renderAISuggestions();
}

// ── Clear Chat ─────────────────────────────────────
function clearChat() {
  history = [];
  const msgs = document.getElementById('messages');
  msgs.innerHTML = `
    <div class="empty-state" id="empty-state">
      <div class="icon">🗄️</div>
      <h4>ADOPT AI Database</h4>
      <p>Ask in plain language or write SQL directly.<br>Click any table in sidebar to browse data.</p>
      <div class="suggest">
        <button class="sugg-btn" onclick="useSuggestion(this)">Show all customers from convergebss database</button>
        <button class="sugg-btn" onclick="useSuggestion(this)">Count records in tblcustomers in adoptconvergebss</button>
        <button class="sugg-btn" onclick="useSuggestion(this)">Show recent 10 rows from adoptconvergebss.tblstaffuser</button>
        <button class="sugg-btn" onclick="useSuggestion(this)">List all tables in adoptinventorymanagement</button>
        <button class="sugg-btn" onclick="useSuggestion(this)">Find customers whose name contains Ahmed</button>
        <button class="sugg-btn" onclick="useSuggestion(this)">Show all databases and their table counts</button>
      </div>
    </div>`;
}

// ── AI Suggestions (chips above input) ────────────────
function renderAISuggestions() {
  const box = document.getElementById('ai-suggestions');
  // Pick 5 random suggestions
  const picked = [...AI_SUGGESTIONS].sort(()=>Math.random()-.5).slice(0,5);
  box.innerHTML = picked.map(s =>
    `<button class="sugg-chip" onclick="useChip(this)">${s}</button>`
  ).join('');
}

function useChip(btn) {
  document.getElementById('inp').value = btn.textContent;
  send();
}

// ── Mode ───────────────────────────────────────────
function setMode(m) {
  mode = m;
  document.getElementById('btn-ai').classList.toggle('active', m==='ai');
  document.getElementById('btn-manual').classList.toggle('active', m==='manual');
  const labels = { ai: 'Ask anything in natural language', manual: 'Write SQL directly — plain syntax: select * from adoptconvergebss.tblcustomers' };
  const hints  = { ai: 'Enter to send · Shift+Enter new line', manual: 'Enter to send · Tab to autocomplete · short DB names auto-fixed' };
  document.getElementById('mode-label').textContent = labels[m];
  document.getElementById('hint').textContent = hints[m];
  document.getElementById('inp').placeholder = m === 'ai'
    ? 'Ask AI e.g. "show me 10 customers" or "list tables in convergebss"'
    : 'e.g.  select * from adoptconvergebss.tblcustomers limit 10';
  // Show/hide suggestion chips
  document.getElementById('ai-suggestions').style.display = m === 'ai' ? 'flex' : 'none';
  hideAC();
}

// ── Sidebar ────────────────────────────────────────
function renderSidebar(schema) {
  const body = document.getElementById('sidebar-body');
  body.innerHTML = '';
  for (const [db, tables] of Object.entries(schema)) {
    const item = document.createElement('div');
    item.className = 'db-item';
    const shortName = db.replace('adopt','');
    item.innerHTML = `
      <div class="db-row" onclick="toggleDB(this, '${db}')">
        <span class="db-icon">▶</span>
        <span class="db-name">${shortName}</span>
        <span class="db-count">${tables.length}</span>
      </div>
      <div class="tables-list" id="tl-${db}">
        ${tables.map(t => `
          <div class="tbl-item" id="ti-${db}-${t}">
            <div class="tbl-row" onclick="toggleTable('${db}','${t}',this)">
              <div class="tbl-dot"></div>
              <span class="tbl-name">${t}</span>
              <span class="tbl-open-btn" onclick="event.stopPropagation();openTable('${db}','${t}',null)" title="Open table data">⊞</span>
            </div>
            <div class="cols-list" id="cl-${db}-${t}"></div>
          </div>`).join('')}
      </div>`;
    body.appendChild(item);
  }
}

async function toggleTable(db, table, rowEl) {
  const colsList = document.getElementById(`cl-${db}-${table}`);
  const isOpen   = colsList.classList.toggle('open');
  rowEl.classList.toggle('active', isOpen);

  if (isOpen && colsList.innerHTML === '') {
    colsList.innerHTML = '<div style="padding:4px 12px 4px 32px;font-size:10px;color:var(--muted)">Loading...</div>';
    try {
      const r = await fetch(`/api/table_columns?db=${db}&table=${table}`).then(x=>x.json());
      if (r.columns && r.columns.length) {
        colsList.innerHTML = r.columns.map(c =>
          `<div class="col-row">
            <span class="col-dot">◦</span>
            <span class="col-name">${c}</span>
          </div>`
        ).join('') +
        `<div class="col-open-data" onclick="openTable('${db}','${table}',null)">
          ▶ Open Table Data
        </div>`;
      } else {
        colsList.innerHTML = '<div style="padding:4px 12px 4px 32px;font-size:10px;color:var(--muted)">No columns</div>';
      }
    } catch(e) {
      colsList.innerHTML = '<div style="padding:4px 12px;font-size:10px;color:var(--red)">Error</div>';
    }
  }
}

function toggleDB(row, db) {
  const list = document.getElementById(`tl-${db}`);
  const icon = row.querySelector('.db-icon');
  const open = list.classList.toggle('open');
  icon.classList.toggle('rotated', open);
  row.classList.toggle('open', open);
}

// ── Table View ─────────────────────────────────────
async function openTable(db, table, rowEl) {
  document.querySelectorAll('.tbl-row').forEach(r => r.classList.remove('active'));
  if (rowEl) rowEl.classList.add('active');

  currentDB  = db;
  currentTbl = table;
  tvPage     = 1;

  document.getElementById('table-view').style.display = 'flex';
  document.getElementById('chat-view').style.display  = 'none';
  document.getElementById('tv-title').textContent = `${db}.${table}`;
  document.getElementById('tv-body').innerHTML = '<div class="loading">Loading...</div>';
  document.getElementById('tv-pager').innerHTML = '';

  await loadTablePage(db, table, 1);
}

async function loadTablePage(db, table, page) {
  try {
    const r = await fetch(`/api/table_data?db=${db}&table=${table}&page=${page}&limit=50`).then(x=>x.json());
    tvPage  = page;
    tvTotal = r.total || 0;
    tvPages = r.pages || 1;

    document.getElementById('tv-info').textContent = `${tvTotal.toLocaleString()} rows total`;

    const body = document.getElementById('tv-body');
    if (!r.success) {
      body.innerHTML = `<div style="padding:20px;color:var(--red);font-size:12px">Error: ${r.error}</div>`;
      return;
    }
    const results = r.results || [];
    if (!results.length || !results[0].rows.length) {
      body.innerHTML = '<div style="padding:20px;color:var(--muted);font-size:12px;text-align:center">No data</div>';
      return;
    }
    const res  = results[0];
    const hdrs = res.columns.map(c => `<th>${esc(c)}</th>`).join('');
    const rows = res.rows.map(row =>
      `<tr>${res.columns.map(c => `<td title="${esc(String(row[c]??''))}">${esc(String(row[c]??''))}</td>`).join('')}</tr>`
    ).join('');
    body.innerHTML = `<table class="data-table"><thead><tr>${hdrs}</tr></thead><tbody>${rows}</tbody></table>`;

    const pager = document.getElementById('tv-pager');
    pager.innerHTML = `
      <button class="pg-btn" onclick="loadTablePage('${db}','${table}',${page-1})" ${page<=1?'disabled':''}>← Prev</button>
      <span class="pg-info">Page ${page} / ${tvPages} &nbsp;·&nbsp; ${tvTotal.toLocaleString()} rows</span>
      <button class="pg-btn" onclick="loadTablePage('${db}','${table}',${page+1})" ${page>=tvPages?'disabled':''}>Next →</button>`;
  } catch(e) {
    document.getElementById('tv-body').innerHTML = `<div style="padding:20px;color:var(--red);font-size:12px">${e.message}</div>`;
  }
}

function closeTableView() {
  document.getElementById('table-view').style.display = 'none';
  document.getElementById('chat-view').style.display  = 'flex';
  document.querySelectorAll('.tbl-row').forEach(r => r.classList.remove('active'));
  currentDB = currentTbl = null;
}

// ─────────────────────────────────────────────────────
//  AUTOCOMPLETE ENGINE (Manual SQL mode)
// ─────────────────────────────────────────────────────
const DB_SHORT = {
  'convergebss':         'adoptconvergebss',
  'notification':        'adoptnotification',
  'commonapigateway':    'adoptcommonapigateway',
  'integrationsystem':   'adoptintegrationsystem',
  'inventorymanagement': 'adoptinventorymanagement',
  'revenuemanagement':   'adoptrevenuemanagement',
  'salesscrms':          'adoptsalesscrms',
  'taskmanagement':      'adopttaskmanagement',
  'ticketmanagement':    'adoptticketmanagement',
  'radiusbss':           'adoptradiusbss',
};

function getWordBefore(text, cursorPos) {
  const before = text.slice(0, cursorPos);
  const match = before.match(/[\w.`]+$/);
  return match ? match[0] : '';
}

function getContextBefore(text, cursorPos) {
  return text.slice(0, cursorPos).toUpperCase().trim();
}

function computeSuggestions(text, cursorPos) {
  const word    = getWordBefore(text, cursorPos).toLowerCase();
  const context = getContextBefore(text, cursorPos);
  const suggestions = [];

  const afterFromPattern = /\b(FROM|JOIN|INTO|UPDATE|TABLE)\s+[\w.`]*$/i;
  const afterDotPattern  = /\b(FROM|JOIN|INTO|UPDATE|TABLE)\s+[\w`]+\.[\w`]*$/i;

  if (afterDotPattern.test(context)) {
    const dotMatch = context.match(/(\w+)\.(\w*)$/);
    if (dotMatch) {
      let dbName = dotMatch[1].toLowerCase();
      dbName = DB_SHORT[dbName] || dbName;
      const typed = dotMatch[2].toLowerCase();
      const tables = Object.keys(fullSchema[dbName] || {});
      tables.filter(t => typed === '' || t.toLowerCase().includes(typed)).slice(0, 20).forEach(t => {
        suggestions.push({ type: 'table', icon: '📋', main: t, sub: dbName, insert: t, replaceWord: dotMatch[2] });
      });
    }
  } else if (afterFromPattern.test(context)) {
    const dbWord = word.replace(/^`|`$/g,'');
    const allDBs = Object.keys(fullSchema);
    allDBs.filter(d => d.includes(dbWord)).slice(0, 10).forEach(d => {
      suggestions.push({ type: 'db', icon: '🗄️', main: d, sub: `${Object.keys(fullSchema[d]||{}).length} tables`, insert: d + '.', replaceWord: dbWord });
    });
    Object.entries(DB_SHORT).filter(([s]) => s.startsWith(dbWord)).slice(0, 5).forEach(([s, full]) => {
      if (!suggestions.find(x => x.main === full)) {
        suggestions.push({ type: 'db', icon: '🗄️', main: full, sub: s + ' (alias)', insert: full + '.', replaceWord: dbWord });
      }
    });
  }

  const selectColPattern = /SELECT\s+([\w\s,.*`]+)$/i;
  const tableInQuery = text.match(/FROM\s+`?([\w]+)`?\.`?([\w]+)`?/i);
  if (selectColPattern.test(context) && tableInQuery) {
    let db  = tableInQuery[1].toLowerCase();
    db = DB_SHORT[db] || db;
    const tbl = tableInQuery[2].toLowerCase();
    const cols = (fullSchema[db] || {})[tbl] || [];
    const lastCommaWord = context.match(/(?:SELECT|,)\s*(\w*)$/i);
    const typed = lastCommaWord ? lastCommaWord[1].toLowerCase() : '';
    cols.filter(c => typed === '' || c.toLowerCase().includes(typed)).slice(0, 15).forEach(c => {
      suggestions.push({ type: 'col', icon: '🔑', main: c, sub: `${db}.${tbl}`, insert: c, replaceWord: typed });
    });
  }

  const wherePattern = /WHERE\s+(\w*)$/i;
  const whereMatch = context.match(wherePattern);
  if (whereMatch && tableInQuery) {
    let db  = tableInQuery[1].toLowerCase();
    db = DB_SHORT[db] || db;
    const tbl = tableInQuery[2].toLowerCase();
    const cols = (fullSchema[db] || {})[tbl] || [];
    const typed = whereMatch[1].toLowerCase();
    cols.filter(c => typed === '' || c.toLowerCase().includes(typed)).slice(0, 15).forEach(c => {
      suggestions.push({ type: 'col', icon: '🔍', main: c, sub: `column in ${tbl}`, insert: c + ' = ', replaceWord: typed });
    });
  }

  const orderPattern = /(?:ORDER BY|GROUP BY)\s+(\w*)$/i;
  const orderMatch = context.match(orderPattern);
  if (orderMatch && tableInQuery) {
    let db = tableInQuery[1].toLowerCase();
    db = DB_SHORT[db] || db;
    const tbl = tableInQuery[2].toLowerCase();
    const cols = (fullSchema[db] || {})[tbl] || [];
    const typed = orderMatch[1].toLowerCase();
    cols.filter(c => typed === '' || c.toLowerCase().includes(typed)).slice(0, 10).forEach(c => {
      suggestions.push({ type: 'col', icon: '↕️', main: c, sub: `order/group column`, insert: c, replaceWord: typed });
    });
  }

  if (suggestions.length === 0 && word.length >= 1) {
    const typed = word.toUpperCase();
    SQL_KEYWORDS.filter(k => k.includes(typed)).slice(0, 8).forEach(k => {
      suggestions.push({ type: 'kw', icon: '⚡', main: k, sub: 'SQL keyword', insert: k + ' ', replaceWord: word });
    });
    SQL_SNIPPETS.filter(s => s.label.toUpperCase().startsWith(typed)).slice(0, 3).forEach(s => {
      suggestions.push({ type: 'snip', icon: '📝', main: s.label, sub: s.desc, insert: s.insert, replaceWord: word });
    });
    Object.keys(fullSchema).filter(d => d.includes(word)).slice(0, 5).forEach(d => {
      suggestions.push({ type: 'db', icon: '🗄️', main: d, sub: `${Object.keys(fullSchema[d]||{}).length} tables`, insert: d + '.', replaceWord: word });
    });
  }

  return suggestions.slice(0, 12);
}

function showAC(items) {
  const box = document.getElementById('autocomplete-box');
  if (!items.length) { hideAC(); return; }
  acItems = items;
  acIdx   = -1;
  const typeLabel = { db: 'DATABASE', table: 'TABLE', col: 'COLUMN', kw: 'KEYWORD', snip: 'SNIPPET' };
  let lastType = null;
  let html = '';
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    if (it.type !== lastType) {
      html += `<div class="ac-header">${typeLabel[it.type] || it.type}</div>`;
      lastType = it.type;
    }
    html += `<div class="ac-item" data-idx="${i}" onclick="applyAC(${i})">
      <span class="ac-icon">${it.icon}</span>
      <span class="ac-main">${esc(it.main)}</span>
      <span class="ac-sub">${esc(it.sub||'')}</span>
      <span class="ac-type ${it.type}">${it.type}</span>
    </div>`;
  }
  box.innerHTML = html;
  box.style.display = 'block';
}

function hideAC() {
  document.getElementById('autocomplete-box').style.display = 'none';
  acItems = [];
  acIdx   = -1;
}

function selectAC(dir) {
  const items = document.querySelectorAll('.ac-item');
  if (!items.length) return;
  if (acIdx >= 0) items[acIdx].classList.remove('selected');
  acIdx = (acIdx + dir + items.length) % items.length;
  items[acIdx].classList.add('selected');
  items[acIdx].scrollIntoView({ block: 'nearest' });
}

function applyAC(idx) {
  const it  = acItems[idx !== undefined ? idx : acIdx];
  if (!it) return;
  const inp = document.getElementById('inp');
  const pos = inp.selectionStart;
  const val = inp.value;
  const before   = val.slice(0, pos);
  const after    = val.slice(pos);
  const replaceLen = it.replaceWord ? it.replaceWord.length : 0;
  const newBefore = before.slice(0, before.length - replaceLen) + it.insert;
  inp.value = newBefore + after;
  const newPos = newBefore.length;
  inp.setSelectionRange(newPos, newPos);
  hideAC();
  inp.focus();
  setTimeout(() => triggerAC(), 50);
}

function triggerAC() {
  if (mode !== 'manual') { hideAC(); return; }
  const inp = document.getElementById('inp');
  const val = inp.value;
  const pos = inp.selectionStart;
  if (!val.trim()) { hideAC(); return; }
  const suggestions = computeSuggestions(val, pos);
  showAC(suggestions);
}

// ── Send ───────────────────────────────────────────
async function send() {
  const inp  = document.getElementById('inp');
  const text = inp.value.trim();
  if (!text) return;

  hideAC();
  inp.value = '';
  inp.style.height = '';
  document.getElementById('send-btn').disabled = true;
  document.getElementById('empty-state')?.remove();

  addMsg('user', text);
  const tid = addThinking();

  history.push({ role: 'user', content: text });

  try {
    if (mode === 'manual') {
      removeEl(tid);
      let fixedSQL = text.trim().replace(/\\+$/, '').trim();
      const dbMap = {
        'convergebss':        'adoptconvergebss',
        'notification':       'adoptnotification',
        'commonapigateway':   'adoptcommonapigateway',
        'integrationsystem':  'adoptintegrationsystem',
        'inventorymanagement':'adoptinventorymanagement',
        'revenuemanagement':  'adoptrevenuemanagement',
        'salesscrms':         'adoptsalesscrms',
        'taskmanagement':     'adopttaskmanagement',
        'ticketmanagement':   'adoptticketmanagement',
        'radiusbss':          'adoptradiusbss',
      };
      for (const [short, full] of Object.entries(dbMap)) {
        fixedSQL = fixedSQL.replace(
          new RegExp('(?<![a-zA-Z`])' + short + '\\.', 'gi'),
          full + '.'
        );
      }

      const needsConfirm = /\b(DELETE|UPDATE|INSERT|DROP|TRUNCATE)\b/i.test(fixedSQL);
      if (needsConfirm) {
        pendingSQL = fixedSQL; pendingDB = null;
        document.getElementById('modal-sql').textContent = text;
        document.getElementById('overlay').classList.add('open');
        document.getElementById('send-btn').disabled = false;
        return;
      }
      const res = await fetch('/api/execute', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({sql: fixedSQL, db: null})
      }).then(x=>x.json());
      renderSQLResult(res, fixedSQL);

    } else {
      // AI mode
      const data = await fetch('/api/chat', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({message: text, history})
      }).then(x=>x.json());

      removeEl(tid);
      history.push({role:'assistant', content: data.ai_response || ''});
      if (history.length > 8) history = history.slice(-8);

      if (data.error) { addMsg('ai', `❌ ${data.error}`); }
      else { renderAIResponse(data); }

      // Refresh suggestion chips after each message
      renderAISuggestions();
    }
  } catch(e) {
    removeEl(tid);
    addMsg('ai', `❌ Network error: ${e.message}`);
  }

  document.getElementById('send-btn').disabled = false;
}

// ── Render AI Response ─────────────────────────────
function renderAIResponse(data) {
  const msgs = document.getElementById('messages');
  const div  = document.createElement('div');
  div.className = 'msg ai';

  // ── Handle complex multi-DB results ──
  if (data.is_complex && data.multi_result) {
    const mr = data.multi_result;
    const multiResults = mr.multi_results || [];
    let inner = `<div style="color:var(--green);font-size:12px;margin-bottom:8px">🔍 ${esc(data.ai_response)}</div>`;

    if (!multiResults.length) {
      inner += `<div class="result-err">✗ No matching records found</div>`;
      if (mr.errors && mr.errors.length) {
        inner += `<div style="font-size:10px;color:var(--muted);margin-top:4px">${mr.errors.slice(0,3).map(e=>esc(e)).join('<br>')}</div>`;
      }
    } else {
      for (const r of multiResults) {
        const uid = 'res_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
        const PAGE_SIZE = 20;
        const totalPages = Math.ceil(r.rows.length / PAGE_SIZE);
        window['__resData_' + uid] = { columns: r.columns, rows: r.rows, page: 1, pageSize: PAGE_SIZE };

        const firstPage = r.rows.slice(0, PAGE_SIZE);
        const hdrs = r.columns.map(c=>`<th>${esc(c)}</th>`).join('');
        const rows = firstPage.map(row =>
          `<tr>${r.columns.map(c=>`<td title="${esc(String(row[c]??''))}">${esc(String(row[c]??''))}</td>`).join('')}</tr>`
        ).join('');

        inner += `<div style="font-size:10px;color:var(--blue);margin:8px 0 3px;font-family:'IBM Plex Mono'">📋 ${esc(r.db)}.${esc(r.table)} — ${r.count} row(s)</div>`;
        inner += `<div class="sql-blk" style="font-size:9px">${esc(r.sql)}</div>`;
        inner += `<div class="inline-result" id="${uid}_table"><table><thead><tr>${hdrs}</tr></thead><tbody id="${uid}_body">${rows}</tbody></table></div>`;

        if (totalPages > 1) {
          inner += `<div class="result-pager">
            <button class="result-pg-btn" onclick="resPage('${uid}',-1)" id="${uid}_prev" disabled>← Prev</button>
            <span class="result-pg-info" id="${uid}_info">Page 1 / ${totalPages} · ${r.rows.length} rows</span>
            <button class="result-pg-btn" onclick="resPage('${uid}',1)" id="${uid}_next">Next →</button>
          </div>`;
        } else {
          inner += `<span class="result-ok">✓ ${r.count} row(s)</span>`;
        }
      }
    }

    div.innerHTML = `<div class="av ai">🤖</div><div class="bubble ai">${inner}</div>`;
    msgs.appendChild(div);
    msgs.scrollTop = msgs.scrollHeight;
    return;
  }

  let text = data.ai_response || '';
  let displayText = text.replace(/```(?:sql|SQL)?[\s\S]*?```/g, '').trim();
  displayText = displayText.replace(/TABLE:(\w+)\.(\w+)/g,
    (_, db, tbl) => `<span class="tbl-link" onclick="openTable('${db}','${tbl}',null)">📋 ${db}.${tbl}</span>`
  );
  displayText = displayText.replace(/\n/g, '<br>');

  let sqlHtml = '';
  if (data.sql) {
    sqlHtml = `<div class="sql-blk">${esc(data.sql)}</div>`;
  }

  // Build paginated inline result
  let resultHtml = '';
  if (data.db_result && data.db_result.success) {
    for (const r of data.db_result.results || []) {
      if (r.type === 'select' && r.rows.length) {
        const uid = 'res_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
        const PAGE_SIZE = 20;
        const totalRows = r.rows.length;
        const totalPages = Math.ceil(totalRows / PAGE_SIZE);

        // Store data globally for paging
        window['__resData_' + uid] = { columns: r.columns, rows: r.rows, page: 1, pageSize: PAGE_SIZE };

        const firstPage = r.rows.slice(0, PAGE_SIZE);
        const hdrs = r.columns.map(c=>`<th>${esc(c)}</th>`).join('');
        const rows = firstPage.map(row =>
          `<tr>${r.columns.map(c=>`<td title="${esc(String(row[c]??''))}">${esc(String(row[c]??''))}</td>`).join('')}</tr>`
        ).join('');
        resultHtml += `<div class="inline-result" id="${uid}_table"><table><thead><tr>${hdrs}</tr></thead><tbody id="${uid}_body">${rows}</tbody></table></div>`;

        if (totalPages > 1) {
          resultHtml += `<div class="result-pager">
            <button class="result-pg-btn" onclick="resPage('${uid}',-1)" id="${uid}_prev" disabled>← Prev</button>
            <span class="result-pg-info" id="${uid}_info">Page 1 / ${totalPages} · ${totalRows} rows</span>
            <button class="result-pg-btn" onclick="resPage('${uid}',1)" id="${uid}_next">Next →</button>
          </div>`;
        } else {
          resultHtml += `<span class="result-ok">✓ ${r.count} row(s) returned</span>`;
        }
      }
    }
  }

  let tableLinks = '';
  if (data.tables && data.tables.length) {
    tableLinks = '<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:4px">';
    for (const [db, tbl] of data.tables) {
      tableLinks += `<span class="tbl-link" onclick="openTable('${db}','${tbl}',null)">📋 ${db}.${tbl}</span>`;
    }
    tableLinks += '</div>';
  }

  div.innerHTML = `
    <div class="av ai">🤖</div>
    <div class="bubble ai">${displayText}${sqlHtml}${tableLinks}${resultHtml}</div>`;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
}

// In-message result pagination
window.resPage = function(uid, dir) {
  const d = window['__resData_' + uid];
  if (!d) return;
  d.page = Math.max(1, Math.min(Math.ceil(d.rows.length / d.pageSize), d.page + dir));
  const start = (d.page - 1) * d.pageSize;
  const pageRows = d.rows.slice(start, start + d.pageSize);
  const totalPages = Math.ceil(d.rows.length / d.pageSize);

  const tbody = document.getElementById(uid + '_body');
  if (tbody) {
    tbody.innerHTML = pageRows.map(row =>
      `<tr>${d.columns.map(c=>`<td title="${esc(String(row[c]??''))}">${esc(String(row[c]??''))}</td>`).join('')}</tr>`
    ).join('');
  }
  const info = document.getElementById(uid + '_info');
  if (info) info.textContent = `Page ${d.page} / ${totalPages} · ${d.rows.length} rows`;
  const prev = document.getElementById(uid + '_prev');
  const next = document.getElementById(uid + '_next');
  if (prev) prev.disabled = d.page <= 1;
  if (next) next.disabled = d.page >= totalPages;
};

function renderSQLResult(res, sql) {
  const msgs = document.getElementById('messages');
  const div  = document.createElement('div');
  div.className = 'msg ai';

  let inner = `<div class="sql-blk">${esc(sql)}</div>`;

  if (!res.success) {
    inner += `<span class="result-err">✗ ${esc(res.error)}</span>`;
  } else {
    for (const r of res.results || []) {
      if (r.type === 'select') {
        if (r.rows.length) {
          const uid = 'res_' + Date.now() + '_' + Math.random().toString(36).slice(2,6);
          const PAGE_SIZE = 20;
          const totalRows = r.rows.length;
          const totalPages = Math.ceil(totalRows / PAGE_SIZE);
          window['__resData_' + uid] = { columns: r.columns, rows: r.rows, page: 1, pageSize: PAGE_SIZE };

          const firstPage = r.rows.slice(0, PAGE_SIZE);
          const hdrs = r.columns.map(c=>`<th>${esc(c)}</th>`).join('');
          const rows = firstPage.map(row =>
            `<tr>${r.columns.map(c=>`<td title="${esc(String(row[c]??''))}">${esc(String(row[c]??''))}</td>`).join('')}</tr>`
          ).join('');
          inner += `<div class="inline-result"><table><thead><tr>${hdrs}</tr></thead><tbody id="${uid}_body">${rows}</tbody></table></div>`;

          if (totalPages > 1) {
            inner += `<div class="result-pager">
              <button class="result-pg-btn" onclick="resPage('${uid}',-1)" id="${uid}_prev" disabled>← Prev</button>
              <span class="result-pg-info" id="${uid}_info">Page 1 / ${totalPages} · ${totalRows} rows</span>
              <button class="result-pg-btn" onclick="resPage('${uid}',1)" id="${uid}_next">Next →</button>
            </div>`;
          }
        }
        inner += `<span class="result-ok">✓ ${r.count} row(s)</span>`;
      } else {
        inner += `<span class="result-ok">✓ ${r.operation} — ${r.affected} row(s) affected</span>`;
      }
    }
  }

  div.innerHTML = `<div class="av ai">⌨️</div><div class="bubble ai">${inner}</div>`;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
}

// ── Modal ──────────────────────────────────────────
function closeModal() {
  document.getElementById('overlay').classList.remove('open');
  pendingSQL = pendingDB = null;
  document.getElementById('send-btn').disabled = false;
}

async function confirmExec() {
  document.getElementById('overlay').classList.remove('open');
  const sql = pendingSQL; const db = pendingDB;
  pendingSQL = pendingDB = null;
  const tid = addThinking();
  try {
    const res = await fetch('/api/execute', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({sql, db})
    }).then(x=>x.json());
    removeEl(tid);
    renderSQLResult(res, sql);
  } catch(e) {
    removeEl(tid);
    addMsg('ai', `❌ ${e.message}`);
  }
  document.getElementById('send-btn').disabled = false;
}

// ── Helpers ────────────────────────────────────────
function addMsg(role, text) {
  const msgs = document.getElementById('messages');
  const div  = document.createElement('div');
  div.className = `msg ${role}`;
  const av = role === 'user' ? '👤' : '🤖';
  div.innerHTML = `<div class="av ${role}">${av}</div><div class="bubble ${role}">${esc(text)}</div>`;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
}

function addThinking() {
  const msgs = document.getElementById('messages');
  const div  = document.createElement('div');
  const id   = 'th-' + Date.now();
  div.className = 'msg ai'; div.id = id;
  div.innerHTML = `<div class="av ai">🤖</div><div class="bubble ai"><div class="thinking-dots"><span></span><span></span><span></span></div></div>`;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
  return id;
}

function removeEl(id) { document.getElementById(id)?.remove(); }

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function useSuggestion(btn) {
  document.getElementById('inp').value = btn.textContent;
  send();
}

// ── Textarea events ────────────────────────────────
const inp = document.getElementById('inp');

inp.addEventListener('input', function() {
  this.style.height = '';
  this.style.height = Math.min(this.scrollHeight, 110) + 'px';
  if (mode === 'manual') triggerAC();
});

inp.addEventListener('keydown', function(e) {
  if (document.getElementById('autocomplete-box').style.display === 'block') {
    if (e.key === 'ArrowDown') { e.preventDefault(); selectAC(1); return; }
    if (e.key === 'ArrowUp')   { e.preventDefault(); selectAC(-1); return; }
    if (e.key === 'Tab' || e.key === 'Enter') {
      if (acIdx >= 0) { e.preventDefault(); applyAC(); return; }
      if (e.key === 'Tab' && acItems.length > 0) { e.preventDefault(); selectAC(1); applyAC(); return; }
    }
    if (e.key === 'Escape') { hideAC(); return; }
  }
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
});

inp.addEventListener('blur', function() {
  setTimeout(hideAC, 200);
});

inp.addEventListener('focus', function() {
  if (mode === 'manual' && this.value.trim()) triggerAC();
});

document.addEventListener('click', function(e) {
  if (!e.target.closest('#autocomplete-box') && !e.target.closest('#inp')) hideAC();
});

// Show AI suggestions on start
document.getElementById('ai-suggestions').style.display = 'flex';

init();
</script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*55)
    print("  ADOPT AI DATABASE ASSISTANT v4")
    print("  API: Cohere (command-r-plus)")
    print("="*55)
    load_schema()
    print("  Dashboard: http://localhost:7000")
    print("="*55 + "\n")
    app.run(host="0.0.0.0", port=7000, debug=False, use_reloader=False)