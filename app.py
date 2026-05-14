"""
Digitalzilaa Portal — Streamlit production management app.

Single-file app: UI + SQLite + APScheduler daily WhatsApp automation.
Run:  streamlit run app.py
"""
from __future__ import annotations

import os
import sqlite3
import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("digitalzilaa")

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "digitalzilaa.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"
UPLOADS_DIR = BASE_DIR / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "change-me")
DAILY_SEND_TIME = os.getenv("DAILY_SEND_TIME", "10:00")

ROLES = ["Designer", "AI Video Maker", "Video Editor"]


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA_PATH.read_text())
    log.info("DB initialised at %s", DB_PATH)


def query(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(sql, params).fetchall()


def execute(sql: str, params: tuple = ()) -> int:
    with get_db() as conn:
        cur = conn.execute(sql, params)
        return cur.lastrowid


# ---------------------------------------------------------------------------
# WhatsApp transports
# ---------------------------------------------------------------------------
def send_whatsapp(to_number: str, message: str) -> tuple[bool, str]:
    """Try Twilio → Webhook → pywhatkit. Never raises."""
    # 1. Twilio
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    sender = os.getenv("TWILIO_WHATSAPP_FROM")
    if sid and token and sender:
        try:
            from twilio.rest import Client
            client = Client(sid, token)
            msg = client.messages.create(
                from_=sender if sender.startswith("whatsapp:") else f"whatsapp:{sender}",
                to=f"whatsapp:{to_number}",
                body=message,
            )
            return True, f"twilio:{msg.sid}"
        except Exception as e:  # noqa: BLE001
            log.warning("Twilio send failed: %s", e)

    # 2. Webhook
    webhook = os.getenv("WHATSAPP_WEBHOOK_URL")
    if webhook:
        try:
            import requests
            r = requests.post(webhook, json={"to": to_number, "message": message}, timeout=10)
            if r.ok:
                return True, f"webhook:{r.status_code}"
            return False, f"webhook http {r.status_code}: {r.text[:120]}"
        except Exception as e:  # noqa: BLE001
            log.warning("Webhook send failed: %s", e)

    # 3. pywhatkit (desktop only)
    if os.getenv("ENABLE_PYWHATKIT", "false").lower() == "true":
        try:
            import pywhatkit
            now = datetime.now()
            send_at = now + timedelta(minutes=1)
            pywhatkit.sendwhatmsg(to_number, message, send_at.hour, send_at.minute,
                                  wait_time=15, tab_close=True)
            return True, "pywhatkit"
        except Exception as e:  # noqa: BLE001
            log.warning("pywhatkit failed: %s", e)

    return False, "no transport configured or all failed"


def build_message(name: str, tasks: list[sqlite3.Row]) -> str:
    if not tasks:
        return f"Hi {name}, you have no tasks scheduled for today. Enjoy the breather!"
    items = ", ".join(f"{i+1}. {t['client_name']} - {t['hook']}"
                      for i, t in enumerate(tasks))
    return (f"Hi {name}, your tasks for today are: {items}. "
            "Please update status on the portal.")


def dispatch_daily(target_date: Optional[date] = None) -> list[dict]:
    target_date = target_date or date.today()
    results = []
    members = query("SELECT * FROM team_members WHERE role IN ('Designer','AI Video Maker','Video Editor')")
    for m in members:
        tasks = query("""
            SELECT t.*, c.name AS client_name
            FROM tasks t JOIN clients c ON c.id = t.client_id
            WHERE t.assignee_role = ? AND t.task_date = ?
            ORDER BY c.name
        """, (m["role"], target_date))
        msg = build_message(m["name"], tasks)
        ok, detail = send_whatsapp(m["whatsapp_number"], msg)
        execute("""INSERT INTO notification_log(sent_date, member_id, channel, status, detail)
                   VALUES (?, ?, 'whatsapp', ?, ?)""",
                (target_date, m["id"], "sent" if ok else "failed", detail))
        results.append({"member": m["name"], "ok": ok, "detail": detail, "tasks": len(tasks)})
    return results


# ---------------------------------------------------------------------------
# Scheduler — runs in the Streamlit process
# ---------------------------------------------------------------------------
@st.cache_resource
def start_scheduler():
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BackgroundScheduler(daemon=True)
    try:
        hour, minute = [int(x) for x in DAILY_SEND_TIME.split(":")]
    except Exception:
        hour, minute = 10, 0
    scheduler.add_job(dispatch_daily, CronTrigger(hour=hour, minute=minute),
                      id="daily_whatsapp", replace_existing=True)
    scheduler.start()
    log.info("Scheduler started — daily WhatsApp at %02d:%02d", hour, minute)
    return scheduler


# ---------------------------------------------------------------------------
# Theme — Digitalzilaa: deep black, white, slate
# ---------------------------------------------------------------------------
THEME_CSS = """
<style>
:root {
  --bg: #0a0a0a; --surface: #111113; --surface-2: #16161a;
  --border: #24242a; --text: #f5f5f7; --muted: #8b8b95; --accent: #64748b;
}
html, body, [class*="css"], .stApp { background: var(--bg) !important; color: var(--text) !important;
  font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", sans-serif; }
section[data-testid="stSidebar"] { background: #07070a !important; border-right: 1px solid var(--border); }
section[data-testid="stSidebar"] * { color: var(--text) !important; }
h1, h2, h3, h4 { color: var(--text); letter-spacing: -0.02em; font-weight: 600; }
.stButton>button, .stDownloadButton>button {
  background: var(--text); color: #000; border: 0; border-radius: 10px;
  padding: 0.55rem 1.1rem; font-weight: 600; transition: transform .08s ease;
}
.stButton>button:hover { transform: translateY(-1px); }
.stTextInput input, .stTextArea textarea, .stNumberInput input, .stDateInput input,
.stSelectbox div[data-baseweb="select"] > div {
  background: var(--surface) !important; color: var(--text) !important;
  border: 1px solid var(--border) !important; border-radius: 10px !important;
}
[data-testid="stMetric"] { background: var(--surface); border: 1px solid var(--border);
  border-radius: 14px; padding: 1rem; }
.task-card { background: var(--surface); border: 1px solid var(--border);
  border-radius: 16px; padding: 1.1rem 1.25rem; margin-bottom: 0.9rem; }
.task-card .brand { font-size: 0.78rem; letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--muted); }
.task-card .hook { font-size: 1.05rem; color: var(--text); margin: 0.35rem 0 0.6rem; font-weight: 500; }
.task-card .meta { color: var(--muted); font-size: 0.82rem; }
.pill { display: inline-block; padding: 0.18rem 0.6rem; border-radius: 999px;
  font-size: 0.72rem; font-weight: 600; letter-spacing: 0.04em; border: 1px solid var(--border);
  background: var(--surface-2); color: var(--muted); text-transform: uppercase; }
.pill.pending { color: #fbbf24; border-color: #3a2f10; }
.pill.review  { color: #60a5fa; border-color: #102a3a; }
.pill.approved{ color: #34d399; border-color: #0f3a2a; }
.pill.rejected{ color: #f87171; border-color: #3a1010; }
.brand-mark { font-weight: 700; letter-spacing: -0.03em; font-size: 1.4rem; }
.brand-mark span { color: var(--muted); font-weight: 400; }
hr { border-color: var(--border) !important; }
</style>
"""


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def login_gate() -> Optional[dict]:
    """Returns user dict {role, name} or None. Renders login form if needed."""
    if "user" in st.session_state:
        return st.session_state.user

    st.markdown("<div class='brand-mark'>Digitalzilaa <span>Portal</span></div>", unsafe_allow_html=True)
    st.caption("Sign in to continue")

    mode = st.radio("Sign in as", ["Admin", "Team Member"], horizontal=True, label_visibility="collapsed")
    if mode == "Admin":
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("Sign in", use_container_width=True):
            if u == ADMIN_USERNAME and p == ADMIN_PASSWORD:
                st.session_state.user = {"role": "Admin", "name": "Admin"}
                st.rerun()
            else:
                st.error("Invalid credentials")
    else:
        members = query("SELECT * FROM team_members WHERE role IN ('Designer','AI Video Maker','Video Editor') ORDER BY name")
        if not members:
            st.info("No team members yet. Ask the admin to add you.")
            return None
        choice = st.selectbox("Who are you?", members, format_func=lambda m: f"{m['name']} — {m['role']}")
        if st.button("Enter", use_container_width=True):
            st.session_state.user = {"role": choice["role"], "name": choice["name"], "id": choice["id"]}
            st.rerun()
    return None


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def status_pill(status: str) -> str:
    cls = {"Pending": "pending", "In Progress": "pending",
           "Under Review": "review", "Approved": "approved", "Rejected": "rejected"}.get(status, "")
    return f"<span class='pill {cls}'>{status}</span>"


def render_task_card(task: sqlite3.Row, *, editable: bool = False):
    logo_html = ""
    if task["logo_path"] and Path(task["logo_path"]).exists():
        logo_html = f"<img src='data:image/png;base64,' style='display:none'>"  # placeholder
    with st.container():
        st.markdown(f"""
        <div class='task-card'>
          <div class='brand'>{task['client_name']}</div>
          <div class='hook'>{task['hook']}</div>
          <div class='meta'>{task['idea'] or ''}</div>
          <div style='margin-top:0.7rem'>{status_pill(task['status'])}
            <span class='meta' style='margin-left:0.6rem'>Due {task['task_date']}</span></div>
        </div>
        """, unsafe_allow_html=True)
        if editable:
            with st.expander("Submit / update"):
                link = st.text_input("Submission link", value=task["submission_link"] or "",
                                     key=f"link_{task['id']}")
                colA, colB = st.columns(2)
                if colA.button("Save & mark Under Review", key=f"sub_{task['id']}"):
                    execute("""UPDATE tasks SET submission_link=?, status='Under Review',
                               updated_at=CURRENT_TIMESTAMP WHERE id=?""", (link, task["id"]))
                    st.success("Submitted for review.")
                    st.rerun()
                if colB.button("Mark In Progress", key=f"prog_{task['id']}"):
                    execute("UPDATE tasks SET status='In Progress', updated_at=CURRENT_TIMESTAMP WHERE id=?",
                            (task["id"],))
                    st.rerun()
                if task["review_comment"]:
                    st.warning(f"Reviewer note: {task['review_comment']}")


# ---------------------------------------------------------------------------
# Pages — Admin
# ---------------------------------------------------------------------------
def page_admin_overview():
    st.header("Overview")
    today = date.today()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clients", query("SELECT COUNT(*) c FROM clients")[0]["c"])
    c2.metric("Team", query("SELECT COUNT(*) c FROM team_members")[0]["c"])
    c3.metric("Today's tasks", query("SELECT COUNT(*) c FROM tasks WHERE task_date=?", (today,))[0]["c"])
    c4.metric("Awaiting review",
              query("SELECT COUNT(*) c FROM tasks WHERE status='Under Review'")[0]["c"])

    st.subheader("Upcoming 7 days")
    rows = query("""
        SELECT t.task_date, t.assignee_role, t.hook, t.status, c.name AS client_name
        FROM tasks t JOIN clients c ON c.id=t.client_id
        WHERE t.task_date BETWEEN ? AND ? ORDER BY t.task_date, c.name
    """, (today, today + timedelta(days=7)))
    if rows:
        st.dataframe(pd.DataFrame([dict(r) for r in rows]), use_container_width=True, hide_index=True)
    else:
        st.info("No upcoming tasks. Add some in the Content Calendar.")


def page_clients():
    st.header("Clients")
    with st.expander("➕ Add client", expanded=False):
        with st.form("add_client", clear_on_submit=True):
            name = st.text_input("Client name")
            quota = st.number_input("Monthly content quota", min_value=0, value=20, step=1)
            logo = st.file_uploader("Logo", type=["png", "jpg", "jpeg", "webp"])
            if st.form_submit_button("Add client"):
                if not name.strip():
                    st.error("Name required")
                else:
                    logo_path = None
                    if logo:
                        logo_path = str(UPLOADS_DIR / f"{datetime.now().timestamp():.0f}_{logo.name}")
                        Path(logo_path).write_bytes(logo.read())
                    try:
                        execute("INSERT INTO clients(name, monthly_quota, logo_path) VALUES (?,?,?)",
                                (name.strip(), quota, logo_path))
                        st.success(f"Added {name}")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("A client with that name already exists.")

    clients = query("SELECT * FROM clients ORDER BY name")
    if not clients:
        st.info("No clients yet.")
        return
    for c in clients:
        with st.container():
            cols = st.columns([1, 4, 2, 2])
            if c["logo_path"] and Path(c["logo_path"]).exists():
                cols[0].image(c["logo_path"], width=64)
            else:
                cols[0].markdown("<div style='width:64px;height:64px;border:1px solid #24242a;border-radius:12px'></div>", unsafe_allow_html=True)
            cols[1].markdown(f"**{c['name']}**  \n<span class='meta'>Quota {c['monthly_quota']}/mo</span>",
                             unsafe_allow_html=True)
            new_q = cols[2].number_input("Quota", value=c["monthly_quota"], key=f"q_{c['id']}",
                                         label_visibility="collapsed")
            if cols[3].button("Save", key=f"save_c_{c['id']}"):
                execute("UPDATE clients SET monthly_quota=? WHERE id=?", (new_q, c["id"]))
                st.toast("Updated"); st.rerun()
            if cols[3].button("Delete", key=f"del_c_{c['id']}"):
                execute("DELETE FROM clients WHERE id=?", (c["id"],)); st.rerun()
            st.divider()


def page_team():
    st.header("Team")
    with st.expander("➕ Add team member", expanded=False):
        with st.form("add_member", clear_on_submit=True):
            name = st.text_input("Name")
            role = st.selectbox("Role", ROLES + ["Admin"])
            wa = st.text_input("WhatsApp number (E.164, e.g. +919812345678)")
            if st.form_submit_button("Add"):
                if not (name and wa):
                    st.error("Name and WhatsApp number required")
                else:
                    execute("INSERT INTO team_members(name, role, whatsapp_number) VALUES (?,?,?)",
                            (name.strip(), role, wa.strip()))
                    st.success(f"Added {name}"); st.rerun()

    members = query("SELECT * FROM team_members ORDER BY role, name")
    if members:
        df = pd.DataFrame([dict(m) for m in members])[["id", "name", "role", "whatsapp_number"]]
        st.dataframe(df, use_container_width=True, hide_index=True)
        del_id = st.number_input("Remove member by ID", min_value=0, value=0, step=1)
        if st.button("Delete member") and del_id:
            execute("DELETE FROM team_members WHERE id=?", (del_id,)); st.rerun()
    else:
        st.info("No team members yet.")


def page_calendar():
    st.header("Content Calendar")
    clients = query("SELECT * FROM clients ORDER BY name")
    if not clients:
        st.warning("Add at least one client first.")
        return
    with st.expander("➕ Add content task", expanded=True):
        with st.form("add_task", clear_on_submit=True):
            cl = st.selectbox("Client", clients, format_func=lambda c: c["name"])
            d = st.date_input("Date", value=date.today())
            role = st.selectbox("Assign to role", ROLES)
            hook = st.text_input("Hook / headline")
            idea = st.text_area("Content idea / brief")
            if st.form_submit_button("Add to calendar"):
                if not hook.strip():
                    st.error("Hook required")
                else:
                    execute("""INSERT INTO tasks(client_id, assignee_role, task_date, hook, idea)
                               VALUES (?,?,?,?,?)""",
                            (cl["id"], role, d, hook.strip(), idea.strip()))
                    st.success("Added"); st.rerun()

    rows = query("""
        SELECT t.*, c.name AS client_name FROM tasks t JOIN clients c ON c.id=t.client_id
        ORDER BY t.task_date DESC, c.name
        LIMIT 200
    """)
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])[
            ["id", "task_date", "client_name", "assignee_role", "hook", "status"]]
        st.dataframe(df, use_container_width=True, hide_index=True)
        del_id = st.number_input("Delete task by ID", min_value=0, value=0, step=1)
        if st.button("Delete task") and del_id:
            execute("DELETE FROM tasks WHERE id=?", (del_id,)); st.rerun()


def page_approvals():
    st.header("Approvals")
    rows = query("""
        SELECT t.*, c.name AS client_name, c.logo_path
        FROM tasks t JOIN clients c ON c.id=t.client_id
        WHERE t.status='Under Review' ORDER BY t.task_date
    """)
    if not rows:
        st.success("Nothing pending review. ✨")
        return
    for t in rows:
        with st.container():
            st.markdown(f"""
            <div class='task-card'>
              <div class='brand'>{t['client_name']} • {t['assignee_role']}</div>
              <div class='hook'>{t['hook']}</div>
              <div class='meta'>{t['idea'] or ''}</div>
              <div class='meta' style='margin-top:.4rem'>Due {t['task_date']}</div>
            </div>
            """, unsafe_allow_html=True)
            if t["submission_link"]:
                st.markdown(f"🔗 [Open submission]({t['submission_link']})")
            comment = st.text_input("Reviewer comment", key=f"cm_{t['id']}")
            cA, cB = st.columns(2)
            if cA.button("Approve", key=f"app_{t['id']}"):
                execute("""UPDATE tasks SET status='Approved', review_comment=?,
                           updated_at=CURRENT_TIMESTAMP WHERE id=?""", (comment, t["id"]))
                st.rerun()
            if cB.button("Reject", key=f"rej_{t['id']}"):
                execute("""UPDATE tasks SET status='Rejected', review_comment=?,
                           updated_at=CURRENT_TIMESTAMP WHERE id=?""", (comment, t["id"]))
                st.rerun()


def page_settings():
    st.header("Settings & Automation")
    st.write(f"**Daily WhatsApp send time:** `{DAILY_SEND_TIME}` (set via `DAILY_SEND_TIME` in `.env`)")
    st.write("Transports detected:")
    st.write({
        "Twilio":   bool(os.getenv("TWILIO_ACCOUNT_SID") and os.getenv("TWILIO_AUTH_TOKEN")),
        "Webhook":  bool(os.getenv("WHATSAPP_WEBHOOK_URL")),
        "pywhatkit": os.getenv("ENABLE_PYWHATKIT", "false").lower() == "true",
    })
    if st.button("📨 Send today's WhatsApp now"):
        with st.spinner("Sending…"):
            results = dispatch_daily()
        if not results:
            st.info("No team members to notify.")
        for r in results:
            (st.success if r["ok"] else st.error)(
                f"{r['member']} — {r['tasks']} task(s) — {r['detail']}")
    st.subheader("Notification log (last 50)")
    rows = query("""SELECT n.*, m.name AS member_name FROM notification_log n
                    LEFT JOIN team_members m ON m.id = n.member_id
                    ORDER BY n.created_at DESC LIMIT 50""")
    if rows:
        st.dataframe(pd.DataFrame([dict(r) for r in rows]), use_container_width=True, hide_index=True)
    else:
        st.caption("No notifications sent yet.")


# ---------------------------------------------------------------------------
# Pages — Team
# ---------------------------------------------------------------------------
def page_team_dashboard(role: str, name: str):
    st.header(f"{role} — {name}")
    st.caption("3-day view: Today, Tomorrow, Day After")
    today = date.today()
    cols = st.columns(3)
    for col, offset, label in zip(cols, [0, 1, 2], ["Today", "Tomorrow", "Day After"]):
        d = today + timedelta(days=offset)
        with col:
            st.subheader(f"{label}")
            st.caption(d.strftime("%A, %d %b"))
            tasks = query("""
                SELECT t.*, c.name AS client_name, c.logo_path
                FROM tasks t JOIN clients c ON c.id=t.client_id
                WHERE t.assignee_role=? AND t.task_date=? ORDER BY c.name
            """, (role, d))
            if not tasks:
                st.markdown("<div class='task-card meta'>Nothing scheduled.</div>", unsafe_allow_html=True)
            for t in tasks:
                render_task_card(t, editable=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Digitalzilaa Portal", page_icon="◼", layout="wide")
    st.markdown(THEME_CSS, unsafe_allow_html=True)

    init_db()
    try:
        start_scheduler()
    except Exception as e:  # noqa: BLE001
        log.warning("Scheduler not started: %s", e)

    user = login_gate()
    if not user:
        return

    with st.sidebar:
        st.markdown("<div class='brand-mark'>Digitalzilaa <span>Portal</span></div>",
                    unsafe_allow_html=True)
        st.caption(f"Signed in as **{user['name']}** · {user['role']}")
        st.divider()
        if user["role"] == "Admin":
            page = st.radio("Navigate", ["Overview", "Clients", "Team",
                                         "Content Calendar", "Approvals", "Settings"],
                            label_visibility="collapsed")
        else:
            page = "Dashboard"
            st.markdown("**My Dashboard**")
        st.divider()
        if st.button("Sign out", use_container_width=True):
            st.session_state.pop("user", None); st.rerun()

    if user["role"] == "Admin":
        {"Overview": page_admin_overview, "Clients": page_clients, "Team": page_team,
         "Content Calendar": page_calendar, "Approvals": page_approvals,
         "Settings": page_settings}[page]()
    else:
        page_team_dashboard(user["role"], user["name"])


if __name__ == "__main__":
    main()
