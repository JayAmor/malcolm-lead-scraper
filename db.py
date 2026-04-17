import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / 'leads.db'

VALID_STATUSES = {'new', 'contacted', 'qualified', 'disqualified'}


def _conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _conn() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS leads (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                business_name TEXT,
                website       TEXT UNIQUE,
                city_state    TEXT,
                industry      TEXT,
                contact_name  TEXT,
                email         TEXT,
                email_valid   TEXT,
                phone         TEXT,
                quality       TEXT,
                notes         TEXT,
                has_google_ads INTEGER DEFAULT 0,
                status        TEXT DEFAULT 'new',
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()


def save_leads(leads):
    saved = skipped = 0
    with _conn() as conn:
        for lead in leads:
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO leads
                    (business_name, website, city_state, industry, contact_name,
                     email, email_valid, phone, quality, notes, has_google_ads)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    lead.get('business_name', ''),
                    lead.get('website', ''),
                    lead.get('city_state', ''),
                    lead.get('industry', ''),
                    lead.get('contact_name', ''),
                    lead.get('email', ''),
                    lead.get('email_valid') or '',
                    lead.get('phone', ''),
                    lead.get('quality', ''),
                    lead.get('notes', ''),
                    1 if lead.get('has_google_ads') else 0,
                ))
                if conn.execute('SELECT changes()').fetchone()[0]:
                    saved += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
        conn.commit()
    return {'saved': saved, 'skipped': skipped}


def get_all_leads():
    with _conn() as conn:
        rows = conn.execute('SELECT * FROM leads ORDER BY created_at DESC').fetchall()
        return [dict(r) for r in rows]


def update_status(lead_id, status):
    if status not in VALID_STATUSES:
        return False
    with _conn() as conn:
        conn.execute('UPDATE leads SET status = ? WHERE id = ?', (status, lead_id))
        conn.commit()
    return True


def delete_lead(lead_id):
    with _conn() as conn:
        conn.execute('DELETE FROM leads WHERE id = ?', (lead_id,))
        conn.commit()


def get_stats():
    with _conn() as conn:
        total = conn.execute('SELECT COUNT(*) FROM leads').fetchone()[0]
        by_status = dict(conn.execute(
            'SELECT status, COUNT(*) FROM leads GROUP BY status'
        ).fetchall())
        by_quality = dict(conn.execute(
            'SELECT quality, COUNT(*) FROM leads GROUP BY quality'
        ).fetchall())
    return {'total': total, 'by_status': by_status, 'by_quality': by_quality}
