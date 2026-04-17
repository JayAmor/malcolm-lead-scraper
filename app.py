import io
import json
import os
import queue as q_module
import threading
from datetime import date

from flask import Flask, Response, jsonify, render_template, request, send_file, stream_with_context

from db import init_db

app = Flask(__name__)
init_db()

INDUSTRIES = {
    'Tier 1 — Home Services': ['Pest Control', 'HVAC', 'Electricians', 'Plumbers'],
    'Tier 2 — Real Estate': ['Real Estate Agent', 'Real Estate Agency'],
    'Tier 3 — Dentistry': ['Dental Implants', 'Full-Arch Dentistry', 'Dentist'],
    'Tier 4 — Medical Spas': ['Medical Spa', 'Med Spa', 'Aesthetic Clinic'],
    'Tier 5 — Senior Services': ['Senior Care', 'Senior Living', 'Home Care for Seniors'],
}

LOCATIONS = {
    'West Coast': [
        'Seattle, WA', 'Portland, OR', 'San Francisco, CA',
        'Sacramento, CA', 'Los Angeles, CA', 'San Diego, CA',
    ],
    'Texas': [
        'Houston, TX', 'Dallas, TX', 'Fort Worth, TX',
        'Austin, TX', 'San Antonio, TX',
    ],
    'Southwest': ['Phoenix, AZ', 'Scottsdale, AZ', 'Las Vegas, NV', 'Denver, CO'],
    'Southeast — Florida': ['Miami, FL', 'Orlando, FL', 'Tampa, FL', 'Sarasota, FL'],
    'Southeast — Carolinas': ['Charleston, SC', 'Raleigh, NC', 'Columbia, SC'],
    'Northeast': ['New York, NY', 'Boston, MA', 'Washington, DC'],
}


# ── Dashboard ──────────────────────────────────────────────────────────────────

@app.route('/')
def dashboard():
    today = date.today().strftime('%A, %B %d, %Y')
    build_list = [
        ('Lead Scraper', 'Malcolm AI outreach — find and qualify businesses by industry and location', '/scraper'),
        ('Lead Cultivation Tracker', 'Monitor prospects through the pipeline with status badges and follow-up reminders', '#'),
        ('HTE Prompt Comparison Tool', 'Side-by-side testing of prompt variants with scoring and notes', '#'),
        ('Daily Briefing Generator', "Auto-summarize Jim's day — meetings, tasks, priorities — into a clean digest", '#'),
        ('Client Onboarding Progress Board', "Visual tracker for each client's onboarding milestones and blockers", '#'),
    ]
    return render_template('dashboard.html', name='Jay', today=today, build_list=build_list)


# ── Scraper page ───────────────────────────────────────────────────────────────

@app.route('/scraper')
def scraper():
    return render_template('scraper.html', industries=INDUSTRIES, locations=LOCATIONS)


# ── SSE scrape stream ──────────────────────────────────────────────────────────

@app.route('/api/scrape-stream')
def api_scrape_stream():
    from scraper import scrape_leads_stream

    industries = [i.strip() for i in request.args.get('industries', '').split(',') if i.strip()]
    location = request.args.get('location', '').strip()
    count = max(1, min(int(request.args.get('count', 15)), 50))

    if not industries or not location:
        return jsonify({'error': 'At least one industry and a location are required'}), 400

    result_q = q_module.Queue()

    def run_scraper():
        try:
            for item in scrape_leads_stream(industries, location, count):
                result_q.put(item)
        except Exception as e:
            result_q.put({'error': str(e), '_done': True, 'total': 0})

    threading.Thread(target=run_scraper, daemon=True).start()

    def generate():
        while True:
            try:
                item = result_q.get(timeout=20)
                yield f'data: {json.dumps(item)}\n\n'
                if item.get('_done') or item.get('error'):
                    break
            except q_module.Empty:
                yield ': keep-alive\n\n'  # prevents Render proxy from dropping connection

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        },
    )


# ── Email validation ───────────────────────────────────────────────────────────

@app.route('/api/validate-emails', methods=['POST'])
def api_validate_emails():
    from email_validator_util import validate_leads_emails
    data = request.json or {}
    leads = data.get('leads', [])
    if not leads:
        return jsonify({'leads': []})
    return jsonify({'leads': validate_leads_emails(leads)})


# ── Export current session results ────────────────────────────────────────────

@app.route('/api/export/csv', methods=['POST'])
def api_export_csv():
    from exporter import to_csv
    leads = (request.json or {}).get('leads', [])
    return send_file(
        io.BytesIO(to_csv(leads)),
        mimetype='text/csv',
        as_attachment=True,
        download_name='malcolm-leads.csv',
    )


@app.route('/api/export/pdf', methods=['POST'])
def api_export_pdf():
    from exporter import to_pdf
    data = request.json or {}
    leads = data.get('leads', [])
    industry = data.get('industry', 'All Industries')
    location = data.get('location', 'All Locations')
    return send_file(
        io.BytesIO(to_pdf(leads, industry, location)),
        mimetype='application/pdf',
        as_attachment=True,
        download_name='malcolm-leads.pdf',
    )


# ── Database routes ────────────────────────────────────────────────────────────

@app.route('/api/leads/save', methods=['POST'])
def api_save_leads():
    from db import save_leads
    leads = (request.json or {}).get('leads', [])
    return jsonify(save_leads(leads))


@app.route('/api/leads')
def api_get_leads():
    from db import get_all_leads
    return jsonify({'leads': get_all_leads()})


@app.route('/api/leads/stats')
def api_lead_stats():
    from db import get_stats
    return jsonify(get_stats())


@app.route('/api/leads/<int:lead_id>/status', methods=['PATCH'])
def api_update_status(lead_id):
    from db import update_status
    status = (request.json or {}).get('status')
    if update_status(lead_id, status):
        return jsonify({'ok': True})
    return jsonify({'error': 'Invalid status'}), 400


@app.route('/api/leads/<int:lead_id>', methods=['DELETE'])
def api_delete_lead(lead_id):
    from db import delete_lead
    delete_lead(lead_id)
    return jsonify({'ok': True})


@app.route('/api/leads/export/csv')
def api_db_export_csv():
    from db import get_all_leads
    from exporter import to_csv
    leads = get_all_leads()
    return send_file(
        io.BytesIO(to_csv(leads)),
        mimetype='text/csv',
        as_attachment=True,
        download_name='all-leads.csv',
    )


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(port=port, debug=False, threaded=True)
