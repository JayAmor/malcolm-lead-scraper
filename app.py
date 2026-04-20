import io
import os
import threading
import uuid
from datetime import date

from flask import Flask, jsonify, render_template, request, send_file

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


# ── Job-based scraping ────────────────────────────────────────────────────────

@app.route('/api/jobs', methods=['POST'])
def start_job():
    try:
        from scraper import scrape_leads_stream
        from db import create_job, append_job_lead, finish_job, purge_old_jobs

        data = request.json or {}
        industries = [i.strip() for i in data.get('industries', '').split(',') if i.strip()]
        location = data.get('location', '').strip()
        count = max(1, min(int(data.get('count', 15)), 50))

        if not industries or not location:
            return jsonify({'error': 'At least one industry and a location are required'}), 400

        purge_old_jobs()
        job_id = uuid.uuid4().hex[:10]
        create_job(job_id)

        def run():
            try:
                for item in scrape_leads_stream(industries, location, count):
                    if item.get('_done'):
                        finish_job(job_id, 'done')
                        break
                    elif item.get('error'):
                        finish_job(job_id, 'error', item['error'])
                        break
                    else:
                        append_job_lead(job_id, item)
            except Exception as e:
                finish_job(job_id, 'error', str(e))

        threading.Thread(target=run, daemon=True).start()
        return jsonify({'job_id': job_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/jobs/<job_id>')
def poll_job(job_id):
    try:
        from db import get_job
        job = get_job(job_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        return jsonify(job)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Email validation ───────────────────────────────────────────────────────────

@app.route('/api/validate-emails', methods=['POST'])
def api_validate_emails():
    try:
        from email_validator_util import validate_leads_emails
        data = request.json or {}
        leads = data.get('leads', [])
        if not leads:
            return jsonify({'leads': []})
        return jsonify({'leads': validate_leads_emails(leads)})
    except Exception as e:
        return jsonify({'error': str(e), 'leads': []}), 500


# ── Export current session results ────────────────────────────────────────────

@app.route('/api/export/csv', methods=['POST'])
def api_export_csv():
    try:
        from exporter import to_csv
        leads = (request.json or {}).get('leads', [])
        if not leads:
            return jsonify({'error': 'No leads to export'}), 400
        return send_file(
            io.BytesIO(to_csv(leads)),
            mimetype='text/csv',
            as_attachment=True,
            download_name='malcolm-leads.csv',
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/export/pdf', methods=['POST'])
def api_export_pdf():
    try:
        from exporter import to_pdf
        data = request.json or {}
        leads = data.get('leads', [])
        if not leads:
            return jsonify({'error': 'No leads to export'}), 400
        industry = data.get('industry', 'All Industries')
        location = data.get('location', 'All Locations')
        return send_file(
            io.BytesIO(to_pdf(leads, industry, location)),
            mimetype='application/pdf',
            as_attachment=True,
            download_name='malcolm-leads.pdf',
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Database routes ────────────────────────────────────────────────────────────

@app.route('/api/leads/save', methods=['POST'])
def api_save_leads():
    try:
        from db import save_leads
        leads = (request.json or {}).get('leads', [])
        return jsonify(save_leads(leads))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/leads')
def api_get_leads():
    try:
        from db import get_all_leads
        return jsonify({'leads': get_all_leads()})
    except Exception as e:
        return jsonify({'leads': [], 'error': str(e)}), 500


@app.route('/api/leads/stats')
def api_lead_stats():
    try:
        from db import get_stats
        return jsonify(get_stats())
    except Exception as e:
        return jsonify({'total': 0, 'by_status': {}, 'by_quality': {}, 'error': str(e)}), 500


@app.route('/api/leads/<int:lead_id>/status', methods=['PATCH'])
def api_update_status(lead_id):
    try:
        from db import update_status
        status = (request.json or {}).get('status')
        if update_status(lead_id, status):
            return jsonify({'ok': True})
        return jsonify({'error': 'Invalid status'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/leads/<int:lead_id>', methods=['DELETE'])
def api_delete_lead(lead_id):
    try:
        from db import delete_lead
        delete_lead(lead_id)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/leads/export/csv')
def api_db_export_csv():
    try:
        from db import get_all_leads
        from exporter import to_csv
        leads = get_all_leads()
        if not leads:
            return jsonify({'error': 'No leads in database'}), 400
        return send_file(
            io.BytesIO(to_csv(leads)),
            mimetype='text/csv',
            as_attachment=True,
            download_name='all-leads.csv',
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(port=port, debug=False, threaded=True)
