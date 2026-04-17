import io
from datetime import date

from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)

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


@app.route('/')
def dashboard():
    today = date.today().strftime('%A, %B %d, %Y')
    build_list = [
        ('Lead Scraper', 'Malcolm AI outreach — find and qualify businesses by industry and location', '/scraper'),
        ('Lead Cultivation Tracker', 'Monitor prospects through the pipeline with status badges and follow-up reminders', '#'),
        ('HTE Prompt Comparison Tool', 'Side-by-side testing of prompt variants with scoring and notes', '#'),
        ('Daily Briefing Generator', 'Auto-summarize Jim\'s day — meetings, tasks, priorities — into a clean digest', '#'),
        ('Client Onboarding Progress Board', 'Visual tracker for each client\'s onboarding milestones and blockers', '#'),
    ]
    return render_template('dashboard.html', name='Jay', today=today, build_list=build_list)


@app.route('/scraper')
def scraper():
    return render_template('scraper.html', industries=INDUSTRIES, locations=LOCATIONS)


@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    from scraper import scrape_leads
    data = request.json or {}
    industry = data.get('industry', '').strip()
    location = data.get('location', '').strip()
    count = max(1, min(int(data.get('count', 20)), 50))

    if not industry or not location:
        return jsonify({'error': 'Industry and location are required'}), 400

    leads = scrape_leads(industry, location, count)
    return jsonify({'leads': leads, 'count': len(leads)})


@app.route('/api/validate-emails', methods=['POST'])
def api_validate_emails():
    from email_validator_util import validate_leads_emails
    data = request.json or {}
    leads = data.get('leads', [])
    if not leads:
        return jsonify({'leads': []})
    validated = validate_leads_emails(leads)
    return jsonify({'leads': validated})


@app.route('/api/export/csv', methods=['POST'])
def api_export_csv():
    from exporter import to_csv
    leads = (request.json or {}).get('leads', [])
    csv_bytes = to_csv(leads)
    return send_file(
        io.BytesIO(csv_bytes),
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
    pdf_bytes = to_pdf(leads, industry, location)
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype='application/pdf',
        as_attachment=True,
        download_name='malcolm-leads.pdf',
    )


if __name__ == '__main__':
    app.run(port=3000, debug=True)
