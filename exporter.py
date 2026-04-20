import csv
import io
from datetime import datetime

from fpdf import FPDF

FIELDS = [
    ('business_name', 'Business Name'),
    ('website', 'Website'),
    ('city_state', 'City / State'),
    ('industry', 'Industry'),
    ('contact_name', 'Contact Name'),
    ('email', 'Email'),
    ('email_valid', 'Email Verified'),
    ('phone', 'Phone'),
    ('quality', 'Quality'),
    ('notes', 'Notes'),
]


def to_csv(leads):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[f[0] for f in FIELDS], extrasaction='ignore')
    writer.writeheader()
    for lead in leads:
        writer.writerow(lead)
    output.seek(0)
    return output.getvalue().encode('utf-8')


class LeadsPDF(FPDF):
    def __init__(self, subtitle):
        super().__init__()
        self.subtitle = subtitle

    def header(self):
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(224, 82, 82)
        self.cell(0, 8, 'Malcolm AI  Lead Report', align='C', new_x='LMARGIN', new_y='NEXT')
        self.set_font('Helvetica', '', 9)
        self.set_text_color(100, 116, 139)
        self.cell(0, 6, self.subtitle, align='C', new_x='LMARGIN', new_y='NEXT')
        self.ln(4)
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(100, 116, 139)
        stamp = datetime.now().strftime('%B %d, %Y %I:%M %p')
        self.cell(0, 8, f'Page {self.page_no()}  |  Generated {stamp}', align='C')


QUALITY_COLORS = {
    'High': (34, 197, 94),
    'Moderate': (234, 179, 8),
    'Low': (239, 68, 68),
}


def to_pdf(leads, industry, location):
    subtitle = f'{industry}  |  {location}  |  {len(leads)} Lead(s)'
    pdf = LeadsPDF(subtitle)
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    for i, lead in enumerate(leads, 1):
        # Card header
        pdf.set_fill_color(235, 235, 240)
        pdf.set_text_color(30, 30, 50)
        pdf.set_font('Helvetica', 'B', 11)
        pdf.cell(0, 9, f'{i}. {lead.get("business_name", "Unknown")}', fill=True,
                 new_x='LMARGIN', new_y='NEXT')

        # Quality badge
        q = lead.get('quality', 'Low')
        r, g, b = QUALITY_COLORS.get(q, (100, 116, 139))
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(r, g, b)
        pdf.cell(0, 6, f'  {q} Quality Lead', new_x='LMARGIN', new_y='NEXT')

        # Data fields
        ev = lead.get('email_valid') or ''
        ev_label = {'valid': 'Verified ✓', 'invalid': 'Invalid ✗', 'unverified': 'Unverified ?'}.get(ev, '')
        data = [
            ('Industry', lead.get('industry', '')),
            ('Location', lead.get('city_state', '')),
            ('Website', lead.get('website', '')),
            ('Phone', lead.get('phone', '')),
            ('Email', lead.get('email', '') + (f'  [{ev_label}]' if ev_label else '')),
            ('Contact', lead.get('contact_name', '')),
            ('Notes', lead.get('notes', '')),
        ]
        for label, value in data:
            if value:
                pdf.set_font('Helvetica', 'B', 8)
                pdf.set_text_color(80, 80, 100)
                pdf.cell(28, 5.5, f'{label}:')
                pdf.set_font('Helvetica', '', 8)
                pdf.set_text_color(40, 40, 60)
                pdf.multi_cell(0, 5.5, str(value))

        pdf.ln(5)

    return bytes(pdf.output())
