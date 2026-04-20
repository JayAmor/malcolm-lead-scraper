import re
import smtplib

import dns.resolver


def _check_mx(domain):
    try:
        records = dns.resolver.resolve(domain, 'MX', lifetime=5)
        return bool(records)
    except Exception:
        return False


def _smtp_verify(email, domain):
    """Returns True (valid), False (invalid), or None (inconclusive — server blocked)."""
    try:
        records = dns.resolver.resolve(domain, 'MX', lifetime=5)
        mx = sorted(records, key=lambda r: r.preference)[0].exchange.to_text().rstrip('.')
        with smtplib.SMTP(timeout=8) as server:
            server.connect(mx, 25)
            server.helo('mail.verify-check.com')
            server.mail('check@verify-check.com')
            code, _ = server.rcpt(email)
            return code == 250
    except Exception:
        return None


def validate_email(email):
    """
    Returns 'valid', 'invalid', or 'unverified'.
    'unverified' means MX records exist but SMTP verification was blocked —
    the email domain is real but the mailbox couldn't be confirmed.
    """
    if not email or '@' not in email:
        return 'invalid'
    if not re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email):
        return 'invalid'

    domain = email.split('@')[1].lower()

    if not _check_mx(domain):
        return 'invalid'

    result = _smtp_verify(email, domain)
    if result is True:
        return 'valid'
    elif result is False:
        return 'invalid'
    return 'unverified'  # MX ok, SMTP blocked — likely real


def validate_leads_emails(leads):
    """
    Validate all lead emails sequentially (no threading — gevent-safe).
    Removes leads whose email fails validation.
    """
    result = []
    for lead in leads:
        email = lead.get('email', '')
        if not email:
            lead['email_valid'] = None
        else:
            status = validate_email(email)
            lead['email_valid'] = status
            if status == 'invalid':
                lead['email'] = ''
        if lead.get('email'):
            result.append(lead)
    return result
