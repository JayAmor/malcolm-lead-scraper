import re

import dns.resolver


def _check_mx(domain):
    try:
        records = dns.resolver.resolve(domain, 'MX', lifetime=5)
        return bool(records)
    except Exception:
        return False


def validate_email(email):
    """
    Returns 'valid', 'invalid', or 'unverified'.
    Checks format + MX record existence. SMTP port 25 is blocked on most cloud
    hosts (including Render), so we skip it — MX check is the reliable signal.
    'valid'     = format ok + MX records found
    'invalid'   = bad format or no MX records (domain can't receive email)
    """
    if not email or '@' not in email:
        return 'invalid'
    if not re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email):
        return 'invalid'

    domain = email.split('@')[1].lower()
    return 'valid' if _check_mx(domain) else 'invalid'


def validate_leads_emails(leads):
    """
    Validate all lead emails sequentially (gevent-safe, no threading).
    Removes leads whose email domain has no MX records.
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
