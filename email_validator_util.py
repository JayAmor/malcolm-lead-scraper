import re

import dns.resolver

_MX_LIFETIME = 3


def _check_mx(domain):
    try:
        records = dns.resolver.resolve(domain, 'MX', lifetime=_MX_LIFETIME)
        return bool(records)
    except Exception:
        return False


def validate_email(email):
    """
    Returns 'valid' or 'invalid'.
    Checks format + MX record existence. SMTP port 25 is blocked on most cloud
    hosts (including Render), so we skip it — MX check is the reliable signal.
    """
    if not email or '@' not in email:
        return 'invalid'
    if not re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email):
        return 'invalid'
    domain = email.split('@')[1].lower()
    return 'valid' if _check_mx(domain) else 'invalid'


def validate_leads_emails(leads):
    """
    Validate all lead emails concurrently using gevent.pool.Pool.
    Each lead gets a hard 4s per-greenlet timeout so a hung DNS lookup
    can never push the total past gunicorn's 120s worker timeout.
    Falls back to sequential on local dev without gevent.
    """
    if not leads:
        return []

    def _validate_one(lead):
        email = lead.get('email', '')
        if not email:
            lead['email_valid'] = None
            return lead

        status = 'unverified'
        try:
            try:
                from gevent import Timeout as _GT
                with _GT(4):
                    status = validate_email(email)
            except ImportError:
                status = validate_email(email)
        except BaseException:
            # BaseException catches gevent.Timeout (which isn't an Exception subclass)
            status = 'unverified'

        lead['email_valid'] = status
        if status == 'invalid':
            lead['email'] = ''
        return lead

    try:
        from gevent.pool import Pool
        pool = Pool(20)
        validated = list(pool.map(_validate_one, leads))
    except ImportError:
        validated = [_validate_one(lead) for lead in leads]

    return [lead for lead in validated if lead.get('email')]
