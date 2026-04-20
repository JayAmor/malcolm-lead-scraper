import re

import dns.resolver


# Reduced from 5s: on Render's DNS each lookup was hitting the full timeout,
# causing 50 leads * 5s = 250s sequential time -- blowing past gunicorn's 120s
# worker timeout. 3s is still generous for a legitimate MX record lookup.
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
    hosts (including Render), so we skip it -- MX check is the reliable signal.
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
    Validate all lead emails concurrently using gevent.pool.Pool.

    WHY CONCURRENT: gunicorn runs with --worker-class gevent, so
    monkey.patch_all() is active. Sequential DNS lookups under gevent are
    cooperative -- each blocks until the MX query returns or times out.
    With 50 leads at 3s timeout each, sequential = up to 150s, which
    exceeds the 120s gunicorn worker timeout. gunicorn kills the worker
    via gevent.GreenletExit (a BaseException), which bypasses all
    'except Exception' handlers and crashes the request.

    Using gevent.pool.Pool collapses N * 3s into ~3s regardless of lead
    count, because all DNS lookups yield to the gevent hub simultaneously.

    Falls back to sequential if gevent is not available (local dev without
    gunicorn).
    """
    if not leads:
        return []

    def _validate_one(lead):
        email = lead.get('email', '')
        if not email:
            lead['email_valid'] = None
        else:
            status = validate_email(email)
            lead['email_valid'] = status
            if status == 'invalid':
                lead['email'] = ''
        return lead

    try:
        from gevent.pool import Pool
        pool = Pool(20)  # 20 concurrent DNS lookups is plenty; DNS is cheap I/O
        validated = list(pool.map(_validate_one, leads))
    except ImportError:
        # gevent not available -- fall back to sequential (local dev server)
        validated = [_validate_one(lead) for lead in leads]

    return [lead for lead in validated if lead.get('email')]
