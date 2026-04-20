import queue
import random
import re
import threading
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS

SKIP_DOMAINS = [
    'yelp.com', 'yellowpages.com', 'bbb.org', 'facebook.com',
    'google.com', 'linkedin.com', 'instagram.com', 'twitter.com',
    'angi.com', 'homeadvisor.com', 'thumbtack.com', 'angieslist.com',
    'mapquest.com', 'tripadvisor.com', 'manta.com', 'superpages.com',
    'whitepages.com', 'foursquare.com', 'houzz.com', 'porch.com',
    'amazon.com', 'bing.com', 'wikipedia.org', 'reddit.com', 'quora.com',
    'indeed.com', 'glassdoor.com', 'nextdoor.com', 'craigslist.org',
    'realtor.com', 'zillow.com', 'redfin.com', 'trulia.com',
    'healthgrades.com', 'zocdoc.com', 'vitals.com',
]

# Manus methodology: reject generic inboxes and free email domains
_GENERIC_LOCAL = {
    'info', 'contact', 'admin', 'support', 'hello', 'team', 'sales',
    'marketing', 'noreply', 'no-reply', 'webmaster', 'office', 'mail',
    'enquiries', 'enquiry', 'general', 'service', 'services', 'help',
    'billing', 'reception', 'inquiries', 'inquiry', 'privacy',
}
_FREE_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'live.com', 'msn.com', 'ymail.com', 'protonmail.com',
}
# National chains / enterprises to skip — SMB focus only
_ENTERPRISE_KEYWORDS = [
    'terminix', 'orkin', 'aptive', 'rollins', 'ehrlich', 'rentokil',
    'roto-rooter', 'mr. rooter', 'mr rooter',
    'mr. electric', 'mister sparky', 'mr electric',
    'carrier', 'trane', 'lennox', 'rheem', 'york',
    'century 21', 'keller williams', 're/max', 'remax', 'coldwell banker',
    'aspen dental', 'pacific dental', 'heartland dental',
    'laseraway', 'ideal image', 'skinspirit',
    'brookdale', 'sunrise senior', 'atria senior',
]

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }


def is_skip_domain(url):
    try:
        domain = urlparse(url).netloc.lower()
        return any(s in domain for s in SKIP_DOMAINS)
    except Exception:
        return True


# ── Candidate gathering ────────────────────────────────────────────────────────

def get_candidates(industry, location, count):
    """
    Returns list of (url, title, prefetched) tuples.
    Tries Yellow Pages first for structured data, then fills from DuckDuckGo.
    prefetched dict carries YP data (name, phone, city_state) so we don't
    re-scrape info we already have.
    """
    seen = set()
    candidates = []

    ddg = _search_duckduckgo(industry, location, count * 3)
    for url, title in ddg:
        if url not in seen and not is_skip_domain(url):
            seen.add(url)
            candidates.append((url, title, {}))

    return candidates


def _search_duckduckgo(industry, location, count):
    city = location.split(',')[0].strip()
    state = location.split(',')[1].strip() if ',' in location else ''

    queries = [
        f'{industry} {city}',
        f'{industry} company {city} {state}',
        f'best {industry} {city} {state}',
        f'local {industry} {city}',
        f'{industry} near {city}',
    ]

    results = []
    seen = set()

    try:
        ddgs = DDGS()
        for query in queries:
            if len(results) >= count:
                break
            try:
                for r in ddgs.text(query, max_results=20):
                    url = r.get('href', '')
                    if url and url not in seen:
                        seen.add(url)
                        results.append((url, r.get('title', '')))
                time.sleep(random.uniform(0.5, 1.0))
            except Exception:
                continue
    except Exception:
        pass

    return results


# ── Streaming generator ────────────────────────────────────────────────────────

def scrape_leads_stream(industries, location, count):
    """
    Generator that yields qualified lead dicts one by one as threads complete,
    then yields {'_done': True} as a sentinel.
    industries is a list of one or more industry strings.
    """
    if isinstance(industries, str):
        industries = [industries]

    seen = set()
    all_candidates = []  # (url, title, prefetched, industry_label)

    per = max(5, count // len(industries))
    for industry in industries:
        for url, title, prefetched in get_candidates(industry, location, per):
            if url not in seen:
                seen.add(url)
                all_candidates.append((url, title, prefetched, industry))

    if not all_candidates:
        yield {'_done': True, 'total': 0}
        return

    result_queue = queue.Queue()
    work_items = all_candidates[: count * 2]

    def worker(url, title, prefetched, industry_label):
        try:
            result = analyze_website(url, title, industry_label, location, prefetched)
            result_queue.put(result)
        except Exception:
            result_queue.put(None)

    threads = []
    for url, title, prefetched, industry_label in work_items:
        t = threading.Thread(target=worker, args=(url, title, prefetched, industry_label), daemon=True)
        t.start()
        threads.append(t)

    yielded = 0
    received = 0

    while received < len(threads) and yielded < count:
        try:
            result = result_queue.get(timeout=20)
            received += 1
            if result and result.get('email'):
                yield result
                yielded += 1
        except queue.Empty:
            break

    yield {'_done': True, 'total': yielded}


# ── Website analysis ───────────────────────────────────────────────────────────

def analyze_website(url, title, industry, location, prefetched=None):
    try:
        resp = requests.get(url, headers=get_headers(), timeout=7, allow_redirects=True)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, 'lxml')
        html = resp.text
        pre = prefetched or {}

        business_name = pre.get('name') or extract_business_name(soup, title, url)
        email = extract_email(soup, html)
        phone = pre.get('phone') or extract_phone(soup, html)
        has_blog = check_blog(soup)
        contact_name = extract_contact_name(soup)
        has_google_ads = detect_google_ads(html)
        city_state = pre.get('city_state') or location

        # Skip national chains / enterprises — SMB focus only
        if _is_enterprise(business_name, url):
            return None

        # Check /contact and /about only if we're still missing email or contact
        if not email or not contact_name:
            sub = _try_subpages(url)
            email = email or sub.get('email')
            phone = phone or sub.get('phone')
            contact_name = contact_name or sub.get('contact_name')

        if not email:
            return None

        # Apply Manus email quality rules: free domain and domain-mismatch = discard
        ok, email_flag = _quality_email(email, url)
        if not ok:
            return None

        notes = []
        if email_flag == 'generic':
            notes.append('Generic inbox (info@/contact@) — look for named contact')
        if not has_blog:
            notes.append('No blog — content gap opportunity')
        else:
            notes.append('Blog present — assess quality')
        if has_google_ads:
            notes.append('Running Google Ads — actively investing in marketing')

        return {
            'business_name': business_name,
            'website': url,
            'city_state': city_state,
            'industry': industry,
            'contact_name': contact_name or '',
            'email': email,
            'email_valid': None,
            'phone': phone or '',
            'quality': classify_lead(has_blog, email, phone, contact_name, has_google_ads),
            'has_google_ads': has_google_ads,
            'notes': '; '.join(notes),
        }
    except Exception:
        return None


def _try_subpages(base_url):
    """Check /contact and /about pages for email/phone/contact_name."""
    found = {}
    paths = ['/contact', '/contact-us', '/about', '/about-us', '/team']

    for path in paths:
        if found.get('email') and found.get('contact_name'):
            break
        try:
            resp = requests.get(
                urljoin(base_url, path), headers=get_headers(),
                timeout=3, allow_redirects=True,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'lxml')
                html = resp.text
                if not found.get('email'):
                    found['email'] = extract_email(soup, html)
                if not found.get('phone'):
                    found['phone'] = extract_phone(soup, html)
                if not found.get('contact_name'):
                    found['contact_name'] = extract_contact_name(soup)
        except Exception:
            continue

    return found


def detect_google_ads(html):
    patterns = [
        'pagead2.googlesyndication.com',
        'adsbygoogle',
        'google_ad_client',
        'doubleclick.net',
        'googleads.g.doubleclick.net',
        'googletag.js',
    ]
    lower = html.lower()
    return any(p in lower for p in patterns)


# ── Extraction helpers ─────────────────────────────────────────────────────────

def extract_business_name(soup, title, url):
    for prop in ['og:site_name', 'og:title']:
        tag = soup.find('meta', property=prop)
        if tag and tag.get('content', '').strip():
            return tag['content'].strip()[:80]

    h1 = soup.find('h1')
    if h1:
        t = h1.get_text(strip=True)
        if t:
            return t[:80]

    page_title = soup.find('title')
    if page_title:
        t = re.sub(r'\s*[\|\-–]\s*.+$', '', page_title.get_text(strip=True)).strip()
        if t:
            return t[:80]

    try:
        return urlparse(url).netloc.replace('www.', '')
    except Exception:
        return title[:80] if title else url


def extract_email(soup, html):
    for link in soup.find_all('a', href=re.compile(r'^mailto:', re.I)):
        email = link['href'].replace('mailto:', '').split('?')[0].strip().lower()
        if '@' in email and _valid_email(email):
            return email

    for el in soup.find_all(attrs={'data-email': True}):
        email = el['data-email'].strip().lower()
        if '@' in email and _valid_email(email):
            return email

    noise = {
        'example', 'domain', 'email', 'test', 'user', 'your', 'name',
        'sentry', 'noreply', 'no-reply', 'webmaster', 'wordpress',
        'wixpress', 'squarespace', 'shopify', 'privacy',
    }
    for email in re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', html):
        email = email.lower()
        if _valid_email(email) and not any(n in email for n in noise):
            return email

    return None


def _valid_email(email):
    return bool(re.match(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', email))


def _quality_email(email, website_url=None):
    """
    Applies Manus-style email quality rules:
    - Not a free email domain (gmail, yahoo, etc.)
    - Domain matches business website domain
    Generic local parts (info@, contact@) are flagged but not rejected —
    they're noted as lower quality so the user can decide.
    """
    if not email or '@' not in email:
        return False, 'invalid'
    local, domain = email.lower().split('@', 1)
    if domain in _FREE_DOMAINS:
        return False, 'free_domain'
    if website_url:
        try:
            site_domain = urlparse(website_url).netloc.lower().replace('www.', '')
            email_domain = domain.replace('www.', '')
            # Only reject if domains share no common root at all
            # e.g. reject info@gmail.com for beesplumbing.com, but keep
            # sales@beesplumbing.com for beesplumbingandheating.com
            site_root = site_domain.split('.')[0] if site_domain else ''
            email_root = email_domain.split('.')[0] if email_domain else ''
            if site_root and email_root and site_root not in email_root and email_root not in site_root:
                return False, 'domain_mismatch'
        except Exception:
            pass
    if local in _GENERIC_LOCAL:
        return True, 'generic'   # Keep but flag
    return True, 'named'


def _is_enterprise(name, url):
    text = (name + ' ' + url).lower()
    return any(kw in text for kw in _ENTERPRISE_KEYWORDS)


def extract_phone(soup, html):
    for link in soup.find_all('a', href=re.compile(r'^tel:', re.I)):
        num = link['href'].replace('tel:', '').strip()
        if num:
            return num

    schema = soup.find(itemprop='telephone')
    if schema:
        return schema.get_text(strip=True)

    phones = re.findall(r'(?:\+1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}', html)
    if phones:
        return phones[0].strip()

    return None


def check_blog(soup):
    kws = ['/blog', '/news', '/articles', '/insights', '/resources', '/posts', '/updates']
    for link in soup.find_all('a', href=True):
        if any(k in link['href'].lower() for k in kws):
            return True
    nav = soup.find(['nav', 'header'])
    if nav:
        nav_text = nav.get_text().lower()
        if any(k.strip('/') in nav_text for k in kws):
            return True
    return False


def extract_contact_name(soup):
    text = soup.get_text(' ', strip=True)
    patterns = [
        r'(?:Owner|Founder|CEO|President|Manager|Director)[:\s,]+([A-Z][a-z]+ [A-Z][a-z]+)',
        r'([A-Z][a-z]+ [A-Z][a-z]+)[,\s]+(?:Owner|Founder|CEO|President|Manager|Director)',
        r'My name is ([A-Z][a-z]+ [A-Z][a-z]+)',
        r"I(?:'m| am) ([A-Z][a-z]+ [A-Z][a-z]+)[,\s]+(?:owner|founder|ceo|president)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None


def classify_lead(has_blog, email, phone, contact_name, has_google_ads=False):
    score = 0
    if not has_blog:
        score += 3   # Content gap = opportunity for ReadTomato
    if email:
        score += 2
    if phone:
        score += 1
    if contact_name:
        score += 1
    if has_google_ads:
        score += 1   # Already investing in marketing = higher capacity
    if score >= 6:
        return 'High'
    elif score >= 3:
        return 'Moderate'
    return 'Low'
