import re
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS

SKIP_DOMAINS = [
    'yelp.com', 'yellowpages.com', 'bbb.org', 'facebook.com',
    'google.com', 'linkedin.com', 'instagram.com', 'twitter.com',
    'angi.com', 'homeadvisor.com', 'thumbtack.com', 'angieslist.com',
    'mapquest.com', 'tripadvisor.com', 'manta.com', 'superpages.com',
    'whitepages.com', 'foursquare.com', 'houzz.com', 'porch.com',
    'amazon.com', 'bing.com', 'wikipedia.org', 'reddit.com', 'quora.com',
    'indeed.com', 'glassdoor.com', 'nextdoor.com', 'craigslist.org',
    'realtor.com', 'zillow.com', 'redfin.com', 'trulia.com',
    'healthgrades.com', 'zocdoc.com', 'vitals.com', 'yelp.com',
    'angieslist.com', 'thumbtack.com', 'buildium.com',
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
        return any(skip in domain for skip in SKIP_DOMAINS)
    except Exception:
        return True


def scrape_leads(industry, location, count):
    city = location.split(',')[0].strip()
    state = location.split(',')[1].strip() if ',' in location else ''

    queries = [
        f"{industry} {city}",
        f"{industry} company {city} {state}",
        f"best {industry} {city} {state}",
        f"local {industry} {city}",
        f"{industry} near {city}",
    ]

    seen_urls = set()
    candidates = []

    try:
        ddgs = DDGS()
        for query in queries:
            if len(candidates) >= count * 3:
                break
            try:
                results = list(ddgs.text(query, max_results=20))
                for r in results:
                    url = r.get('href', '')
                    title = r.get('title', '')
                    if url and url not in seen_urls and not is_skip_domain(url):
                        seen_urls.add(url)
                        candidates.append((url, title))
                time.sleep(random.uniform(1.0, 2.0))
            except Exception:
                continue
    except Exception:
        return []

    if not candidates:
        return []

    leads = []
    lock_count = [0]

    def analyze_safe(url, title):
        result = analyze_website(url, title, industry, location)
        return result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(analyze_safe, url, title): url
            for url, title in candidates[: count * 2]
        }
        for future in as_completed(futures):
            if lock_count[0] >= count:
                break
            result = future.result()
            if result:
                leads.append(result)
                lock_count[0] += 1

    quality_order = {'High': 0, 'Moderate': 1, 'Low': 2}
    leads.sort(key=lambda x: quality_order.get(x.get('quality', 'Low'), 2))
    return leads[:count]


def analyze_website(url, title, industry, location):
    try:
        resp = requests.get(url, headers=get_headers(), timeout=10, allow_redirects=True)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, 'lxml')
        html = resp.text

        business_name = extract_business_name(soup, title, url)
        email = extract_email(soup, html)
        phone = extract_phone(soup, html)
        has_blog = check_blog(soup)
        contact_name = extract_contact_name(soup)

        notes = []
        if not has_blog:
            notes.append('No blog detected — content gap opportunity')
        else:
            notes.append('Blog present — assess content quality')
        if not email:
            notes.append('No public email found')
        if not phone:
            notes.append('No phone found on site')

        return {
            'business_name': business_name,
            'website': url,
            'city_state': location,
            'industry': industry,
            'contact_name': contact_name or '',
            'email': email or '',
            'email_valid': None,
            'phone': phone or '',
            'quality': classify_lead(has_blog, email, phone, contact_name),
            'notes': '; '.join(notes),
        }
    except Exception:
        return None


def extract_business_name(soup, title, url):
    for meta_prop in ['og:site_name', 'og:title']:
        tag = soup.find('meta', property=meta_prop)
        if tag and tag.get('content', '').strip():
            return tag['content'].strip()[:80]

    h1 = soup.find('h1')
    if h1:
        t = h1.get_text(strip=True)
        if t:
            return t[:80]

    page_title = soup.find('title')
    if page_title:
        t = re.sub(r'\s*[\|\-–|]\s*.+$', '', page_title.get_text(strip=True)).strip()
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
    blog_kw = ['/blog', '/news', '/articles', '/insights', '/resources', '/posts', '/updates']
    for link in soup.find_all('a', href=True):
        href = link['href'].lower()
        if any(kw in href for kw in blog_kw):
            return True
    nav = soup.find(['nav', 'header'])
    if nav:
        nav_text = nav.get_text().lower()
        if any(kw.strip('/') in nav_text for kw in blog_kw):
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
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def classify_lead(has_blog, email, phone, contact_name):
    score = 0
    if not has_blog:
        score += 3  # Content gap = opportunity for ReadTomato
    if email:
        score += 2
    if phone:
        score += 1
    if contact_name:
        score += 1
    if score >= 5:
        return 'High'
    elif score >= 3:
        return 'Moderate'
    return 'Low'
