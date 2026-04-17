import random
import time
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

YP_BASE = 'https://www.yellowpages.com/search'

_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# Domains that are YP redirects or internal — not real business websites
_YP_INTERNAL = ('yellowpages.com', 'yp.com')


def _headers():
    return {
        'User-Agent': random.choice(_USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.yellowpages.com/',
    }


def search_yellow_pages(industry, location, max_results=30):
    """
    Scrape Yellow Pages for businesses. Returns a list of dicts:
      {name, phone, address, website, city_state}
    website may be empty if YP doesn't show a direct link.
    """
    city = location.split(',')[0].strip()
    state = location.split(',')[1].strip() if ',' in location else ''
    geo = f'{city}, {state}' if state else city

    url = f'{YP_BASE}?search_terms={quote_plus(industry)}&geo_location_terms={quote_plus(geo)}'

    try:
        resp = requests.get(url, headers=_headers(), timeout=15)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, 'lxml')
        results = []

        for listing in soup.select('.result'):
            parsed = _parse_listing(listing, location)
            if parsed:
                results.append(parsed)
            if len(results) >= max_results:
                break

        time.sleep(random.uniform(1.0, 2.0))
        return results

    except Exception:
        return []


def _parse_listing(tag, fallback_location):
    name_tag = tag.select_one('.business-name')
    if not name_tag:
        return None

    name = name_tag.get_text(strip=True)

    phone_tag = tag.select_one('.phone')
    phone = phone_tag.get_text(strip=True) if phone_tag else ''

    # Try to get the direct business website link
    website = ''
    for sel in ['a.track-visit-website', 'a[class*="website"]']:
        wt = tag.select_one(sel)
        if wt:
            href = wt.get('href', '')
            # Only accept direct external URLs; skip YP internal/redirect URLs
            if href.startswith('http') and not any(d in href for d in _YP_INTERNAL):
                website = href
                break

    # Address
    addr_parts = []
    street = tag.select_one('.street-address')
    locality = tag.select_one('.locality')
    if street:
        addr_parts.append(street.get_text(strip=True))
    city_state = locality.get_text(strip=True) if locality else fallback_location

    return {
        'name': name,
        'phone': phone,
        'website': website,
        'address': ', '.join(addr_parts) if addr_parts else '',
        'city_state': city_state or fallback_location,
    }
