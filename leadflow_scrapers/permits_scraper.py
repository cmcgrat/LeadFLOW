#!/usr/bin/env python3
"""
Building Permits Scraper
Companies pulling permits = need builders risk, GL, WC

Many cities publish permit data for free.
Focus on major metros in target states.
"""

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
import json
from supabase import create_client
import os

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4bWNsbnZkaW1ibmt1emtkbnllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAxOTM1NzIsImV4cCI6MjA4NTc2OTU3Mn0.mkhALhXeDzgCzm4GvYCZq6rvYnf25U56HI6521MT_mc')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}

# Cities with open data portals
CITY_PORTALS = {
    # Texas
    'Houston': {
        'state': 'TX',
        'url': 'https://cohgis-mycity.opendata.arcgis.com/datasets/building-permits/api',
        'type': 'arcgis'
    },
    'Dallas': {
        'state': 'TX', 
        'url': 'https://www.dallasopendata.com/resource/building-permits.json',
        'type': 'socrata'
    },
    'Austin': {
        'state': 'TX',
        'url': 'https://data.austintexas.gov/resource/3syk-w9eu.json',
        'type': 'socrata'
    },
    # Georgia
    'Atlanta': {
        'state': 'GA',
        'url': 'https://gis.atlantaga.gov/portal/rest/services',
        'type': 'arcgis'
    },
    # Tennessee
    'Nashville': {
        'state': 'TN',
        'url': 'https://data.nashville.gov/resource/building-permits.json',
        'type': 'socrata'
    },
    # Arkansas
    'Little Rock': {
        'state': 'AR',
        'url': None,  # No open data portal
        'type': None
    },
}


def scrape_socrata_permits(city, config, days_back=7):
    """Scrape permits from Socrata open data portal"""
    leads = []
    
    if not config.get('url'):
        return leads
    
    # Calculate date filter
    since_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00')
    
    url = config['url']
    params = {
        '$where': f"issue_date > '{since_date}'",
        '$limit': 100
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            for permit in data:
                contractor = permit.get('contractor_name') or permit.get('applicant_name', '')
                
                if not contractor or len(contractor) < 3:
                    continue
                
                # Skip individuals (look for LLC, Inc, etc)
                if not any(x in contractor.upper() for x in ['LLC', 'INC', 'CORP', 'CO', 'COMPANY', 'CONSTRUCTION', 'BUILDER']):
                    continue
                
                lead = {
                    'company_name': contractor.upper()[:200],
                    'industry': 'Construction',
                    'city': city,
                    'state': config['state'],
                    'zip': permit.get('zip_code', ''),
                    'phone': permit.get('contractor_phone', ''),
                    'email': '',
                    'signal_type': 'building_permit',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'source': 'permits_scraper',
                    'source_id': f"PERMIT-{city}-{permit.get('permit_number', hash(contractor) % 10000000)}",
                    'employees_estimated': '5-10',
                    'priority': 'HIGH',
                    'score': 85,
                    'lead_type': 'likely_uninsured',
                    'stage': 'new',
                    'owner': 'Unassigned',
                }
                
                leads.append(lead)
        
        print(f"[Permits] Found {len(leads)} contractors in {city}")
        
    except Exception as e:
        print(f"[Permits-{city}] Error: {e}")
    
    return leads


def scrape_permits(cities=None, days_back=7):
    """Scrape building permits from multiple cities"""
    if cities is None:
        cities = list(CITY_PORTALS.keys())
    
    all_leads = []
    
    for city in cities:
        config = CITY_PORTALS.get(city, {})
        
        if config.get('type') == 'socrata':
            leads = scrape_socrata_permits(city, config, days_back)
            all_leads.extend(leads)
        
        time.sleep(2)
    
    return all_leads


def run_permits_scraper(states=None):
    """Run permits scraper for states"""
    print("=" * 60)
    print(f"Building Permits Scraper - {datetime.now()}")
    print("=" * 60)
    
    # Filter cities by state if specified
    if states:
        cities = [city for city, config in CITY_PORTALS.items() if config['state'] in states]
    else:
        cities = list(CITY_PORTALS.keys())
    
    leads = scrape_permits(cities)
    
    print(f"Total: {len(leads)} leads")
    return leads


if __name__ == '__main__':
    import sys
    states = sys.argv[1].split(',') if len(sys.argv) > 1 else None
    run_permits_scraper(states)
