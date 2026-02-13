#!/usr/bin/env python3
"""
Contractor License Scraper
Newly licensed contractors = need GL, WC, auto, bonds

Most states have public license lookup portals.
"""

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
import re
from supabase import create_client
import os

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4bWNsbnZkaW1ibmt1emtkbnllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAxOTM1NzIsImV4cCI6MjA4NTc2OTU3Mn0.mkhALhXeDzgCzm4GvYCZq6rvYnf25U56HI6521MT_mc')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}

# State license boards
LICENSE_BOARDS = {
    'TX': {
        'name': 'Texas Department of Licensing and Regulation',
        'url': 'https://www.tdlr.texas.gov/LicenseSearch/',
        'types': ['HVAC', 'Electrician', 'Plumber']
    },
    'GA': {
        'name': 'Georgia Secretary of State',
        'url': 'https://verify.sos.ga.gov/',
        'types': ['Contractor', 'Electrician', 'Plumber']
    },
    'TN': {
        'name': 'Tennessee Board for Licensing Contractors',
        'url': 'https://verify.tn.gov/',
        'types': ['Contractor']
    },
    'AR': {
        'name': 'Arkansas Contractors Licensing Board',
        'url': 'https://www.aclb.arkansas.gov/verify/',
        'types': ['Contractor']
    },
    'LA': {
        'name': 'Louisiana State Licensing Board for Contractors',
        'url': 'https://www.lslbc.louisiana.gov/contractor-search/',
        'types': ['Contractor']
    },
    'OK': {
        'name': 'Oklahoma Construction Industries Board',
        'url': 'https://cib.ok.gov/verify-license',
        'types': ['Contractor', 'Electrician', 'Plumber']
    },
}

LICENSE_TYPE_TO_INDUSTRY = {
    'contractor': 'Construction',
    'general contractor': 'Construction',
    'building contractor': 'Construction',
    'roofing': 'Construction',
    'hvac': 'Plumbing',
    'air conditioning': 'Plumbing',
    'plumb': 'Plumbing',
    'electric': 'Electrical',
}


def determine_industry(license_type):
    """Determine industry from license type"""
    type_lower = license_type.lower()
    
    for keyword, industry in LICENSE_TYPE_TO_INDUSTRY.items():
        if keyword in type_lower:
            return industry
    
    return 'Construction'


def scrape_texas_licenses(days_back=7):
    """Scrape Texas TDLR for new licenses"""
    leads = []
    
    # TDLR has a public search portal
    # Would need to implement form submission for actual search
    
    return leads


def scrape_georgia_licenses(days_back=7):
    """Scrape Georgia SOS for new contractor licenses"""
    leads = []
    
    url = "https://verify.sos.ga.gov/verification/Search.aspx"
    
    try:
        # Would need to implement actual search
        pass
        
    except Exception as e:
        print(f"[License-GA] Error: {e}")
    
    return leads


def scrape_contractor_licenses(state, days_back=7):
    """Scrape contractor licenses for a state"""
    leads = []
    
    if state not in LICENSE_BOARDS:
        return leads
    
    board = LICENSE_BOARDS[state]
    
    # State-specific implementations
    if state == 'TX':
        return scrape_texas_licenses(days_back)
    elif state == 'GA':
        return scrape_georgia_licenses(days_back)
    
    # Generic approach for other states
    # Most require form submission
    
    return leads


def run_license_scraper(states=None):
    """Run license scraper for all states"""
    if states is None:
        states = list(LICENSE_BOARDS.keys())
    
    print("=" * 60)
    print(f"Contractor License Scraper - {datetime.now()}")
    print("=" * 60)
    
    all_leads = []
    
    for state in states:
        if state in LICENSE_BOARDS:
            print(f"[{state}] Scraping {LICENSE_BOARDS[state]['name']}...")
            leads = scrape_contractor_licenses(state)
            all_leads.extend(leads)
            time.sleep(2)
    
    print(f"Total: {len(all_leads)} leads")
    return all_leads


if __name__ == '__main__':
    import sys
    states = sys.argv[1].split(',') if len(sys.argv) > 1 else None
    run_license_scraper(states)
