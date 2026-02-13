#!/usr/bin/env python3
"""
UCC Filings Scraper
UCC (Uniform Commercial Code) filings show equipment purchases
= Growing companies that need more coverage

Many states have free UCC search portals.
"""

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
from supabase import create_client
import os

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4bWNsbnZkaW1ibmt1emtkbnllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAxOTM1NzIsImV4cCI6MjA4NTc2OTU3Mn0.mkhALhXeDzgCzm4GvYCZq6rvYnf25U56HI6521MT_mc')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}

# State UCC portals
UCC_PORTALS = {
    'TX': 'https://direct.sos.state.tx.us/UCC/UCC-FS.asp',
    'GA': 'https://ecorp.sos.ga.gov/UCCSearch',
    'FL': 'https://ccfcorp.dos.state.fl.us/ucc/ucc_search.html',
    # Add more as needed
}

EQUIPMENT_KEYWORDS = {
    'truck': 'Trucking',
    'trailer': 'Trucking',
    'tractor': 'Construction',
    'excavator': 'Construction',
    'crane': 'Construction',
    'forklift': 'Warehouse',
    'vehicle': 'Auto Services',
    'equipment': 'Construction',
    'machinery': 'Manufacturing',
    'restaurant': 'Restaurant',
    'medical': 'Medical',
}


def determine_industry(collateral_desc):
    """Determine industry from UCC collateral description"""
    desc_lower = collateral_desc.lower()
    
    for keyword, industry in EQUIPMENT_KEYWORDS.items():
        if keyword in desc_lower:
            return industry
    
    return 'General Business'


def scrape_texas_ucc(days_back=7):
    """Scrape Texas UCC filings"""
    leads = []
    
    # Texas SOS UCC search
    url = "https://direct.sos.state.tx.us/UCC/UCC-FS.asp"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Parse UCC filings
            # This would need form submission for actual search
            
    except Exception as e:
        print(f"[UCC-TX] Error: {e}")
    
    return leads


def scrape_ucc_filings(state, days_back=7):
    """Generic UCC scraper"""
    leads = []
    
    if state == 'TX':
        return scrape_texas_ucc(days_back)
    
    # For other states, check if we have a portal
    portal = UCC_PORTALS.get(state)
    if not portal:
        return leads
    
    try:
        response = requests.get(portal, headers=HEADERS, timeout=30)
        # Parse based on state-specific format
        
    except Exception as e:
        print(f"[UCC-{state}] Error: {e}")
    
    return leads


def run_ucc_scraper(states=None):
    """Run UCC scraper"""
    if states is None:
        states = ['TX', 'GA', 'FL']
    
    print("=" * 60)
    print(f"UCC Filings Scraper - {datetime.now()}")
    print("=" * 60)
    
    all_leads = []
    
    for state in states:
        leads = scrape_ucc_filings(state)
        all_leads.extend(leads)
        time.sleep(2)
    
    print(f"Total: {len(all_leads)} leads")
    return all_leads


if __name__ == '__main__':
    run_ucc_scraper()
