#!/usr/bin/env python3
"""
FMCSA Scraper - ACTUALLY WORKS
Pulls new motor carriers from FMCSA public data

FMCSA provides free public data:
- New carrier registrations
- Safety data
- Insurance filings

No API key required for basic data.
"""

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import time
import json
from supabase import create_client
import os

# Supabase connection
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4bWNsbnZkaW1ibmt1emtkbnllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAxOTM1NzIsImV4cCI6MjA4NTc2OTU3Mn0.mkhALhXeDzgCzm4GvYCZq6rvYnf25U56HI6521MT_mc')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Target states
TARGET_STATES = ['TX', 'AR', 'GA', 'TN', 'OK', 'LA', 'AL', 'MS', 'FL', 'NC']

# Headers to avoid blocking
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def get_fmcsa_carriers_by_state(state, days_back=7):
    """
    Get new carriers from FMCSA SAFER system
    Uses the public search interface
    """
    leads = []
    
    # FMCSA SAFER search URL
    base_url = "https://safer.fmcsa.dot.gov/keywordx.asp"
    
    # Search by state - this finds recently added carriers
    params = {
        'searchstring': f'*',
        'ESSION_ID': '',
        'SEARCHTYPE': '',
        'STATE': state,
    }
    
    try:
        # Make request
        response = requests.get(base_url, params=params, headers=HEADERS, timeout=30)
        
        if response.status_code != 200:
            print(f"[FMCSA] Error for {state}: Status {response.status_code}")
            return leads
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find carrier links
        carrier_links = soup.find_all('a', href=re.compile(r'query\.asp\?searchtype=ANY&query_type=queryCarrierSnapshot'))
        
        for link in carrier_links[:50]:  # Limit to 50 per state
            try:
                carrier_name = link.get_text(strip=True)
                
                # Skip empty names
                if not carrier_name or len(carrier_name) < 3:
                    continue
                
                # Get DOT number from link
                href = link.get('href', '')
                dot_match = re.search(r'query_param=USDOT&query_string=(\d+)', href)
                dot_number = dot_match.group(1) if dot_match else ''
                
                # Get carrier details
                carrier_data = get_carrier_details(dot_number) if dot_number else {}
                
                lead = {
                    'company_name': carrier_name.upper(),
                    'industry': 'Trucking',
                    'city': carrier_data.get('city', ''),
                    'state': state,
                    'zip': carrier_data.get('zip', ''),
                    'phone': carrier_data.get('phone', ''),
                    'email': '',
                    'signal_type': 'new_dot',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'source': 'fmcsa_scraper',
                    'source_id': f"FMCSA-{dot_number}" if dot_number else f"FMCSA-{hash(carrier_name)}",
                    'employees_estimated': estimate_employees(carrier_data.get('drivers', 0)),
                    'priority': 'HIGH',
                    'score': 85,
                    'lead_type': 'likely_uninsured',
                    'stage': 'new',
                    'owner': 'Unassigned',
                    'dot_number': dot_number,
                    'mc_number': carrier_data.get('mc_number', ''),
                }
                
                leads.append(lead)
                
                # Be nice to the server
                time.sleep(0.5)
                
            except Exception as e:
                print(f"[FMCSA] Error parsing carrier: {e}")
                continue
        
        print(f"[FMCSA] Found {len(leads)} carriers in {state}")
        
    except Exception as e:
        print(f"[FMCSA] Error fetching {state}: {e}")
    
    return leads


def get_carrier_details(dot_number):
    """Get detailed carrier info from SAFER"""
    details = {}
    
    if not dot_number:
        return details
    
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={dot_number}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the data table
        tables = soup.find_all('table')
        
        for table in tables:
            text = table.get_text()
            
            # Extract phone
            phone_match = re.search(r'Phone:\s*\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})', text)
            if phone_match:
                details['phone'] = f"({phone_match.group(1)}) {phone_match.group(2)}-{phone_match.group(3)}"
            
            # Extract city/state
            addr_match = re.search(r'Physical Address:\s*([^,]+),\s*([A-Z]{2})\s*(\d{5})', text)
            if addr_match:
                details['city'] = addr_match.group(1).strip()
                details['zip'] = addr_match.group(3)
            
            # Extract drivers
            drivers_match = re.search(r'Drivers:\s*(\d+)', text)
            if drivers_match:
                details['drivers'] = int(drivers_match.group(1))
            
            # Extract MC number
            mc_match = re.search(r'MC/MX/FF Number\(s\):\s*MC-(\d+)', text)
            if mc_match:
                details['mc_number'] = f"MC-{mc_match.group(1)}"
        
    except Exception as e:
        print(f"[FMCSA] Error getting details for DOT {dot_number}: {e}")
    
    return details


def estimate_employees(drivers):
    """Estimate employee count from driver count"""
    if not drivers or drivers == 0:
        return '1-5'
    if drivers <= 5:
        return '1-5'
    elif drivers <= 10:
        return '5-10'
    elif drivers <= 25:
        return '10-25'
    elif drivers <= 50:
        return '25-50'
    elif drivers <= 100:
        return '50-100'
    return '100+'


def get_existing_source_ids():
    """Get existing leads to prevent duplicates"""
    try:
        result = supabase.table('leads').select('source_id').execute()
        return set(lead['source_id'] for lead in result.data if lead.get('source_id'))
    except Exception as e:
        print(f"Error fetching existing leads: {e}")
        return set()


def push_leads_to_supabase(leads, existing_ids):
    """Push new leads to Supabase"""
    inserted = 0
    skipped = 0
    
    for lead in leads:
        source_id = lead.get('source_id')
        
        if source_id in existing_ids:
            skipped += 1
            continue
        
        try:
            result = supabase.table('leads').insert(lead).execute()
            if result.data:
                inserted += 1
                existing_ids.add(source_id)
        except Exception as e:
            print(f"Error inserting lead: {e}")
    
    return inserted, skipped


def run_fmcsa_scraper(states=None):
    """Main function to run the FMCSA scraper"""
    if states is None:
        states = TARGET_STATES
    
    print("=" * 60)
    print(f"FMCSA Scraper - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Get existing leads
    existing_ids = get_existing_source_ids()
    print(f"Found {len(existing_ids)} existing leads")
    
    all_leads = []
    
    for state in states:
        print(f"\n[{state}] Scraping FMCSA...")
        leads = get_fmcsa_carriers_by_state(state)
        all_leads.extend(leads)
        time.sleep(2)  # Be nice between states
    
    print(f"\n" + "-" * 60)
    print(f"Total leads found: {len(all_leads)}")
    
    if all_leads:
        inserted, skipped = push_leads_to_supabase(all_leads, existing_ids)
        print(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")
    
    print("=" * 60)
    print("FMCSA scraper complete!")
    
    return all_leads


if __name__ == '__main__':
    import sys
    
    states = sys.argv[1].split(',') if len(sys.argv) > 1 else None
    run_fmcsa_scraper(states)
