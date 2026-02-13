#!/usr/bin/env python3
"""
OpenCorporates Scraper - Free Business Formation Data
https://opencorporates.com/

OpenCorporates aggregates business registration data from all 50 states.
Free tier: 500 API calls/month
No key needed for basic web scraping
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

# State codes mapping
STATE_CODES = {
    'TX': 'us_tx', 'AR': 'us_ar', 'GA': 'us_ga', 'TN': 'us_tn',
    'OK': 'us_ok', 'LA': 'us_la', 'AL': 'us_al', 'MS': 'us_ms',
    'FL': 'us_fl', 'NC': 'us_nc', 'SC': 'us_sc', 'KY': 'us_ky',
    'MO': 'us_mo', 'KS': 'us_ks', 'NM': 'us_nm', 'AZ': 'us_az',
    'CO': 'us_co', 'CA': 'us_ca', 'OH': 'us_oh', 'PA': 'us_pa',
}

# Industry keywords
INDUSTRY_KEYWORDS = {
    'construction': 'Construction',
    'contractor': 'Construction',
    'builder': 'Construction',
    'roofing': 'Construction',
    'concrete': 'Construction',
    'plumbing': 'Plumbing',
    'hvac': 'Plumbing',
    'heating': 'Plumbing',
    'electric': 'Electrical',
    'trucking': 'Trucking',
    'transport': 'Trucking',
    'freight': 'Trucking',
    'logistics': 'Trucking',
    'hauling': 'Trucking',
    'oilfield': 'Oilfield',
    'energy': 'Oilfield',
    'petroleum': 'Oilfield',
    'drilling': 'Oilfield',
    'restaurant': 'Restaurant',
    'cafe': 'Restaurant',
    'food': 'Restaurant',
    'catering': 'Restaurant',
    'medical': 'Medical',
    'health': 'Medical',
    'dental': 'Medical',
    'clinic': 'Medical',
    'therapy': 'Medical',
    'landscap': 'Landscaping',
    'lawn': 'Landscaping',
    'tree': 'Landscaping',
    'cleaning': 'Cleaning',
    'janitorial': 'Cleaning',
    'maid': 'Cleaning',
    'auto': 'Auto Services',
    'mechanic': 'Auto Services',
    'body shop': 'Auto Services',
    'tire': 'Auto Services',
    'staffing': 'Staffing',
    'temp ': 'Staffing',
    'personnel': 'Staffing',
    'warehouse': 'Warehouse',
    'storage': 'Warehouse',
    'manufactur': 'Manufacturing',
    'machine': 'Manufacturing',
    'weld': 'Manufacturing',
    'metal': 'Manufacturing',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def determine_industry(company_name):
    """Determine industry from company name"""
    name_lower = company_name.lower()
    
    for keyword, industry in INDUSTRY_KEYWORDS.items():
        if keyword in name_lower:
            return industry
    
    return 'General Business'


def scrape_opencorporates(state, days_back=7):
    """
    Scrape OpenCorporates for new business formations
    """
    leads = []
    state_code = STATE_CODES.get(state, f'us_{state.lower()}')
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # OpenCorporates search URL
    base_url = f"https://opencorporates.com/companies/{state_code}"
    
    try:
        # Get the page
        response = requests.get(base_url, headers=HEADERS, timeout=30)
        
        if response.status_code != 200:
            print(f"[OpenCorp] Error for {state}: Status {response.status_code}")
            return leads
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find company listings
        companies = soup.find_all('a', class_='company_search_result')
        if not companies:
            companies = soup.find_all('li', class_='company')
        
        for company in companies[:100]:  # Limit to 100 per state
            try:
                # Get company name
                name_elem = company.find('a') if company.name != 'a' else company
                if not name_elem:
                    continue
                    
                company_name = name_elem.get_text(strip=True)
                
                if not company_name or len(company_name) < 3:
                    continue
                
                # Skip inactive/dissolved
                if 'dissolved' in company_name.lower() or 'inactive' in company_name.lower():
                    continue
                
                # Get company URL for more details
                company_url = name_elem.get('href', '')
                
                # Determine industry
                industry = determine_industry(company_name)
                
                # Create lead
                lead = {
                    'company_name': company_name.upper()[:200],  # Limit length
                    'industry': industry,
                    'city': '',
                    'state': state,
                    'zip': '',
                    'phone': '',
                    'email': '',
                    'signal_type': 'new_formation',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'source': 'opencorporates',
                    'source_id': f"OC-{state}-{hash(company_name) % 10000000}",
                    'employees_estimated': '1-5',
                    'priority': 'MEDIUM' if industry == 'General Business' else 'HIGH',
                    'score': 75 if industry == 'General Business' else 85,
                    'lead_type': 'likely_uninsured',
                    'stage': 'new',
                    'owner': 'Unassigned',
                }
                
                leads.append(lead)
                
            except Exception as e:
                print(f"[OpenCorp] Error parsing company: {e}")
                continue
        
        print(f"[OpenCorp] Found {len(leads)} companies in {state}")
        
    except Exception as e:
        print(f"[OpenCorp] Error fetching {state}: {e}")
    
    return leads


def scrape_state_sos_texas():
    """
    Scrape Texas SOS directly
    Texas Comptroller public data
    """
    leads = []
    
    # Texas Comptroller Franchise Tax Search
    url = "https://mycpa.cpa.state.tx.us/coa/coaSearchBtn"
    
    try:
        # Search for recent formations
        # This would need form submission - placeholder for now
        pass
        
    except Exception as e:
        print(f"[TX SOS] Error: {e}")
    
    return leads


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


def run_opencorporates_scraper(states=None):
    """Main function"""
    if states is None:
        states = ['TX', 'AR', 'GA', 'TN', 'OK', 'LA']
    
    print("=" * 60)
    print(f"OpenCorporates Scraper - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    existing_ids = get_existing_source_ids()
    print(f"Found {len(existing_ids)} existing leads")
    
    all_leads = []
    
    for state in states:
        print(f"\n[{state}] Scraping OpenCorporates...")
        leads = scrape_opencorporates(state)
        all_leads.extend(leads)
        time.sleep(3)  # Be nice
    
    print(f"\n" + "-" * 60)
    print(f"Total leads found: {len(all_leads)}")
    
    if all_leads:
        inserted, skipped = push_leads_to_supabase(all_leads, existing_ids)
        print(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")
    
    print("=" * 60)
    print("OpenCorporates scraper complete!")
    
    return all_leads


if __name__ == '__main__':
    import sys
    states = sys.argv[1].split(',') if len(sys.argv) > 1 else None
    run_opencorporates_scraper(states)
