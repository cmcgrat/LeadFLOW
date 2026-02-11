#!/usr/bin/env python3
"""
OSHA Violations Scraper - Free Federal Data
https://www.osha.gov/pls/imis/industry.html

Companies with OSHA violations = coverage gap opportunities
- They need better WC coverage
- They need safety consulting
- Carriers may non-renew them
"""

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import time
from supabase import create_client
import os

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4bWNsbnZkaW1ibmt1emtkbnllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAxOTM1NzIsImV4cCI6MjA4NTc2OTU3Mn0.mkhALhXeDzgCzm4GvYCZq6rvYnf25U56HI6521MT_mc')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}

# SIC codes to industries
SIC_TO_INDUSTRY = {
    '15': 'Construction', '16': 'Construction', '17': 'Construction',
    '42': 'Trucking', '44': 'Trucking', '45': 'Trucking',
    '13': 'Oilfield',
    '20': 'Food Services', '58': 'Restaurant',
    '28': 'Chemical', '29': 'Chemical',
    '30': 'Manufacturing', '31': 'Manufacturing', '32': 'Manufacturing',
    '33': 'Manufacturing', '34': 'Manufacturing', '35': 'Manufacturing',
    '36': 'Manufacturing', '37': 'Manufacturing',
    '50': 'Wholesale', '51': 'Wholesale',
    '52': 'Retail', '53': 'Retail', '54': 'Retail', '55': 'Auto Services',
    '70': 'Staffing', '72': 'Cleaning', '73': 'Staffing',
    '76': 'Electrical', '78': 'Landscaping',
    '80': 'Medical',
}

STATE_FIPS = {
    'TX': '48', 'AR': '05', 'GA': '13', 'TN': '47',
    'OK': '40', 'LA': '22', 'AL': '01', 'MS': '28',
    'FL': '12', 'NC': '37', 'SC': '45', 'KY': '21',
}


def get_osha_violations_by_state(state, days_back=30):
    """
    Get recent OSHA violations for a state
    Uses OSHA's public enforcement data
    """
    leads = []
    
    # OSHA enforcement search
    # This URL searches for recent inspections
    base_url = "https://www.osha.gov/pls/imis/establishment.inspection_list"
    
    params = {
        'p_logger': '1',
        'State': state,
        'owner': '',
        'startmonth': (datetime.now() - timedelta(days=days_back)).strftime('%m'),
        'startday': (datetime.now() - timedelta(days=days_back)).strftime('%d'),
        'startyear': (datetime.now() - timedelta(days=days_back)).strftime('%Y'),
        'endmonth': datetime.now().strftime('%m'),
        'endday': datetime.now().strftime('%d'),
        'endyear': datetime.now().strftime('%Y'),
    }
    
    try:
        response = requests.get(base_url, params=params, headers=HEADERS, timeout=30)
        
        if response.status_code != 200:
            print(f"[OSHA] Error for {state}: {response.status_code}")
            return leads
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find inspection table
        table = soup.find('table', {'class': 'table'})
        if not table:
            tables = soup.find_all('table')
            for t in tables:
                if 'Establishment' in t.get_text():
                    table = t
                    break
        
        if not table:
            print(f"[OSHA] No data table found for {state}")
            return leads
        
        rows = table.find_all('tr')[1:]  # Skip header
        
        for row in rows[:50]:  # Limit to 50
            try:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                
                company_name = cells[0].get_text(strip=True)
                city = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                
                if not company_name or len(company_name) < 3:
                    continue
                
                # Determine industry from SIC if available
                industry = 'Manufacturing'  # Default for OSHA violations
                sic_match = re.search(r'SIC:\s*(\d{2})', row.get_text())
                if sic_match:
                    sic = sic_match.group(1)
                    industry = SIC_TO_INDUSTRY.get(sic, 'Manufacturing')
                
                lead = {
                    'company_name': company_name.upper()[:200],
                    'industry': industry,
                    'city': city,
                    'state': state,
                    'zip': '',
                    'phone': '',
                    'email': '',
                    'signal_type': 'osha_violation',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'source': 'osha_scraper',
                    'source_id': f"OSHA-{state}-{hash(company_name) % 10000000}",
                    'employees_estimated': '10-25',
                    'priority': 'HIGH',
                    'score': 80,
                    'lead_type': 'coverage_gap',
                    'stage': 'new',
                    'owner': 'Unassigned',
                }
                
                leads.append(lead)
                
            except Exception as e:
                continue
        
        print(f"[OSHA] Found {len(leads)} violations in {state}")
        
    except Exception as e:
        print(f"[OSHA] Error: {e}")
    
    return leads


def run_osha_scraper(states=None):
    """Run OSHA scraper for all states"""
    if states is None:
        states = ['TX', 'AR', 'GA', 'TN', 'OK', 'LA']
    
    print("=" * 60)
    print(f"OSHA Scraper - {datetime.now()}")
    print("=" * 60)
    
    all_leads = []
    
    for state in states:
        leads = get_osha_violations_by_state(state)
        all_leads.extend(leads)
        time.sleep(2)
    
    print(f"Total: {len(all_leads)} leads")
    return all_leads


if __name__ == '__main__':
    import sys
    states = sys.argv[1].split(',') if len(sys.argv) > 1 else None
    run_osha_scraper(states)
