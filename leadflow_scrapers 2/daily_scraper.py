#!/usr/bin/env python3
"""
LeadFlow Daily Scraper Runner
Run all scrapers and push leads to Supabase

SCRAPERS INCLUDED:
1. FMCSA - New trucking companies (all states)
2. OpenCorporates - New business formations (all states)
3. OSHA - Workplace violations (all states)
4. Building Permits - Contractors in major cities
5. Contractor Licenses - Newly licensed contractors

Usage:
    python daily_scraper.py                    # Run all scrapers
    python daily_scraper.py --states TX,AR    # Specific states
    python daily_scraper.py --test            # Test mode
"""

import os
import sys
import argparse
from datetime import datetime
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from supabase import create_client

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ4bWNsbnZkaW1ibmt1emtkbnllIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAxOTM1NzIsImV4cCI6MjA4NTc2OTU3Mn0.mkhALhXeDzgCzm4GvYCZq6rvYnf25U56HI6521MT_mc')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DEFAULT_STATES = ['TX', 'AR', 'GA', 'TN', 'OK', 'LA']

ALL_STATES = [
    'TX', 'AR', 'GA', 'TN', 'OK', 'LA',
    'AL', 'MS', 'FL', 'NC', 'SC', 'KY',
    'MO', 'KS', 'NM', 'AZ', 'CO',
    'CA', 'OH', 'PA', 'IL', 'NY',
]


def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")


def get_existing_leads():
    try:
        result = supabase.table('leads').select('source_id').execute()
        return set(lead['source_id'] for lead in result.data if lead.get('source_id'))
    except Exception as e:
        log(f"Error fetching existing leads: {e}")
        return set()


def push_lead(lead, existing_ids, test_mode=False):
    source_id = lead.get('source_id')
    
    if source_id in existing_ids:
        return 'skipped'
    
    if test_mode:
        log(f"  [TEST] Would insert: {lead['company_name']}")
        return 'test'
    
    try:
        result = supabase.table('leads').insert(lead).execute()
        if result.data:
            existing_ids.add(source_id)
            return 'inserted'
    except Exception as e:
        log(f"  Error: {e}")
        return 'error'
    
    return 'error'


def push_leads(leads, existing_ids, test_mode=False):
    """Push multiple leads"""
    inserted = 0
    skipped = 0
    
    for lead in leads:
        result = push_lead(lead, existing_ids, test_mode)
        if result == 'inserted':
            inserted += 1
        elif result == 'skipped':
            skipped += 1
    
    return inserted, skipped


# ============================================
# SCRAPER RUNNERS
# ============================================

def run_fmcsa(states, existing_ids, test_mode):
    """FMCSA - New trucking companies"""
    log("=" * 40)
    log("FMCSA SCRAPER (New DOT Numbers)")
    log("=" * 40)
    
    try:
        from fmcsa_real import get_fmcsa_carriers_by_state
        
        all_leads = []
        for state in states:
            log(f"  [{state}] Scraping...")
            leads = get_fmcsa_carriers_by_state(state)
            all_leads.extend(leads)
            time.sleep(2)
        
        inserted, skipped = push_leads(all_leads, existing_ids, test_mode)
        log(f"FMCSA: {inserted} inserted, {skipped} skipped")
        return inserted
        
    except Exception as e:
        log(f"FMCSA error: {e}")
        return 0


def run_opencorporates(states, existing_ids, test_mode):
    """OpenCorporates - New business formations"""
    log("=" * 40)
    log("OPENCORPORATES SCRAPER (New Formations)")
    log("=" * 40)
    
    try:
        from opencorporates_scraper import scrape_opencorporates
        
        all_leads = []
        for state in states:
            log(f"  [{state}] Scraping...")
            leads = scrape_opencorporates(state)
            all_leads.extend(leads)
            time.sleep(3)
        
        inserted, skipped = push_leads(all_leads, existing_ids, test_mode)
        log(f"OpenCorporates: {inserted} inserted, {skipped} skipped")
        return inserted
        
    except Exception as e:
        log(f"OpenCorporates error: {e}")
        return 0


def run_osha(states, existing_ids, test_mode):
    """OSHA - Workplace violations"""
    log("=" * 40)
    log("OSHA SCRAPER (Violations)")
    log("=" * 40)
    
    try:
        from osha_real import get_osha_violations_by_state
        
        all_leads = []
        for state in states:
            log(f"  [{state}] Scraping...")
            leads = get_osha_violations_by_state(state)
            all_leads.extend(leads)
            time.sleep(2)
        
        inserted, skipped = push_leads(all_leads, existing_ids, test_mode)
        log(f"OSHA: {inserted} inserted, {skipped} skipped")
        return inserted
        
    except Exception as e:
        log(f"OSHA error: {e}")
        return 0


def run_permits(states, existing_ids, test_mode):
    """Building Permits - Contractors"""
    log("=" * 40)
    log("BUILDING PERMITS SCRAPER")
    log("=" * 40)
    
    try:
        from permits_scraper import run_permits_scraper
        
        leads = run_permits_scraper(states)
        inserted, skipped = push_leads(leads, existing_ids, test_mode)
        log(f"Permits: {inserted} inserted, {skipped} skipped")
        return inserted
        
    except Exception as e:
        log(f"Permits error: {e}")
        return 0


def run_licenses(states, existing_ids, test_mode):
    """Contractor Licenses"""
    log("=" * 40)
    log("CONTRACTOR LICENSE SCRAPER")
    log("=" * 40)
    
    try:
        from license_scraper import scrape_contractor_licenses
        
        all_leads = []
        for state in states:
            log(f"  [{state}] Scraping...")
            leads = scrape_contractor_licenses(state)
            all_leads.extend(leads)
            time.sleep(2)
        
        inserted, skipped = push_leads(all_leads, existing_ids, test_mode)
        log(f"Licenses: {inserted} inserted, {skipped} skipped")
        return inserted
        
    except Exception as e:
        log(f"Licenses error: {e}")
        return 0


# ============================================
# MAIN
# ============================================

def run_all_scrapers(states=None, test_mode=False):
    if states is None:
        states = DEFAULT_STATES
    
    log("=" * 60)
    log("LEADFLOW DAILY SCRAPER")
    log(f"States: {', '.join(states)}")
    log(f"Test mode: {test_mode}")
    log("=" * 60)
    
    existing_ids = get_existing_leads()
    log(f"Existing leads: {len(existing_ids)}")
    
    total = 0
    
    # Run all scrapers
    total += run_fmcsa(states, existing_ids, test_mode)
    total += run_opencorporates(states, existing_ids, test_mode)
    total += run_osha(states, existing_ids, test_mode)
    total += run_permits(states, existing_ids, test_mode)
    total += run_licenses(states, existing_ids, test_mode)
    
    log("=" * 60)
    log(f"COMPLETE - Total new leads: {total}")
    log("=" * 60)
    
    return total


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='LeadFlow Daily Scraper')
    parser.add_argument('--states', type=str, help='Comma-separated states')
    parser.add_argument('--all-states', action='store_true', help='All supported states')
    parser.add_argument('--test', action='store_true', help='Test mode')
    
    args = parser.parse_args()
    
    if args.all_states:
        states = ALL_STATES
    elif args.states:
        states = [s.strip().upper() for s in args.states.split(',')]
    else:
        states = DEFAULT_STATES
    
    run_all_scrapers(states=states, test_mode=args.test)
