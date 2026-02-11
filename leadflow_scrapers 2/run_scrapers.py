#!/usr/bin/env python3
"""
LeadFlow Scraper Runner
Run all scrapers or specific ones on demand
"""
import argparse
import logging
from datetime import datetime
import json
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tx_sos_scraper import TexasSOSScraper
from ar_sos_scraper import ArkansasSOSScraper
from ga_sos_scraper import GeorgiaSOSScraper
from fmcsa_scraper import FMCSAScraper
from osha_scraper import OSHAScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('LeadFlowRunner')

# Available scrapers
SCRAPERS = {
    'tx_sos': TexasSOSScraper,
    'ar_sos': ArkansasSOSScraper,
    'ga_sos': GeorgiaSOSScraper,
    'fmcsa': FMCSAScraper,
    'osha': OSHAScraper,
}

# Scraper groups by type
SCRAPER_GROUPS = {
    'sos': ['tx_sos', 'ar_sos', 'ga_sos'],
    'federal': ['fmcsa', 'osha'],
    'all': list(SCRAPERS.keys()),
}

# Scrapers by state
STATE_SCRAPERS = {
    'TX': ['tx_sos', 'fmcsa', 'osha'],
    'AR': ['ar_sos', 'fmcsa', 'osha'],
    'GA': ['ga_sos', 'fmcsa', 'osha'],
    'TN': ['fmcsa', 'osha'],
    'OK': ['fmcsa', 'osha'],
    'LA': ['fmcsa', 'osha'],
}


def run_scrapers(scraper_names: list) -> dict:
    """Run specified scrapers and return results"""
    results = {
        'timestamp': datetime.now().isoformat(),
        'scrapers': {},
        'totals': {
            'scraped': 0,
            'inserted': 0,
            'skipped': 0,
            'errors': 0
        }
    }
    
    for name in scraper_names:
        if name not in SCRAPERS:
            logger.warning(f"Unknown scraper: {name}")
            continue
        
        logger.info(f"Running {name} scraper...")
        
        try:
            scraper_class = SCRAPERS[name]
            scraper = scraper_class()
            result = scraper.run()
            
            results['scrapers'][name] = result
            
            if 'error' in result:
                results['totals']['errors'] += 1
            else:
                results['totals']['scraped'] += result.get('scraped', 0)
                results['totals']['inserted'] += result.get('inserted', 0)
                results['totals']['skipped'] += result.get('skipped', 0)
                
        except Exception as e:
            logger.error(f"Error running {name}: {e}")
            results['scrapers'][name] = {'error': str(e)}
            results['totals']['errors'] += 1
    
    return results


def main():
    parser = argparse.ArgumentParser(description='LeadFlow Scraper Runner')
    parser.add_argument('--source', '-s', type=str, help='Specific scraper to run')
    parser.add_argument('--state', '-t', type=str, help='Run all scrapers for a state')
    parser.add_argument('--group', '-g', type=str, choices=['sos', 'federal', 'all'], help='Run a group')
    parser.add_argument('--list', '-l', action='store_true', help='List available scrapers')
    parser.add_argument('--output', '-o', type=str, help='Output results to JSON file')
    
    args = parser.parse_args()
    
    if args.list:
        print("\nAvailable Scrapers:")
        for name in SCRAPERS.keys():
            print(f"  {name}")
        print("\nGroups: sos, federal, all")
        print("\nStates: TX, AR, GA, TN, OK, LA")
        return
    
    scrapers_to_run = []
    
    if args.source:
        scrapers_to_run = [args.source]
    elif args.state:
        state = args.state.upper()
        scrapers_to_run = STATE_SCRAPERS.get(state, [])
    elif args.group:
        scrapers_to_run = SCRAPER_GROUPS.get(args.group, [])
    else:
        scrapers_to_run = SCRAPER_GROUPS['all']
    
    if not scrapers_to_run:
        logger.error("No scrapers to run")
        return
    
    logger.info(f"Running: {', '.join(scrapers_to_run)}")
    results = run_scrapers(scrapers_to_run)
    
    print("\n" + "=" * 50)
    print("COMPLETE")
    print(f"Inserted: {results['totals']['inserted']}")
    print(f"Skipped: {results['totals']['skipped']}")
    print(f"Errors: {results['totals']['errors']}")
    print("=" * 50)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)


if __name__ == '__main__':
    main()
