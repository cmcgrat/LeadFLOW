"""
Base scraper class for LeadFlow
"""
import os
import hashlib
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BaseScraper(ABC):
    """Base class for all LeadFlow scrapers"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.supabase = create_client(
            os.getenv('SUPABASE_URL', 'https://fxmclnvdimbnkuzkdnye.supabase.co'),
            os.getenv('SUPABASE_KEY', '')
        )
        self.leads_inserted = 0
        self.leads_skipped = 0
    
    @abstractmethod
    def scrape(self) -> list:
        """Scrape data from source. Returns list of lead dictionaries."""
        pass
    
    @abstractmethod
    def get_source_name(self) -> str:
        """Return the source identifier (e.g., 'tx_sos', 'fmcsa')"""
        pass
    
    def generate_source_id(self, *args) -> str:
        """Generate unique source ID from arguments"""
        combined = '_'.join(str(a) for a in args)
        return hashlib.md5(combined.encode()).hexdigest()[:16]
    
    def lead_exists(self, source_id: str) -> bool:
        """Check if lead with this source_id already exists"""
        result = self.supabase.table('leads').select('id').eq('source_id', source_id).execute()
        return len(result.data) > 0
    
    def calculate_score(self, lead: dict) -> int:
        """Calculate lead score based on various factors"""
        score = 50  # Base score
        
        # Industry scoring
        high_value_industries = ['Construction', 'Trucking', 'Oilfield', 'Manufacturing', 'Medical']
        if lead.get('industry') in high_value_industries:
            score += 20
        
        # Employee count scoring
        employees = lead.get('employees_estimated', '1-5')
        if employees in ['50-100', '100+']:
            score += 15
        elif employees in ['25-50']:
            score += 10
        elif employees in ['10-25']:
            score += 5
        
        # Signal type scoring
        signal_type = lead.get('signal_type', '')
        if signal_type in ['osha_violation', 'high_emod']:
            score += 15  # Coverage gap - urgent
        elif signal_type in ['new_formation', 'new_dot']:
            score += 10  # Likely uninsured
        
        # Recency scoring
        signal_date = lead.get('signal_date')
        if signal_date:
            try:
                days_old = (datetime.now() - datetime.fromisoformat(signal_date)).days
                if days_old <= 7:
                    score += 15
                elif days_old <= 14:
                    score += 10
                elif days_old <= 30:
                    score += 5
            except:
                pass
        
        return min(score, 100)  # Cap at 100
    
    def determine_priority(self, score: int) -> str:
        """Determine priority based on score"""
        if score >= 80:
            return 'HIGH'
        elif score >= 60:
            return 'MEDIUM'
        return 'LOW'
    
    def determine_lead_type(self, signal_type: str) -> str:
        """Determine lead type based on signal"""
        uninsured_signals = ['new_formation', 'new_dot', 'first_permit', 'foreign_registration']
        if signal_type in uninsured_signals:
            return 'likely_uninsured'
        return 'coverage_gap'
    
    def save_lead(self, lead: dict) -> bool:
        """Save lead to Supabase. Returns True if inserted, False if skipped."""
        source_id = lead.get('source_id')
        
        if self.lead_exists(source_id):
            self.leads_skipped += 1
            self.logger.debug(f"Skipping duplicate: {lead.get('company_name')}")
            return False
        
        # Calculate score and priority
        lead['score'] = self.calculate_score(lead)
        lead['priority'] = self.determine_priority(lead['score'])
        lead['lead_type'] = self.determine_lead_type(lead.get('signal_type', ''))
        lead['stage'] = 'new'
        lead['owner'] = 'Unassigned'
        lead['source'] = self.get_source_name()
        
        try:
            self.supabase.table('leads').insert(lead).execute()
            self.leads_inserted += 1
            self.logger.info(f"Inserted: {lead.get('company_name')} ({lead.get('city')}, {lead.get('state')})")
            return True
        except Exception as e:
            self.logger.error(f"Error inserting lead: {e}")
            return False
    
    def run(self):
        """Main entry point - scrape and save leads"""
        self.logger.info(f"Starting {self.get_source_name()} scraper...")
        
        try:
            leads = self.scrape()
            self.logger.info(f"Scraped {len(leads)} leads")
            
            for lead in leads:
                self.save_lead(lead)
            
            self.logger.info(f"Complete: {self.leads_inserted} inserted, {self.leads_skipped} skipped")
            return {
                'source': self.get_source_name(),
                'scraped': len(leads),
                'inserted': self.leads_inserted,
                'skipped': self.leads_skipped
            }
        except Exception as e:
            self.logger.error(f"Scraper failed: {e}")
            return {
                'source': self.get_source_name(),
                'error': str(e)
            }
