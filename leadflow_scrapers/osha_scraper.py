"""
OSHA Violations Scraper - Federal OSHA Database (All States)
Source: https://www.osha.gov/ords/imis/establishment.search
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import re
from base_scraper import BaseScraper

class OSHAScraper(BaseScraper):
    """Scrape OSHA violations for coverage gap leads"""
    
    # OSHA Data API
    SEARCH_URL = "https://www.osha.gov/ords/imis/establishment.search"
    DETAIL_URL = "https://www.osha.gov/ords/imis/establishment.inspection_detail"
    
    # Target states
    TARGET_STATES = ['TX', 'AR', 'GA', 'TN', 'OK', 'LA', 'AL', 'MS']
    
    # Industry SIC codes we care about
    TARGET_SIC_CODES = {
        '15': 'Construction',  # Building Construction
        '16': 'Construction',  # Heavy Construction
        '17': 'Construction',  # Special Trade Contractors
        '42': 'Trucking',      # Motor Freight Transportation
        '13': 'Oilfield',      # Oil and Gas Extraction
        '49': 'Oilfield',      # Electric, Gas, Sanitary Services
        '34': 'Manufacturing', # Fabricated Metal Products
        '35': 'Manufacturing', # Industrial Machinery
        '36': 'Manufacturing', # Electronic Equipment
        '80': 'Medical',       # Health Services
        '58': 'Restaurant',    # Eating and Drinking Places
    }
    
    def get_source_name(self) -> str:
        return 'osha'
    
    def classify_industry_from_sic(self, sic_code: str) -> str:
        """Get industry from SIC code"""
        if not sic_code:
            return 'Other'
        prefix = sic_code[:2]
        return self.TARGET_SIC_CODES.get(prefix, 'Other')
    
    def parse_penalty(self, penalty_str: str) -> float:
        """Parse penalty string to float"""
        if not penalty_str:
            return 0
        try:
            # Remove $ and commas
            cleaned = re.sub(r'[,$]', '', penalty_str)
            return float(cleaned)
        except:
            return 0
    
    def scrape(self) -> list:
        """Scrape recent OSHA violations"""
        leads = []
        
        # Get violations from last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        self.logger.info(f"Scraping OSHA violations from {start_date.date()} to {end_date.date()}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        
        session = requests.Session()
        
        for state in self.TARGET_STATES:
            self.logger.info(f"Searching OSHA violations in {state}...")
            
            try:
                # Search for recent inspections with violations
                params = {
                    'State': state,
                    'sic': '',  # All SIC codes
                    'officetype': '',
                    'Office': '',
                    'startmonth': start_date.month,
                    'startday': start_date.day,
                    'startyear': start_date.year,
                    'endmonth': end_date.month,
                    'endday': end_date.day,
                    'endyear': end_date.year,
                    'p_finish': '',
                    'sort': 'close_date desc',
                    'owner': '',
                    'establishment': '',
                    'InspNr': '',
                }
                
                response = session.get(self.SEARCH_URL, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find results table
                    table = soup.find('table', {'class': 'table'}) or soup.find('table')
                    
                    if table:
                        rows = table.find_all('tr')[1:]  # Skip header
                        
                        for row in rows[:50]:  # Limit to 50 per state
                            try:
                                cols = row.find_all('td')
                                if len(cols) >= 5:
                                    company_name = cols[0].get_text(strip=True)
                                    city = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                                    sic_code = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                                    penalty = cols[4].get_text(strip=True) if len(cols) > 4 else '$0'
                                    inspection_nr = ''
                                    
                                    # Try to extract inspection number from link
                                    link = cols[0].find('a')
                                    if link and 'href' in link.attrs:
                                        href = link['href']
                                        match = re.search(r'InspNr=(\d+)', href)
                                        if match:
                                            inspection_nr = match.group(1)
                                    
                                    if not company_name or len(company_name) < 3:
                                        continue
                                    
                                    penalty_amount = self.parse_penalty(penalty)
                                    
                                    # Only include significant violations (penalty > $1000)
                                    if penalty_amount < 1000:
                                        continue
                                    
                                    industry = self.classify_industry_from_sic(sic_code)
                                    
                                    # Skip industries we don't target
                                    if industry == 'Other':
                                        continue
                                    
                                    lead = {
                                        'company_name': company_name.upper(),
                                        'industry': industry,
                                        'city': city.title() if city else 'Unknown',
                                        'state': state,
                                        'source_id': self.generate_source_id('osha', inspection_nr or company_name),
                                        'signal_type': 'osha_violation',
                                        'signal_date': datetime.now().isoformat()[:10],
                                        'employees_estimated': '10-25',  # OSHA typically inspects larger employers
                                        'raw_data': {
                                            'inspection_nr': inspection_nr,
                                            'sic_code': sic_code,
                                            'penalty': penalty_amount,
                                            'violation_type': 'Serious'  # Default assumption
                                        }
                                    }
                                    
                                    leads.append(lead)
                                    
                            except Exception as e:
                                self.logger.debug(f"Error parsing row: {e}")
                                continue
                
                # Respect rate limits
                time.sleep(2)
                
            except requests.RequestException as e:
                self.logger.warning(f"Request failed for {state}: {e}")
                continue
        
        self.logger.info(f"Found {len(leads)} OSHA violation leads")
        return leads


# For testing
if __name__ == '__main__':
    scraper = OSHAScraper()
    result = scraper.run()
    print(result)
