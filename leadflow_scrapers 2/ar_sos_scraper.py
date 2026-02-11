"""
Arkansas Secretary of State - New Business Formations Scraper
Source: https://www.sos.arkansas.gov/corps/search_all.php
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
from base_scraper import BaseScraper

class ArkansasSOSScraper(BaseScraper):
    """Scrape new business formations from Arkansas Secretary of State"""
    
    SEARCH_URL = "https://www.sos.arkansas.gov/corps/search_all.php"
    
    # Industry keywords for classification
    INDUSTRY_KEYWORDS = {
        'Construction': ['construction', 'builder', 'roofing', 'concrete', 'framing', 'drywall', 'excavation'],
        'Trucking': ['trucking', 'freight', 'transport', 'hauling', 'logistics', 'carrier'],
        'Medical': ['medical', 'health', 'clinic', 'dental', 'therapy', 'physician', 'healthcare'],
        'Restaurant': ['restaurant', 'cafe', 'grill', 'kitchen', 'food', 'catering'],
        'Manufacturing': ['manufacturing', 'fabrication', 'machine', 'welding'],
        'Retail': ['retail', 'store', 'shop', 'boutique'],
        'Real Estate': ['real estate', 'realty', 'property'],
    }
    
    # Arkansas cities
    AR_CITIES = {
        'little rock', 'fort smith', 'fayetteville', 'springdale', 'jonesboro',
        'rogers', 'conway', 'north little rock', 'bentonville', 'pine bluff',
        'hot springs', 'benton', 'texarkana', 'sherwood', 'jacksonville',
        'bella vista', 'paragould', 'cabot', 'russellville', 'searcy',
        'van buren', 'el dorado', 'maumelle', 'bryant', 'siloam springs'
    }
    
    def get_source_name(self) -> str:
        return 'ar_sos'
    
    def classify_industry(self, company_name: str) -> str:
        """Classify company industry based on name keywords"""
        name_lower = company_name.lower()
        for industry, keywords in self.INDUSTRY_KEYWORDS.items():
            for keyword in keywords:
                if keyword in name_lower:
                    return industry
        return 'Other'
    
    def extract_city(self, address: str) -> str:
        """Extract city from address string"""
        if not address:
            return 'Unknown'
        
        address_lower = address.lower()
        for city in self.AR_CITIES:
            if city in address_lower:
                return city.title()
        
        parts = address.split(',')
        if len(parts) >= 2:
            return parts[-2].strip().title()
        
        return 'Unknown'
    
    def scrape(self) -> list:
        """Scrape new business formations from AR SOS"""
        leads = []
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        self.logger.info(f"Scraping AR SOS formations from {start_date.date()} to {end_date.date()}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        
        session = requests.Session()
        
        target_keywords = ['construction', 'trucking', 'medical', 'restaurant']
        
        for keyword in target_keywords:
            self.logger.info(f"Searching AR SOS for: {keyword}")
            
            try:
                params = {
                    'ESSION': 'CORE',
                    'ESSION_ID': '',
                    'ESSION_TYPE': 'NAME',
                    'SEARCH_TEXT': keyword,
                    'SEARCH_TYPE': 'BEGINS',
                    'STATUS': 'GOOD',
                }
                
                response = session.get(self.SEARCH_URL, params=params, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Parse results
                    results = soup.find_all('tr', class_='odd') + soup.find_all('tr', class_='even')
                    
                    for row in results[:30]:  # Limit per keyword
                        try:
                            cols = row.find_all('td')
                            if len(cols) >= 3:
                                company_name = cols[0].get_text(strip=True)
                                file_number = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                                status = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                                
                                if not company_name or status.upper() != 'GOOD STANDING':
                                    continue
                                
                                industry = self.classify_industry(company_name)
                                if industry == 'Other':
                                    continue
                                
                                lead = {
                                    'company_name': company_name.upper(),
                                    'industry': industry,
                                    'city': 'Unknown',  # Would need detail page for address
                                    'state': 'AR',
                                    'source_id': self.generate_source_id('ar_sos', file_number),
                                    'signal_type': 'new_formation',
                                    'signal_date': datetime.now().isoformat()[:10],
                                    'employees_estimated': '1-5',
                                    'raw_data': {
                                        'file_number': file_number,
                                        'status': status
                                    }
                                }
                                
                                leads.append(lead)
                                
                        except Exception as e:
                            self.logger.debug(f"Error parsing row: {e}")
                            continue
                
                time.sleep(2)
                
            except requests.RequestException as e:
                self.logger.warning(f"Request failed for {keyword}: {e}")
                continue
        
        self.logger.info(f"Found {len(leads)} leads from AR SOS")
        return leads


if __name__ == '__main__':
    scraper = ArkansasSOSScraper()
    result = scraper.run()
    print(result)
