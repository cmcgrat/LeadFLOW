"""
Texas Secretary of State - New Business Formations Scraper
Source: https://mycpa.cpa.state.tx.us/coa/
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re
import time
from base_scraper import BaseScraper

class TexasSOSScraper(BaseScraper):
    """Scrape new business formations from Texas Secretary of State"""
    
    BASE_URL = "https://mycpa.cpa.state.tx.us/coa/"
    SEARCH_URL = "https://mycpa.cpa.state.tx.us/coa/coaSearch.do"
    
    # Industry keywords for classification
    INDUSTRY_KEYWORDS = {
        'Construction': ['construction', 'builder', 'roofing', 'concrete', 'framing', 'drywall', 'excavation', 'grading', 'paving'],
        'Trucking': ['trucking', 'freight', 'transport', 'hauling', 'logistics', 'carrier', 'moving'],
        'Oilfield': ['oilfield', 'drilling', 'petroleum', 'energy', 'wellhead', 'pipeline', 'frac'],
        'Electrical': ['electric', 'electrical', 'wiring', 'power'],
        'Plumbing': ['plumbing', 'plumber', 'hvac', 'heating', 'cooling', 'air conditioning'],
        'Restaurant': ['restaurant', 'cafe', 'grill', 'kitchen', 'food', 'catering', 'bar', 'tavern'],
        'Medical': ['medical', 'health', 'clinic', 'dental', 'therapy', 'chiropractic', 'physician'],
        'Landscaping': ['landscaping', 'lawn', 'garden', 'tree service', 'irrigation'],
        'Manufacturing': ['manufacturing', 'fabrication', 'machine', 'welding', 'metal'],
        'Cleaning': ['cleaning', 'janitorial', 'maid', 'sanitation'],
        'Auto Services': ['auto', 'automotive', 'mechanic', 'body shop', 'tire', 'collision'],
        'Staffing': ['staffing', 'employment', 'recruiting', 'temp', 'personnel'],
        'Warehouse': ['warehouse', 'storage', 'distribution', 'fulfillment'],
        'Retail': ['retail', 'store', 'shop', 'boutique', 'sales'],
        'Wholesale': ['wholesale', 'distributor', 'supply'],
        'Real Estate': ['real estate', 'realty', 'property', 'investment'],
        'Technology': ['technology', 'software', 'tech', 'digital', 'it services', 'computer'],
        'Consulting': ['consulting', 'consultant', 'advisory', 'management']
    }
    
    # Texas cities for validation
    TX_CITIES = {
        'houston', 'dallas', 'san antonio', 'austin', 'fort worth', 'el paso', 'arlington',
        'corpus christi', 'plano', 'laredo', 'lubbock', 'irving', 'garland', 'amarillo',
        'grand prairie', 'brownsville', 'mckinney', 'frisco', 'pasadena', 'mesquite',
        'killeen', 'mcallen', 'waco', 'midland', 'odessa', 'beaumont', 'denton', 'carrollton',
        'round rock', 'lewisville', 'tyler', 'college station', 'abilene', 'pearland',
        'san angelo', 'league city', 'allen', 'longview', 'sugar land', 'edinburg',
        'mission', 'bryan', 'pharr', 'baytown', 'temple', 'missouri city', 'flower mound',
        'north richland hills', 'harlingen', 'conroe', 'new braunfels', 'victoria', 'cedar park'
    }
    
    def get_source_name(self) -> str:
        return 'tx_sos'
    
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
        
        # Common patterns
        address_lower = address.lower()
        for city in self.TX_CITIES:
            if city in address_lower:
                return city.title()
        
        # Try to extract from comma-separated
        parts = address.split(',')
        if len(parts) >= 2:
            return parts[-2].strip().title()
        
        return 'Unknown'
    
    def scrape(self) -> list:
        """Scrape new business formations from TX SOS"""
        leads = []
        
        # Get formations from last 7 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        self.logger.info(f"Scraping TX SOS formations from {start_date.date()} to {end_date.date()}")
        
        try:
            # Note: TX SOS requires specific search parameters
            # This is a simplified version - real implementation would need
            # to handle their specific form submission and pagination
            
            session = requests.Session()
            
            # Set headers to mimic browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            
            # Search parameters for new formations
            # Target industries we care about
            target_keywords = ['construction', 'trucking', 'transport', 'oilfield', 'electric', 'plumbing']
            
            for keyword in target_keywords:
                self.logger.info(f"Searching for: {keyword}")
                
                params = {
                    'searchType': 'name',
                    'searchString': keyword,
                    'filingStartDate': start_date.strftime('%m/%d/%Y'),
                    'filingEndDate': end_date.strftime('%m/%d/%Y'),
                }
                
                try:
                    response = session.get(self.SEARCH_URL, params=params, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Parse results table
                        results = soup.find_all('tr', class_='resultsRow')
                        
                        for row in results:
                            try:
                                cols = row.find_all('td')
                                if len(cols) >= 4:
                                    company_name = cols[0].get_text(strip=True)
                                    file_number = cols[1].get_text(strip=True)
                                    formation_date = cols[2].get_text(strip=True)
                                    address = cols[3].get_text(strip=True) if len(cols) > 3 else ''
                                    
                                    # Skip if not a real business name
                                    if not company_name or len(company_name) < 3:
                                        continue
                                    
                                    city = self.extract_city(address)
                                    industry = self.classify_industry(company_name)
                                    
                                    lead = {
                                        'company_name': company_name.upper(),
                                        'industry': industry,
                                        'city': city,
                                        'state': 'TX',
                                        'source_id': self.generate_source_id('tx_sos', file_number),
                                        'signal_type': 'new_formation',
                                        'signal_date': datetime.now().isoformat()[:10],
                                        'employees_estimated': '1-5',  # New businesses typically small
                                        'raw_data': {
                                            'file_number': file_number,
                                            'formation_date': formation_date,
                                            'address': address
                                        }
                                    }
                                    
                                    leads.append(lead)
                                    
                            except Exception as e:
                                self.logger.debug(f"Error parsing row: {e}")
                                continue
                    
                    # Be nice to the server
                    time.sleep(2)
                    
                except requests.RequestException as e:
                    self.logger.warning(f"Request failed for {keyword}: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Scraping error: {e}")
        
        self.logger.info(f"Found {len(leads)} potential leads from TX SOS")
        return leads


# For testing
if __name__ == '__main__':
    scraper = TexasSOSScraper()
    result = scraper.run()
    print(result)
