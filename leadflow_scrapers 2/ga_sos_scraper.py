"""
Georgia Secretary of State - New Business Formations Scraper
Source: https://ecorp.sos.ga.gov/BusinessSearch
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
from base_scraper import BaseScraper

class GeorgiaSOSScraper(BaseScraper):
    """Scrape new business formations from Georgia Secretary of State"""
    
    SEARCH_URL = "https://ecorp.sos.ga.gov/BusinessSearch"
    API_URL = "https://ecorp.sos.ga.gov/BusinessSearch/BusinessSearchResults"
    
    INDUSTRY_KEYWORDS = {
        'Construction': ['construction', 'builder', 'roofing', 'concrete', 'framing', 'drywall', 'excavation', 'contractor'],
        'Trucking': ['trucking', 'freight', 'transport', 'hauling', 'logistics', 'carrier', 'moving'],
        'Medical': ['medical', 'health', 'clinic', 'dental', 'therapy', 'physician', 'healthcare', 'wellness'],
        'Restaurant': ['restaurant', 'cafe', 'grill', 'kitchen', 'food', 'catering', 'bar'],
        'Manufacturing': ['manufacturing', 'fabrication', 'machine', 'welding', 'industrial'],
        'Technology': ['technology', 'software', 'tech', 'digital', 'it ', 'computer'],
        'Real Estate': ['real estate', 'realty', 'property', 'investment'],
        'Landscaping': ['landscaping', 'lawn', 'garden', 'tree'],
    }
    
    GA_CITIES = {
        'atlanta', 'augusta', 'columbus', 'macon', 'savannah', 'athens',
        'sandy springs', 'roswell', 'johns creek', 'albany', 'warner robins',
        'alpharetta', 'marietta', 'valdosta', 'smyrna', 'brookhaven',
        'dunwoody', 'peachtree corners', 'gainesville', 'newnan', 'dalton',
        'kennesaw', 'lawrenceville', 'duluth', 'woodstock', 'canton',
        'carrollton', 'griffin', 'mcdonough', 'acworth', 'rome'
    }
    
    def get_source_name(self) -> str:
        return 'ga_sos'
    
    def classify_industry(self, company_name: str) -> str:
        name_lower = company_name.lower()
        for industry, keywords in self.INDUSTRY_KEYWORDS.items():
            for keyword in keywords:
                if keyword in name_lower:
                    return industry
        return 'Other'
    
    def extract_city(self, address: str) -> str:
        if not address:
            return 'Unknown'
        
        address_lower = address.lower()
        for city in self.GA_CITIES:
            if city in address_lower:
                return city.title()
        
        parts = address.split(',')
        if len(parts) >= 2:
            return parts[-2].strip().title()
        
        return 'Atlanta'  # Default to Atlanta for GA
    
    def scrape(self) -> list:
        """Scrape new business formations from GA SOS"""
        leads = []
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        self.logger.info(f"Scraping GA SOS formations from {start_date.date()} to {end_date.date()}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, */*',
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        
        session = requests.Session()
        
        target_keywords = ['construction', 'trucking', 'medical', 'restaurant', 'logistics']
        
        for keyword in target_keywords:
            self.logger.info(f"Searching GA SOS for: {keyword}")
            
            try:
                # Georgia uses a POST-based search
                data = {
                    'SearchType': 'BusinessName',
                    'SearchText': keyword,
                    'BusinessType': 'All',
                    'Status': 'Active',
                    'StartDate': start_date.strftime('%m/%d/%Y'),
                    'EndDate': end_date.strftime('%m/%d/%Y'),
                }
                
                response = session.post(self.API_URL, data=data, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    # Try parsing as HTML
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    rows = soup.find_all('tr')[1:]  # Skip header
                    
                    for row in rows[:30]:
                        try:
                            cols = row.find_all('td')
                            if len(cols) >= 4:
                                company_name = cols[0].get_text(strip=True)
                                control_number = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                                status = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                                
                                if not company_name:
                                    continue
                                
                                if 'active' not in status.lower():
                                    continue
                                
                                industry = self.classify_industry(company_name)
                                if industry == 'Other':
                                    continue
                                
                                lead = {
                                    'company_name': company_name.upper(),
                                    'industry': industry,
                                    'city': 'Atlanta',  # Default - would need detail lookup
                                    'state': 'GA',
                                    'source_id': self.generate_source_id('ga_sos', control_number or company_name),
                                    'signal_type': 'new_formation',
                                    'signal_date': datetime.now().isoformat()[:10],
                                    'employees_estimated': '1-5',
                                    'raw_data': {
                                        'control_number': control_number,
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
        
        self.logger.info(f"Found {len(leads)} leads from GA SOS")
        return leads


if __name__ == '__main__':
    scraper = GeorgiaSOSScraper()
    result = scraper.run()
    print(result)
