"""
FMCSA Scraper - New DOT Numbers and MC Authority (All States)
Source: https://ai.fmcsa.dot.gov/SMS/
"""
import requests
from datetime import datetime, timedelta
import time
from base_scraper import BaseScraper

class FMCSAScraper(BaseScraper):
    """Scrape new motor carrier registrations from FMCSA"""
    
    # FMCSA public APIs
    BASE_URL = "https://mobile.fmcsa.dot.gov/qc/services"
    CARRIER_API = "https://mobile.fmcsa.dot.gov/qc/services/carriers"
    
    # Target states
    TARGET_STATES = ['TX', 'AR', 'GA', 'TN', 'OK', 'LA', 'AL', 'MS']
    
    # State name mapping
    STATE_NAMES = {
        'TX': 'TEXAS', 'AR': 'ARKANSAS', 'GA': 'GEORGIA', 'TN': 'TENNESSEE',
        'OK': 'OKLAHOMA', 'LA': 'LOUISIANA', 'AL': 'ALABAMA', 'MS': 'MISSISSIPPI',
        'FL': 'FLORIDA', 'NC': 'NORTH CAROLINA', 'SC': 'SOUTH CAROLINA'
    }
    
    def get_source_name(self) -> str:
        return 'fmcsa'
    
    def estimate_employees(self, power_units: int, drivers: int) -> str:
        """Estimate employee count from power units and drivers"""
        total = max(power_units, drivers)
        if total >= 100:
            return '100+'
        elif total >= 50:
            return '50-100'
        elif total >= 25:
            return '25-50'
        elif total >= 10:
            return '10-25'
        elif total >= 5:
            return '5-10'
        return '1-5'
    
    def scrape(self) -> list:
        """Scrape new motor carriers from FMCSA"""
        leads = []
        
        self.logger.info("Scraping FMCSA for new motor carriers...")
        
        headers = {
            'User-Agent': 'LeadFlow/1.0',
            'Accept': 'application/json',
        }
        
        for state in self.TARGET_STATES:
            self.logger.info(f"Fetching carriers for {state}...")
            
            try:
                # FMCSA provides a public lookup API
                # Note: Production would need to handle proper API authentication
                # and pagination for large result sets
                
                params = {
                    'stateAbbrev': state,
                    'mcMxFfNum': '',
                    'dotNum': '',
                    'legalName': '',
                    'dbaName': '',
                    'city': '',
                    'start': 0,
                    'size': 100
                }
                
                # Try the carrier lookup endpoint
                response = requests.get(
                    f"{self.CARRIER_API}/name",
                    params=params,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    carriers = data.get('content', [])
                    
                    for carrier in carriers:
                        try:
                            # Check if recently registered (within last 90 days)
                            mcs150_date = carrier.get('mcs150FormDate', '')
                            if mcs150_date:
                                try:
                                    form_date = datetime.strptime(mcs150_date, '%Y-%m-%d')
                                    if (datetime.now() - form_date).days > 90:
                                        continue  # Skip older carriers
                                except:
                                    pass
                            
                            dot_number = carrier.get('dotNumber', '')
                            company_name = carrier.get('legalName', '') or carrier.get('dbaName', '')
                            city = carrier.get('phyCity', '')
                            
                            if not company_name or not dot_number:
                                continue
                            
                            power_units = int(carrier.get('totalPowerUnits', 0) or 0)
                            drivers = int(carrier.get('totalDrivers', 0) or 0)
                            
                            lead = {
                                'company_name': company_name.upper(),
                                'industry': 'Trucking',
                                'city': city.title() if city else 'Unknown',
                                'state': state,
                                'zip': carrier.get('phyZipcode', ''),
                                'phone': carrier.get('telephone', ''),
                                'source_id': self.generate_source_id('fmcsa', dot_number),
                                'signal_type': 'new_dot',
                                'signal_date': mcs150_date or datetime.now().isoformat()[:10],
                                'employees_estimated': self.estimate_employees(power_units, drivers),
                                'raw_data': {
                                    'dot_number': dot_number,
                                    'mc_number': carrier.get('mcNumber', ''),
                                    'power_units': power_units,
                                    'drivers': drivers,
                                    'carrier_operation': carrier.get('carrierOperation', ''),
                                    'cargo_carried': carrier.get('cargoCarried', '')
                                }
                            }
                            
                            leads.append(lead)
                            
                        except Exception as e:
                            self.logger.debug(f"Error parsing carrier: {e}")
                            continue
                
                # Respect rate limits
                time.sleep(1)
                
            except requests.RequestException as e:
                self.logger.warning(f"Request failed for {state}: {e}")
                continue
        
        self.logger.info(f"Found {len(leads)} new trucking leads from FMCSA")
        return leads


# For testing
if __name__ == '__main__':
    scraper = FMCSAScraper()
    result = scraper.run()
    print(result)
