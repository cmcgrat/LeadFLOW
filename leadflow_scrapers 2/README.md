# LeadFlow Real Scrapers

Production-ready scrapers for multi-state commercial insurance lead generation.

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Supabase service key
python run_scrapers.py
```

## Available Scrapers

| Source | Data | States |
|--------|------|--------|
| tx_sos | TX Secretary of State | TX |
| ar_sos | AR Secretary of State | AR |
| ga_sos | GA Secretary of State | GA |
| fmcsa | New DOT Numbers | All |
| osha | Safety Violations | All |

## Usage

```bash
# Run all
python run_scrapers.py

# Run specific source
python run_scrapers.py --source fmcsa

# Run by state
python run_scrapers.py --state TX

# List scrapers
python run_scrapers.py --list
```

## Scheduling

```bash
# Cron - daily at 6 AM
0 6 * * * cd /path/to/scrapers && python run_scrapers.py
```

## Lead Scoring

- HIGH: Score >= 80
- MEDIUM: Score 60-79
- LOW: Score < 60

Score factors: Industry (+20), Employees (+5-15), Signal type (+10-15), Recency (+5-15)
