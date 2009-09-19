from decimal import Decimal

USER_AGENTS = [
    'Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.5) Gecko/2008121623 Ubuntu/8.10 (intrepid) Firefox/3.0.5',
    'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.0.5) Gecko/2008120122 Firefox/3.0.5 (.NET CLR 3.5.30729)',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.27.1 (KHTML, like Gecko) Version/3.2.1 Safari/525.27.1',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.43 Safari/525.19',
    'Opera/9.63 (Windows NT 5.1; U; en) Presto/2.1.1',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30618)',
]

# Database
DB_CONNECTION = 'mysql://root:@localhost:3306/adwords'
DEBUG_DB = False

# USD
CAMPAIGN_BUDGET = Decimal('1000.00')

# Seconds
FAKE_DELAY_MIN = 2.0
FAKE_DELAY_MAX = 4.0

# Set False to turn logging off
LOGGER = file('./log.txt', 'a').write
# 0 - log only processor routines calles, 1 - also log each http-request
DEBUG_LEVEL = 0

# Limits
MAX_CAMPAIGNS_PER_ACCOUNT = 25
MAX_ADGROUPS_PER_CAMPAIGN = 100
MAX_KEYWORDS_PER_ADGROUP = 1000

# Targeting locations
TARGETED_LOCATIONS = {
    'US': 'COUNTRY|2840|United States|US|Country|State|false|true|45493238|-104785149',
    'CA': 'COUNTRY|2124|Canada|CA|Country|Territory|false|true|61313568|-94184135',
}
DEFAULT_TARGETED_LOCATION_STRING = TARGETED_LOCATIONS['US']
