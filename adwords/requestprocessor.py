'''
adwords.requestprocessor

@author: Philip Rud
@version: 0.1.1
'''

import urllib
import urllib2
import cookielib
import re
import time
import datetime
import decimal
import random

import settings

#-------------------------------------------------------------------------------

def log(message):
    if settings.LOGGER:
        settings.LOGGER('[' + str(datetime.datetime.now()) + ']: ' + message + '\n')

#-------------------------------------------------------------------------------

class UnexpectedResponseError(Exception):
    '''
    Raised by adwords.RequestProcessor when it comes to inconsistent state
    after getting a response from AdWords it wasn't expecting for.
    '''
    def __init__(self, message=''):
        log('! ERROR - UnexpectedResponseError')
        Exception.__init__(self, message)

class IncorrectStateError(Exception):
    '''
    Raised when one tries to use adwords.RequestProcessor to perform an action
    that can't be currently performed like trying to sign out before signed in.
    '''
    def __init__(self, message=''):
        log('! ERROR - IncorrectStateError')
        Exception.__init__(self, message)

#-------------------------------------------------------------------------------

class Keyword:
    '''
    Can be passed as an element of keywords list to methods of RequestProcessor
    '''
    
    MATCH_MODE_BROAD = 'broad'
    MATCH_MODE_PHRASE = 'phrase'
    MATCH_MODE_EXACT = 'exact'
    MATCH_MODE_NEGATIVE = 'negative'
    
    def __init__(self, keyword, bid=None, url=None):
        self.keyword = keyword
        self.bid = decimal.Decimal(bid) if bid else None
        self.url = url
        
    def __str__(self):
        result = self.keyword
            
        if self.bid: result += ' ** ' + ('%.2f' % self.bid)
        if self.url: result += ' ** ' + self.url
        
        return result
    
    def __unicode__(self):
        return self.__str__()
    
    def __repr__(self):
        return self.__str__()

#-------------------------------------------------------------------------------

class RequestProcessor:
    '''
    Handles all low-level data intercharge with Google AdWords. Tries to
    emulate browser behavior when it's possible.  
    '''
    
    # identifies the current processor state as being logged in
    _signed_in = False


    def _fetchurl(self, request):
        '''
        'Open' function of an opener that was build for current instance
        
        @param request: urllib2.Request
        @return: urllib2.Response
        '''
        response = self._urlopener.open(request)
        if request.get_full_url() != response.geturl():
            if settings.DEBUG_LEVEL > 0:
                log('   -> ' + response.geturl())
        
        return response

    
    def _unescape_js(self, unescaped):
        '''
        Decodes strings containing HEX-escaped chars to use in
        javascript strings back to their normal values.
        
        @param unescaped: string
        @return: string  
        '''
        def hexcode2char(matchobj):
            charcode = '0x' + matchobj.group('hexcode')
            return chr(int(charcode, 16))
            
        return re.sub(r'\\x(?P<hexcode>\w\w)', hexcode2char, unescaped)
    
    
    def _create_browserlike_request(self, url):
        '''
        Creates a urllib2.Request object that tries to look like the one
        sent by a real browser by different ways like random User-Agent.
        
        @param url: string
        @return: urllib2.Request
        '''
        request = urllib2.Request(url)
        request.add_header('User-Agent', 
            settings.USER_AGENTS[hash(self._current_email) % len(settings.USER_AGENTS)])
        request.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
        request.add_header('Accept-Language', 'en-us,en;q=0.5')
        request.add_header('Accept-Charset', 'ISO-8859-1,utf-8;q=0.7,*;q=0.7')
        
        if settings.DEBUG_LEVEL > 0:
            log(url)
        
        return request
        
        
    def _do_fake_delay(self):
        '''
        Waits random amount of time to emulate a real user/browser behavior.
        '''
        time.sleep(random.uniform(settings.FAKE_DELAY_MIN, settings.FAKE_DELAY_MAX))
        
        
    def __init__(self, email, password):
        '''
        @param email: str
        @param password: str
        '''
        self._current_email = email
        self._current_password = password
        
        self._cookiejar = cookielib.CookieJar()
        self._cookieprocessor = urllib2.HTTPCookieProcessor(self._cookiejar)
        self._urlopener = urllib2.build_opener(self._cookieprocessor)
        
        random.seed()
    
    
    def sign_in(self):
        '''
        Renders the current instance as logged into Google AdWords account
        with given credentials.
        
        @param email: string
        @param password: string  
        '''
        log(' + sign_in')
        
        request = self._create_browserlike_request('http://adwords.google.com/')
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://www.google.com/accounts/ServiceLoginAuth?service=adwords')
        request.add_data(urllib.urlencode({
            'continue': 'https://adwords.google.com/select/gaiaauth?apt=None&ugl=true',
            'service': 'adwords',
            'ifr': 'false',
            'ltmpl': 'adwords',
            'hl': 'en-US',
            'alwf': 'true',
            'Email': self._current_email,
            'Passwd': self._current_password,
            'PersistentCookie': 'yes',
            'rmShown': '1',
            'signIn': 'Sign in',
        }))
        response = self._fetchurl(request)

        try:
            sid_url = re.search(r'location.replace\("(?P<url>.*)"\)', response.read()).group('url')
            sid_url = self._unescape_js(sid_url)
        except:
            raise UnexpectedResponseError('Failed to sign in with given credentials.')
        
        request = self._create_browserlike_request(sid_url)
        self._fetchurl(request)
        self._do_fake_delay()
        self._signed_in = True
    
    
    def sign_out(self):
        '''
        Signs the current instance out.
        '''
        log(' +++ sign_out')
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/gaialogout')
        self._fetchurl(request)
        self._do_fake_delay()
        self._signed_in = False
    
    
    def is_signed_in(self):
        '''
        Returns whether this instance is signed in.
        
        @return: bool
        '''
        return self._signed_in
    
    
    def add_campaign(self, campaign_name, adgroup_name, display_url, url, headline, adline1, adline2, keywords, bid):
        '''
        Adds a campaign with a given AdGroup (as the first AdGroup in campaign)
        and returns ID of the created campaign and ID of its first AdGroup
        
        @param campaign_name: str 
        @param adgroup_name: str
        @param display_url: str
        @param url: str
        @param headline: str
        @param adline1: str  
        @param adline2: str
        @param keywords: list
        @param bid: decimal.Decimal
        @return: long,long
        '''
        log(' +++ add_campaign "%s" (first adgroup - "%s")' % (campaign_name, adgroup_name))
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
        
        # Step 0
        request = self._create_browserlike_request('https://adwords.google.com/select/StartNewCampaign')
        response = self._fetchurl(request)
        self._do_fake_delay()
        
        try:
            wizard_key = re.search('TargetingWizardWithGeoPicker.+wizardKey=(?P<wizard_key>\w+)', response.geturl()).group('wizard_key')
        except:
            raise UnexpectedResponseError()
        
        # Step 1
        request = self._create_browserlike_request('https://adwords.google.com/select/TargetingWizardWithGeoPickerInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({
            'campaignBox': 'noneSelected',
            'campaignName': campaign_name,
            'adGroupName': adgroup_name,
            'language': 'en',
            'targetedLocationsSerialized': settings.DEFAULT_TARGETED_LOCATION_STRING,
            'excludedLocationsSerialized': '',
            'emptyAudienceMeansTargetsAllCountries': 'false',
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)
        
        if re.search('FirstAdTypeFinder', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 2
        request = self._create_browserlike_request('https://adwords.google.com/select/StartCKSRequest?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({
            'thisAction': '//CreateAd',
            'creativeScope': 'textController.textCreative',
            'controllerScope': 'textController',
            'textController.textCreative.headline': headline,
            'textController.textCreative.description1': adline1,
            'textController.textCreative.description2': adline2,
            'textController.textCreative.visibleUrl': display_url,
            'textController.protocol': 'https://' if url.startswith('https://') else 'http://',
            'textController.destUrl': url if url.find('://') == -1 else url[url.find('://') + 3:],
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)
        
        if re.search('ChooseKeywords', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 3
        request = self._create_browserlike_request('https://adwords.google.com/select/ChooseKeywordsInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({
            'thisAction': '//ChooseKeywords',
            'akssSuggestedKeywords': '',
            'cksSuggestedKeywords': '',
            'helperSuggestedKeywords': '',
            'keywords': '\x0D\x0A'.join(keywords),
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)
        
        if re.search('SetPricing', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 4
        request = self._create_browserlike_request('https://adwords.google.com/select/SetPricingInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({                                          
            'thisAction': '//SetPricing',
            'initialCurrencyCode': 'USD',
            'usersBudgetUnits': '%.2f' % settings.CAMPAIGN_BUDGET,
            'usersBudgetPeriod': 'DAILY',
            'usersMaxCpcUnits': '%.2f' % bid,
            'usersMaxContentCpcUnits': '',
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)
        
        if re.search('ReviewAccount', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 5
        request = self._create_browserlike_request('https://adwords.google.com/select/ReviewAccountInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({                                          
            'saveCampaignButton': 'Save Campaign',
        }))
        response = self._fetchurl(request)
        
        try:
            campaign_id = re.search('CampaignManagement.+campaignid=(?P<campaign_id>\d+)', response.geturl()).group('campaign_id')
            campaign_id = long(campaign_id)
            
            adgroup_id = re.search('adgroupid=(?P<adgroup_id>\d+)', response.read()).group('adgroup_id')
            adgroup_id = long(adgroup_id)
        except:
            raise UnexpectedResponseError()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
        
        return campaign_id, adgroup_id
    
    
    def add_adgroup(self, campaign_id, adgroup_name, display_url, url, headline, adline1, adline2, keywords, bid):
        '''
        Adds an AdGroup to a given campaign and returns its AdGroup ID
        
        @param campaign_id: long
        @param adgroup_name: str 
        @param display_url: str
        @param url: str
        @param headline: str  
        @param adline1: str  
        @param adline2: str
        @param keywords: list
        @param bid: decimal.Decimal
        @return: long
        '''
        log(' +++ add_adgroup "%s"' % adgroup_name)
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagementDispatcher?campaignid=%d#a' % campaign_id)
        self._fetchurl(request)
        self._do_fake_delay()
        
        # Step 0
        request = self._create_browserlike_request('https://adwords.google.com/select/StartNewAdGroup?campaignId=%d' % campaign_id)
        response = self._fetchurl(request)
        self._do_fake_delay()

        try:
            wizard_key = re.search('TargetingWizard.+wizardKey=(?P<wizard_key>\w+)', response.geturl()).group('wizard_key')
        except:
            raise UnexpectedResponseError()
        
        # Step 1
        request = self._create_browserlike_request('https://adwords.google.com/select/TargetingWizardInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({
            'thisAction': '//TargetingWizard',
            'adGroupName': adgroup_name,
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)

        if re.search('FirstAdTypeFinder', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 2
        request = self._create_browserlike_request('https://adwords.google.com/select/StartCKSRequest?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({
            'thisAction': '//CreateAd',
            'creativeScope': 'textController.textCreative',
            'controllerScope': 'textController',
            'textController.textCreative.headline': headline,
            'textController.textCreative.description1': adline1,
            'textController.textCreative.description2': adline2,
            'textController.textCreative.visibleUrl': display_url,
            'textController.protocol': 'https://' if url.startswith('https://') else 'http://',
            'textController.destUrl': url if url.find('://') == -1 else url[url.find('://') + 3:],
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)

        if re.search('ChooseKeywords', response.geturl()) == None:
            raise UnexpectedResponseError()
            
        # Step 3
        request = self._create_browserlike_request('https://adwords.google.com/select/ChooseKeywordsInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({
            'thisAction': '//ChooseKeywords',
            'akssSuggestedKeywords': '',
            'cksSuggestedKeywords': '',
            'helperSuggestedKeywords': '',
            'keywords': '\x0D\x0A'.join(keywords),
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)

        if re.search('SetPricing', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 4
        request = self._create_browserlike_request('https://adwords.google.com/select/SetPricingInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({                                    
            'initialCurrencyCode': 'USD',
            'usersMaxCpcUnits': '%.2f' % bid,
            'usersMaxContentCpcUnits': '',
            'continueButton': 'Continue \xC2\xBB',
        }))
        response = self._fetchurl(request)

        if re.search('ReviewAccount', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        # Step 5
        request = self._create_browserlike_request('https://adwords.google.com/select/ReviewAccountInput?wizardKey=%s' % wizard_key)
        request.add_data(urllib.urlencode({                                          
            'saveAdgroupButton': 'Save Ad Group',
        }))
        response = self._fetchurl(request)

        try:
            adgroup_id = re.search('CampaignManagement.+adgroupid=(?P<adgroup_id>\d+)', response.geturl()).group('adgroup_id')
            adgroup_id = long(adgroup_id)
        except:
            raise UnexpectedResponseError()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
        
        return adgroup_id
    
    
    def delete_adgroup(self, campaign_id, adgroup_id):
        '''
        Deletes adgroup
        
        @param campaign_id: long
        @param adgroup_id: long
        '''
        log(' +++ delete_adgroup')
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d' % (adgroup_id, campaign_id))
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/ModifyAdGroup?url=CampaignManagement&adgroupid=%d&campaignId=%d&mode=deleteadgroup' % (adgroup_id, campaign_id))
        response = self._fetchurl(request)
        self._do_fake_delay()
        
        if re.search('ModifyAdGroup', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()

    
    def get_keywords(self, campaign_id, adgroup_id):
        '''
        Returns current keywords for given domain
        
        @param campaign_id: long
        @param adgroup_id: long
        @return: list
        '''
        log(' +++ get_keywords')
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d' % (adgroup_id, campaign_id))
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/EditKeywords?adgroupid=%d&campaignId=%d#a' % (adgroup_id, campaign_id))
        response = self._fetchurl(request)
        self._do_fake_delay()
        
        try:
            keywords = re.search('<textarea [^>]*name="keywords"[^>]*>(?P<keywords>[^<]*)</textarea>', response.read())
            keywords = keywords.group('keywords')
        except:
            raise UnexpectedResponseError()
 
        result = []
        for keyword_string in keywords.split('\n'):
            splitted = keyword_string.split(' ** ')
            keyword = Keyword(splitted[0])
            if len(splitted) > 1:
                try: keyword.bid = float(splitted[1])
                except ValueError: keyword.url = splitted[1]
            if len(splitted) > 2:
                keyword.url = splitted[2]
            result.append(keyword)
            
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()

        return result

    
    def set_default_bid(self, campaign_id, adgroup_id, bid):
        '''
        Changes the default bid for given AdGroup
        
        @param campaign_id: long
        @param adgroup_id: long
        @param bid: decimal.Decimal
        '''
        log(' +++ set_default_bid')
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d' % (adgroup_id, campaign_id))
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/EditKeywords?adgroupid=%d&campaignId=%d#a' % (adgroup_id, campaign_id))
        response = self._fetchurl(request)
        self._do_fake_delay()
        
        try:
            keywords = re.search('<textarea [^>]*name="keywords"[^>]*>(?P<keywords>[^<]*)</textarea>', response.read())
            keywords = keywords.group('keywords')
        except:
            raise UnexpectedResponseError()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/EditKeywords')
        request.add_data(urllib.urlencode({    
            'campaignId': campaign_id,
            'adgroupid': adgroup_id,
            'price': '%.2f' % bid,
            'priceContent': 'Auto',
            'keywords': keywords,
            'save': 'Save+Changes',
        }))
        response = self._fetchurl(request)
        
        if re.search('CampaignManagement', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
    

    def set_keywords(self, campaign_id, adgroup_id, keywords):
        '''
        Sets (replaces) keywords for given AdGroup
        
        @param campaign_id: long
        @param adgroup_id: long
        @param keywords: list
        '''
        log(' +++ set_keywords')
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d' % (adgroup_id, campaign_id))
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/EditKeywords?adgroupid=%d&campaignId=%d#a' % (adgroup_id, campaign_id))
        response = self._fetchurl(request)
        self._do_fake_delay()
        
        try:
            price = re.search('<input [^>]*name="price"[^>]*value="(?P<price>[^"]+)"', response.read())
            price = price.group('price')
        except:
            raise UnexpectedResponseError()
        
        keywords_processed = []
        for keyword in keywords:
            keywords_processed.append(str(keyword))
        
        request = self._create_browserlike_request('https://adwords.google.com/select/EditKeywords')
        request.add_data(urllib.urlencode({    
            'campaignId': campaign_id,
            'adgroupid': adgroup_id,
            'price': price,
            'priceContent': 'Auto',
            'keywords': '\x0D\x0A'.join(keywords_processed),
            'save': 'Save+Changes',
        }))
        response = self._fetchurl(request)
        
        if re.search('CampaignManagement', response.geturl()) == None:
            raise UnexpectedResponseError()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
        
        
    def get_keywords_report(self, campaign_id, adgroup_id, days=7):
        '''
        Generates a keywords performance report - dict with keys-keywords and
        values - dicts with various rates. If a rate can't be estimated in the
        current context it becomes None.
        
        @param campaign_id: long
        @param adgroup_id: long
        @param days: int
        
        @return: dict    
        '''
        log(' +++ get_keywords_report')
        
        if not self._signed_in:
            raise IncorrectStateError('You have to be signed in to perform this action.')
        if not int(days) > 0:
            raise ValueError('"days" should be an int greater or equal than 1')
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d' % (adgroup_id, campaign_id))
        self._fetchurl(request)
        self._do_fake_delay()
        
        period_end = datetime.datetime.today()
        period_begin = period_end - datetime.timedelta(int(days) - 1)
        
        request_url = 'https://adwords.google.com/select/CampaignManagement?%s' % urllib.urlencode({
            'campaignid': campaign_id,
            'adgroupid': adgroup_id,
            'mode': '',
            'timeperiod': 'date',
            'timeperiod.begin.month': period_begin.month,
            'timeperiod.begin.day': period_begin.day,
            'timeperiod.begin.year': period_begin.year,
            'timeperiod.begin.dateField': period_begin.strftime('%b %d, %Y'),
            'timeperiod.end.month': period_end.month,
            'timeperiod.end.day': period_end.day,
            'timeperiod.end.year': period_end.year,
            'timeperiod.end.dateField': period_end.strftime('%b %d, %Y'),
            'timeperiod.display': 'Go',
            'hideDeleted': '1',
        })
        
        request = self._create_browserlike_request(request_url)
        self._fetchurl(request)
        self._do_fake_delay()

        #setting page to 1
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d&keywordt=0&active_tab=keywordt&advariationst=4&mode=#%d' % (adgroup_id, campaign_id, adgroup_id))
        response = self._fetchurl(request).read()
        self._do_fake_delay()
        
        result = {}
        
        while True:
            for row_match in re.finditer(r'<tr[^>]*?id="tr_\d+"[^>]*?>(?P<data_row>.*?)</tr>', response, re.DOTALL):
                data_row = row_match.group('data_row')
                keyword = re.search('</div>\n</span>\n(?P<keyword>[\w\s\d]+?)</td>\n', data_row).group('keyword')
                bid = re.search('<td nowrap align="center" colspan="2">(?P<bid>[$\d\.-]+?)</td>\n', data_row).group('bid')
                
                data = re.search('<td class="" align="right">(?P<clicks>[\d-]+?)\n' +
                                 '.*?</td>\n+?' +
                                 '<td class="" align="right">(?P<impr>[\d-]+?)\n' +
                                 '.*?</td>\n+?' +
                                 '<td class="" align="right">(?P<ctr>[\d\.-]+?)\n' +
                                 '.*?</td>\n+?' +
                                 '<td class="" align="right">(?P<cpc>[$\d\.-]+?)\n' +
                                 '.*?</td>\n+?' +
                                 '<td class="" align="right">(?P<cost>[$\d\.-]+?)\n' +
                                 '.*?</td>\n+?' +
                                 '<td class="rightcolumn" align="right">(?P<pos>[\d\.-]+?)\n' +
                                 '.*?</td>\n+?', data_row)
                
                result[keyword] = {'bid': bid,
                                   'clicks': int(data.group('clicks')),
                                   'impr': int(data.group('impr')),
                                   'ctr': None if data.group('ctr') == '-' else decimal.Decimal(data.group('ctr')),
                                   'cpc': None if data.group('cpc') == '-' else decimal.Decimal((data.group('cpc'))[1:]),
                                   'cost': None if data.group('cost') == '-' else decimal.Decimal((data.group('cost'))[1:]),
                                   'pos': None if data.group('pos') == '-' else decimal.Decimal(data.group('pos'))}
                              
            next_page = re.search('<a href="(?P<url>[^"]+?)"><b>Next', response)
            if next_page:
                next_page = next_page.group('url').replace('&amp;', '&')
                next_page = 'https://adwords.google.com/select/' + next_page
                request = self._create_browserlike_request(next_page)
                response = self._fetchurl(request).read()
                self._do_fake_delay()
            else:
                break

        #setting page to 1
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignManagement?adgroupid=%d&campaignId=%d&keywordt=0&active_tab=keywordt&advariationst=4&mode=#%d' % (adgroup_id, campaign_id, adgroup_id))
        self._fetchurl(request)
        self._do_fake_delay()
        
        request = self._create_browserlike_request('https://adwords.google.com/select/CampaignSummary')
        self._fetchurl(request)
        self._do_fake_delay()
        
        return result
#-------------------------------------------------------------------------------