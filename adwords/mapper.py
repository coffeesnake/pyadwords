'''
adwords.mapper

@author: Philip Rud
@version: 0.1.1
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relation
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, Numeric
from sqlalchemy.sql import func, desc

from requestprocessor import RequestProcessor
import settings


engine = create_engine(settings.DB_CONNECTION, echo=settings.DEBUG_DB)
session = sessionmaker(bind=engine)()
Base = declarative_base()


class AdGroup(Base):
    __tablename__ = 'adwords_adgroups'
    
    id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer, ForeignKey('adwords_campaigns.id'))
    set = Column(String(1024))
    default_bid = Column(Numeric())
    default_url = Column(String(1024))
    display_url = Column(String(35))
    headline = Column(String(25))
    adline1 = Column(String(35))
    adline2 = Column(String(35))
    
    def __init__(self, id, campaign_id, set, default_bid, default_url, display_url, headline, adline1, adline2):
        self.id = id
        self.campaign_id = campaign_id
        self.set = set
        self.default_bid = default_bid
        self.default_url = default_url
        self.display_url = display_url
        self.headline = headline
        self.adline1 = adline1
        self.adline2 = adline2
    
    def __repr__(self):
        return '<AdGroup "%s" from set "%s">' % (self.primary_keyword, self.set)
    
    @classmethod
    def find_unique_name(cls, prefix, campaign=None):
        if campaign == None:
            unique_name = '%s__%s' % (prefix, 1)
        else:
            adgroups = session.query(UsedNames) \
                .filter(UsedNames.entity_type == cls.__name__) \
                .filter(UsedNames.entity_parent_id == campaign.id).all()
            adgroups_names = [entry.entity_name for entry in adgroups]
            
            suffix = 0
            unique_suffix_found = False
            
            while not unique_suffix_found:
                suffix += 1
                name = '%s__%s' % (prefix, suffix)
                if name not in adgroups_names:
                    unique_suffix_found = True
            
            unique_name = '%s__%s' % (prefix, suffix)
            
        return unique_name


class Campaign(Base):
    __tablename__ = 'adwords_campaigns'
    
    NAME_PREFIX = 'campaign'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('adwords_accounts.id'))
    adgroups = relation(AdGroup, order_by=AdGroup.id, backref='campaign')
    
    def __init__(self, id, account_id):
        self.id = id
        self.account_id = account_id
    
    def __repr__(self):
        return '<Campaign %s>' % str(self.id)
    
    @classmethod
    def find_unique_name(cls, account):
        campaigns = session.query(UsedNames) \
            .filter(UsedNames.entity_type == cls.__name__) \
            .filter(UsedNames.entity_parent_id == account.id).all()
        campaign_names = [entry.entity_name for entry in campaigns]
            
        suffix = 0
        unique_suffix_found = False
        
        while not unique_suffix_found:
            suffix += 1
            name = '%s__%s' % (cls.NAME_PREFIX, suffix)
            if name not in campaign_names:
                unique_suffix_found = True
        
        unique_name = '%s__%s' % (cls.NAME_PREFIX, suffix)
            
        return unique_name


class Account(Base):
    __tablename__ = 'adwords_accounts'
    
    id = Column(Integer, primary_key=True)
    email = Column(String(250), unique=True)
    password = Column(String(250))
    campaigns = relation(Campaign, order_by=Campaign.id, backref='account')
    
    def __init__(self, email, password):
        self.email = email
        self.password = password
    
    def __repr__(self):
        return '<Account "%s">' % self.email
    
    
class UsedNames(Base):
    __tablename__ = 'adwords_usednames'
    
    id = Column(Integer, primary_key=True)
    entity_type = Column(String(50))
    entity_id = Column(Integer, nullable=True)
    entity_parent_id = Column(Integer, nullable=True)
    entity_name = Column(String(100))
    
    @classmethod
    def add_entity(cls, entity_type, entity_id, entity_parent_id, entity_name):
        used_name = cls()
        used_name.entity_id = entity_id
        used_name.entity_name = entity_name
        used_name.entity_parent_id = entity_parent_id
        used_name.entity_type = entity_type
        
        session.add(used_name)
        
    @classmethod
    def remove_entity(cls, entity_type, entity_id):
        used_name = session.query(cls) \
            .filter(cls.entity_type == entity_type) \
            .filter(cls.entity_id == entity_id) \
            .one()
        used_name.id = None
        
        session.add(used_name)
        
    @classmethod
    def get_entity_name(cls, entity_type, entity_id):
        used_name = session.query(cls) \
            .filter(cls.entity_type == entity_type) \
            .filter(cls.entity_id == entity_id) \
            .one()
        
        return used_name.entity_name


def preprocess_url(url, campaign_name=None, adgroup_name=None):
    import urllib
    
    result = url
    
    if campaign_name:
        result = result.replace('%%campaign%%', urllib.quote(campaign_name))
    if adgroup_name:
        result = result.replace('%%adgroup%%', urllib.quote(adgroup_name))
    
    return result


def preprocess_keywords(keywords, campaign_name=None, adgroup_name=None, plain=False):
    for keyword in keywords:
        if isinstance(keyword, Keyword):
            if plain:
                yield keyword.keyword
            else:
                url = keyword.url
                if url != None:
                    url = preprocess_url(url, campaign_name, adgroup_name)
                yield Keyword(keyword.keyword, keyword.bid, url)
        else:
            yield keyword

#-------------------------------------------------------------------------------
# Public API
#-------------------------------------------------------------------------------

from requestprocessor import Keyword


def install():
    '''
    Creates a new db schema in an empty database
    '''
    Base.metadata.create_all(engine)


def add_account(email, password):
    '''
    Adds a new empty account to the mapper.
    
    @param email: str
    @param password: str  
    
    @return: Account
    '''
    account = Account(email, password)
    session.add(account)
    session.commit()
    
    return account

def remove_account(email):
    '''
    Removes account and related data from the mapper database.
    
    @param email: str
    '''
    conn = session.connection()
    
    account = session.query(Account).filter(Account.email == email).one()
    campaigns = session.query(Campaign).join(Account).filter(Account.id == account.id).all()
    
    for campaign in campaigns:
        conn.execute(AdGroup.__table__.delete().where(AdGroup.__table__.c.campaign_id == campaign.id))
    conn.execute(Campaign.__table__.delete().where(Campaign.__table__.c.account_id == account.id))
    session.delete(account)
    
    session.commit()
    
    
def get_account_capacity(email):
    '''
    Returns a list. Each element specifies how many adgroups (set parts) can be
    created per each campaign (maybe not existing yet) of the account.
    
    The list is reverse sorted so one can only compare count of slices needed
    to store a set with the first element of a list to ensure the account is 
    able to store a set.
    
    @param email: str
    
    @return: list
    '''
    account = session.query(Account).filter(Account.email == email).first()
    if not account:
        raise ValueError('Account "%s" not found' % email)
    
    adgroups_left = [settings.MAX_ADGROUPS_PER_CAMPAIGN] \
        * (settings.MAX_CAMPAIGNS_PER_ACCOUNT - len(account.campaigns))

    adgroups_left += [settings.MAX_ADGROUPS_PER_CAMPAIGN - len(campaign.adgroups) \
        for campaign in account.campaigns]
    
    adgroups_left.sort(reverse=True)
    
    return adgroups_left


def get_capacity(set):
    '''
    Returns max number of keywords can be put into the set using 
    modify_keywords() function
    
    @param set: str
    '''
    an_adgroup = session.query(AdGroup).filter(AdGroup.set == set).first()
    
    if not an_adgroup:
        raise ValueError('Set "%s" not found' % set)
    
    campaign_adgroups_count = session.query(AdGroup) \
        .filter(AdGroup.campaign_id == an_adgroup.campaign.id).count()
    set_adgroups_count = session.query(AdGroup) \
        .filter(AdGroup.set == set).count()
    
    capacity = settings.MAX_ADGROUPS_PER_CAMPAIGN 
    capacity -= (campaign_adgroups_count - set_adgroups_count)
    capacity *= settings.MAX_KEYWORDS_PER_ADGROUP
    
    return capacity


def clone_account(email_source, email_dest):
    '''
    Clones an account identified by its email to another one.
    
    Destination account should be empty (or at least to be capable enough
    to store all source account data). Removes source account after cloning.
    
    @param email_source: str
    @param email_dest: str
    '''
    account_from = session.query(Account).filter(Account.email == email_source).one()
    account_to = session.query(Account).filter(Account.email == email_dest).one()
    
    processor_from = RequestProcessor(account_from.email, account_from.password)
    processor_from.sign_in()
    
    processor_to = RequestProcessor(account_to.email, account_to.password)
    processor_to.sign_in()
    
    campaigns = session.query(Campaign).join(AdGroup).group_by(Campaign) \
        .filter(Campaign.account_id == account_from.id).all()
        
    for campaign in campaigns:
        adgroups = session.query(AdGroup).filter(AdGroup.campaign_id == campaign.id).all()
        first_adgroup = adgroups.pop(0)
        first_adgroup_keywords = processor_from.get_keywords(campaign.id, first_adgroup.id)
        
        new_campaign_name = Campaign.find_unique_name(account_to)
        new_adgroup_name = AdGroup.find_unique_name(first_adgroup.set)
        new_campaign_id, new_adgroup_id = processor_to.add_campaign(
            new_campaign_name, new_adgroup_name,
            first_adgroup.display_url, preprocess_url(first_adgroup.default_url, new_campaign_name, new_adgroup_name), 
            first_adgroup.headline, first_adgroup.adline1, first_adgroup.adline2, 
            preprocess_keywords(first_adgroup_keywords, plain=True), first_adgroup.default_bid
        )
        UsedNames.add_entity(Campaign.__name__, new_campaign_id, account_to.id, new_campaign_name)
        UsedNames.add_entity(AdGroup.__name__, new_adgroup_id, new_campaign_id, new_adgroup_name)
        
        processor_to.set_keywords(new_campaign_id, new_adgroup_id, preprocess_keywords(first_adgroup_keywords, new_campaign_name, new_adgroup_name))
        
        session.add(Campaign(long(new_campaign_id), account_to.id))
        session.add(AdGroup(long(new_adgroup_id), long(new_campaign_id), first_adgroup.set, 
                first_adgroup.default_bid, first_adgroup.default_url, first_adgroup.display_url, 
                first_adgroup.headline, first_adgroup.adline1, first_adgroup.adline2))
        
        for adgroup in adgroups:
            adgroup_keywords = processor_from.get_keywords(campaign.id, adgroup.id)
            
            campaign_name = UsedNames.get_entity_name(Campaign.__name__, campaign.id)
            new_adgroup_name = AdGroup.find_unique_name(adgroup.set, campaign)
            new_adgroup_id = processor_to.add_adgroup(
                campaign.id, new_adgroup_name, adgroup.display_url, preprocess_url(adgroup.default_url, campaign_name, new_adgroup_name), 
                adgroup.headline, adgroup.adline1, adgroup.adline2, 
                preprocess_keywords(adgroup_keywords, plain=True), adgroup.default_bid
            )
            UsedNames.add_entity(AdGroup.__name__, new_adgroup_id, campaign.id, new_adgroup_name)
            processor_to.set_keywords(campaign.id, adgroup.id, preprocess_keywords(first_adgroup_keywords, campaign_name, new_adgroup_name))
            
            session.add(AdGroup(long(new_adgroup_id), campaign.id, adgroup.set,
                adgroup.default_bid, adgroup.default_url, adgroup.display_url, 
                adgroup.headline, adgroup.adline1, adgroup.adline2))
            
    session.commit()
    processor_from.sign_out()
    processor_to.sign_out()
    remove_account(email_source)


def create_set(set, display_url, default_bid, default_url, headline, adline1, adline2, keywords, account_email=None):
    '''
    Creates a new keywords set.
    
    The current strategy is to prefer filling up existing campaigns/accounts
    instead of creating new ones. 'keywords' should be a list of strings and/or
    Keyword instances.
    
    Optional account argument can be used to explictly specify which account
    should be used to store a new set. OverflowError will be raised in case
    there's not enough capacity there to store a new set.
    
    @param set: str
    @param display_url: str
    @param default_bid: decimal.Decimal
    @param default_url: str
    @param headline: str
    @param adline1: str
    @param adline2: str
    @param keywords: list
    @param account_email: Account
    
    @return: Account
    '''
    
    if session.query(AdGroup).filter(AdGroup.set == set).count() > 0:
        raise ValueError('Set "%s" already exists' % set)

    if account_email:
        account = session.query(Account).filter(Account.email == account_email).first()
        if not account:
            raise ValueError('Account "%s" not found' % account_email)
    
    processor = None
    
    # getting how many parts should be created
    parts_count = int(len(keywords)) / int(settings.MAX_KEYWORDS_PER_ADGROUP)
    if int(len(keywords)) % int(settings.MAX_KEYWORDS_PER_ADGROUP) != 0:
        parts_count += 1
    if account_email and (parts_count > get_account_capacity(account_email)[0]):
        raise OverflowError('Specified account is not capable enough to store a set')
    
    # looking for campaigns capable to store required number of parts
    campaigns = session.query(Campaign).outerjoin(AdGroup) \
        .group_by(Campaign) \
        .having(func.count('*') <= (settings.MAX_ADGROUPS_PER_CAMPAIGN - parts_count)) \
        .order_by(desc(func.count('*')))
    if account_email:
        campaigns = campaigns.having(Campaign.account_id == account.id)
    
    if campaigns.count() > 0:
        # campaign found
        campaign = campaigns.first()
    else:
        # creating a new campaign
        # looking for an account to use
        if not account_email:
            accounts = session.query(Account).outerjoin(Campaign) \
                .group_by(Account) \
                .having(func.count('*') < settings.MAX_CAMPAIGNS_PER_ACCOUNT) \
                .order_by(desc(func.count('*')))
                
            if accounts.count() > 0:
                account = accounts.first()
            else:
                raise OverflowError('limits exceeded during new set creation')
        
        processor = RequestProcessor(account.email, account.password)
        processor.sign_in()
        
        keywords_part = keywords[:settings.MAX_KEYWORDS_PER_ADGROUP]
        
        new_campaign_name = Campaign.find_unique_name(account)
        new_adgroup_name = AdGroup.find_unique_name(set)
        new_campaign_id, new_adgroup_id = processor.add_campaign(
            new_campaign_name, new_adgroup_name, display_url, preprocess_url(default_url, new_campaign_name, new_adgroup_name), headline, adline1, adline2, 
            preprocess_keywords(keywords_part, plain=True), default_bid
        )
        UsedNames.add_entity(Campaign.__name__, new_campaign_id, account.id, new_campaign_name)
        UsedNames.add_entity(AdGroup.__name__, new_adgroup_id, new_campaign_id, new_adgroup_name)
        processor.set_keywords(new_campaign_id, new_adgroup_id, preprocess_keywords(keywords_part, new_campaign_name, new_adgroup_name))
        
        campaign = Campaign(long(new_campaign_id), account.id)
        session.add(campaign)
        
        adgroup = AdGroup(long(new_adgroup_id), long(new_campaign_id), set,
            default_bid, default_url, display_url, headline, adline1, adline2)
        session.add(adgroup)
        
        keywords = keywords[settings.MAX_KEYWORDS_PER_ADGROUP:]
        
    # now we have a campaign to put the rest of set parts into
    if not processor:
        processor = RequestProcessor(campaign.account.email, campaign.account.password)
        processor.sign_in()

    # creating the rest of set parts
    while len(keywords) > 0:
        keywords_part = keywords[:settings.MAX_KEYWORDS_PER_ADGROUP]
        
        campaign_name = UsedNames.get_entity_name(Campaign.__name__, campaign.id)        
        new_adgroup_name = AdGroup.find_unique_name(set, campaign)
        new_adgroup_id = processor.add_adgroup(
            campaign.id, new_adgroup_name, display_url, preprocess_url(default_url, campaign_name, new_adgroup_name), headline, adline1, adline2, 
            preprocess_keywords(keywords_part, plain=True), default_bid
        )
        UsedNames.add_entity(AdGroup.__name__, new_adgroup_id, campaign.id, new_adgroup_name)
        processor.set_keywords(campaign.id, new_adgroup_id, preprocess_keywords(keywords_part, campaign_name, new_adgroup_name))
        
        adgroup = AdGroup(long(new_adgroup_id), campaign.id, set,
            default_bid, default_url, display_url, headline, adline1, adline2)
        session.add(adgroup)
        
        keywords = keywords[settings.MAX_KEYWORDS_PER_ADGROUP:]

    processor.sign_out()
    session.commit()
    
    return campaign.account
    
    
def drop_set(set):
    '''
    Deletes the current set of keywords with related AdGroups.
    
    @param set: str
    '''
    adgroups = session.query(AdGroup).filter(AdGroup.set == set)
    
    if adgroups.count() == 0:
        raise ValueError('Set "%s" not found' % set)
    else:
        campaign = adgroups.first().campaign
        
        processor = RequestProcessor(campaign.account.email, campaign.account.password)
        processor.sign_in()
        
        for adgroup in adgroups.all():
            processor.delete_adgroup(campaign.id, adgroup.id)
            UsedNames.remove_entity(AdGroup.__name__, adgroup.id)
            session.delete(adgroup)
        
        session.commit()
        processor.sign_out()

    
def get_keywords(set):
    '''
    Returns list of Keyword instances from given set.
    
    @param set: str
    
    @return: list
    '''
    adgroups = session.query(AdGroup).filter(AdGroup.set == set)
    
    if adgroups.count() == 0:
        raise ValueError('Set "%s" not found' % set)
    else:
        campaign = adgroups.first().campaign
        processor = RequestProcessor(campaign.account.email, campaign.account.password)
        processor.sign_in()
        
        keywords = []
        for adgroup in adgroups.all():
            keywords += processor.get_keywords(campaign.id, adgroup.id)
        
        processor.sign_out()
    
    return keywords


def modify_keywords(set, new_keywords):
    '''
    Resubmits the keywords list of a given set with a new one.
    
    List can contain both strings and Keyword instances
    
    @param set: str
    @param keywords: list
    ''' 
    adgroups = session.query(AdGroup).filter(AdGroup.set == set)
    
    if adgroups.count() == 0:
        raise ValueError('Set "%s" not found' % set)
    if len(new_keywords) == 0:
        raise ValueError('"new_keywords" should not be empty')
    
    campaign = adgroups.first().campaign
    processor = RequestProcessor(campaign.account.email, campaign.account.password)
    processor.sign_in()
    
    an_adgroup = adgroups.first()
    adgroups = adgroups.all()
    
    while True:
        adgroup = adgroups.pop(0)
        keywords_part = new_keywords[:settings.MAX_KEYWORDS_PER_ADGROUP]
        new_keywords = new_keywords[settings.MAX_KEYWORDS_PER_ADGROUP:]
        
        campaign_name = UsedNames.get_entity_name(Campaign.__name__, campaign.id)
        adgroup_name = UsedNames.get_entity_name(AdGroup.__name__, adgroup.id)
        processor.set_keywords(campaign.id, adgroup.id, preprocess_keywords(keywords_part, campaign_name, adgroup_name))
        
        if len(adgroups) == 0 or len(new_keywords) == 0:
            break
        
    if len(adgroups) > 0:
        for adgroup_to_remove in adgroups:
            processor.delete_adgroup(campaign.id, adgroup_to_remove.id)
            session.delete(adgroup_to_remove)
            
    while len(new_keywords) > 0:
        if not (len(campaign.adgroups) < settings.MAX_ADGROUPS_PER_CAMPAIGN):
            raise OverflowError('limits exceeded during modifying keywords')
        
        new_adgroup_name = AdGroup.find_unique_name(set, campaign)
        campaign_name = UsedNames.get_entity_name(Campaign.__name__, campaign.id)
        new_adgroup_id = processor.add_adgroup(
            campaign.id, new_adgroup_name, an_adgroup.display_url, preprocess_url(an_adgroup.default_url, campaign_name, new_adgroup_name), 
            an_adgroup.headline, an_adgroup.adline1, an_adgroup.adline2, 
            preprocess_keywords(new_keywords[:settings.MAX_KEYWORDS_PER_ADGROUP], plain=True), an_adgroup.default_bid
        )
        UsedNames.add_entity(AdGroup.__name__, new_adgroup_id, campaign.id, new_adgroup_name)
        processor.set_keywords(campaign.id, new_adgroup_id, preprocess_keywords(new_keywords[:settings.MAX_KEYWORDS_PER_ADGROUP], campaign_name, new_adgroup_name))
        
        adgroup = AdGroup(long(new_adgroup_id), campaign.id, set,
            an_adgroup.default_bid, an_adgroup.default_url, an_adgroup.display_url, 
            an_adgroup.headline, an_adgroup.adline1, an_adgroup.adline2)
        session.add(adgroup)
        
        new_keywords = new_keywords[settings.MAX_KEYWORDS_PER_ADGROUP:]
        
    session.commit()
    processor.sign_out()


def change_default_bid(set, bid):
    '''
    Changes default bid value of a set
    
    @param set: str
    @param bid: Decimal
    '''
    adgroups = session.query(AdGroup).filter(AdGroup.set == set)
    
    if adgroups.count() == 0:
        raise ValueError('Set "%s" not found' % set)
    else:
        campaign = adgroups.first().campaign
        processor = RequestProcessor(campaign.account.email, campaign.account.password)
        processor.sign_in()
        
        for adgroup in adgroups.all():
            processor.set_default_bid(campaign.id, adgroup.id, bid)
        
        processor.sign_out()


def report_set_performance(set, days=7):
    '''
    Returns keywords performance report of given set for specified days count
    as a dictionary with keywords as keys and their perf values as the values
    of that dict.
    
    Some values may appear as None in case the value cannot be
    calculated yet.
    
    @param set: str
    @param days: int
    
    @return: dict
    '''
    adgroups = session.query(AdGroup).filter(AdGroup.set == set)
    
    if days < 1:
        raise ValueError('Days cannot be %d' % days)
    if adgroups.count() == 0:
        raise ValueError('Set "%s" not found' % set)
    
    campaign = adgroups.first().campaign
    processor = RequestProcessor(campaign.account.email, campaign.account.password)
    processor.sign_in()
    
    keywords = {}
    for adgroup in adgroups.all():
        keywords.update(processor.get_keywords_report(campaign.id, adgroup.id, days))
    
    processor.sign_out()
    
    return keywords
#-------------------------------------------------------------------------------