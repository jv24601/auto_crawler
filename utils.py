import aiohttp
from throttler import Throttler
from bs4 import BeautifulSoup
from urllib.parse import urlparse,urljoin,urldefrag,urlunparse
import crawler_settings

def is_none_or_empty(val)->bool:
    return(val==None or val=="")

def get_page_name(soup:BeautifulSoup)->str:
    return soup.find('h1').text

def get_starting_url()->str:
    starting_url = crawler_settings.settings.get('starting_url')
    return starting_url

def get_throttle_limit()->int:
    return crawler_settings.settings.get('request_throttle_limit')[0]

def get_max_depth()->int:
    return crawler_settings.settings.get('depth')

# returns a default
def get_batch_size()->int:
    batch_size = crawler_settings.settings.get('batch_size')
    return batch_size

BATCH_SIZE = get_batch_size()

def get_absolute_page_limit()->int:
    return crawler_settings.settings.get('absolute_page_limit')

# gets the base url i.e. the scheme and netloc of the starting page
def get_base_url()->str:
    starting_url = urlparse(crawler_settings.settings.get('starting_url'))

    #return only the scheme and the net location for the url to parse
    base_url = starting_url.scheme+'://'+starting_url.netloc

    return base_url

# gets the setting for should crawl internal links only
def should_crawl_internal_only()->bool:
    return crawler_settings.settings.get('internal_links_only')==True


def url_has_netloc(netloc:str)->bool:
    if(is_none_or_empty(netloc)):
        return False
    else:
        return True
    
def get_links_from_html(html:str)->list[str]:
    if html==None:
        return []
    else:
        return get_links_from_soup(BeautifulSoup(html,'html.parser'))


# get all the links from a Beautiful Soup as urls
def get_links_from_soup(soup:BeautifulSoup)->list[str]:

    links = []

    for link in soup.find_all('a'): 

        found_link = link.get('href')
        parsed_result = urlparse(found_link)
        base_url = urlparse(get_base_url())

        if(not(url_has_netloc(parsed_result.netloc))):
            # use only the scheme, netloc and path for internal links (ignore fragments)
            links.append(urljoin(get_base_url(),parsed_result.path))
        elif(parsed_result.netloc==base_url.netloc or not(should_crawl_internal_only())):
            # if the link is a full ink, we still only want the
            links.append(urljoin(parsed_result.scheme, parsed_result.netloc,parsed_result.path))   

    return list(set(links))

# makes a throttled web request
async def fetch_page(session:aiohttp.ClientSession,throttler:Throttler,url:str)->str: 
    try:
        # make GET request using session
        async with throttler,session.get(url) as response:
            # return HTML content
            html = await response.text()
        return html
    
    except Exception as e:pass
