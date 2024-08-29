from bs4 import BeautifulSoup

# Custom, slow data processing utilities that may not be relevant to your web-scraping, so I am skipping it by default.
USE_PAGETYPE = True

# a quick check for if a beautiful soup is for an album article
def is_soup_album_page(soup):

    al = 'Track listing'

    is_album = (soup.find(string=al) == al)

    return is_album

def get_album_artist(album_soup):

    try:
        artist = album_soup.find(string=' by ').parent.find(class_='contributor').find('a')
        return artist.text, artist.get('href')
    except:
        return None, None
    
# a quick check for if a beautiful soup is for an artist article
def is_soup_artist_page(soup):

    score = 0

    should_have_strings = ('Born','Genres','Background information','Discography','Instrument(s)')
    s1 = 1.0/len(should_have_strings)

    for search_string in should_have_strings:
        if(soup.find(string=search_string)):
            score += s1

    is_artist_page = (score >= .8)

    return is_artist_page

# a quick check for if a beautiful soup is for a genre article
def is_soup_genre_page(soup):

    score = 0
    should_have_strings = ('Stylistic origins','Cultural origins','Derivative forms')
    s1 = 1.0/len(should_have_strings)

    for search_string in should_have_strings:
        if(soup.find(string=search_string)):
            score += s1

    is_genre_page = (score >= .66)

    return is_genre_page

# gets the type of wikipedia page. wikipedia specific, slow, and not guaranteed to be accurate, so off by default
def get_pagetype(html = None,soup = None):

    ret_val = ''

    if(USE_PAGETYPE == False):
        return ret_val

    if((html==None or html =='') and soup==None):
        ret_val = ''
    
    elif(soup == None):
        soup = BeautifulSoup(html,'html.parser')

    if(is_soup_album_page(soup)):
        return 'album'
    elif(is_soup_artist_page(soup)):
        return 'artist'
    elif(is_soup_genre_page(soup)):
        return 'genre'

    return ret_val
