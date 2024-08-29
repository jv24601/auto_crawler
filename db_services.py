import sqlite3
import sqlalchemy as db
from urllib.parse import urljoin,urlparse
from sqlalchemy_utils import database_exists, create_database
from utils import BATCH_SIZE,get_page_name
from data_utils import get_pagetype
from bs4 import BeautifulSoup
import aiohttp
from throttler import Throttler

# initialize database: this will connect to an sqlite3 db by default or connect to a db url
def get_connection(url: str | None = None)->db.Connection | None:

    if(url==None):
        url = 'sqlite:///jazz_crawler.sqlite'


    # creates the database if it does not exist
    if(not(database_exists(url))):
       create_database(url)

    engine = db.create_engine(url, echo=False)
    conn = engine.connect()
    metadata = db.MetaData()

    # initializes the database with our a schema appropriate for web-scraping
    _init_db(metadata,conn)

    # gets a connection to the database
    conn = engine.connect()

    return conn,engine

# initializes the database with a fixed schema
def _init_db(metadata:db.MetaData,conn:db.Connection):

    webpage = db.Table('webpage', metadata, 
                       db.Column('url',db.Text(),primary_key=True),
                       db.Column('html',db.Text()),
                       db.Column('depth',db.Integer()),
                       db.Column('parsed',db.Boolean()),
                       db.Column('name',db.Text()),
                       db.Column('text',db.Text()),
                       db.Column('pagetype',db.Text())
                       ) # table object to represent page data
    
    linkmap = db.Table('linkmap',metadata,
                    db.Column('source',db.Text()),
                    db.Column('destination',db.Text())
    ) # table to represent links from one page to another

    rank = db.Table('rank',metadata,
        db.Column('url',db.Text(),primary_key=True),
        db.Column('rank',db.Integer())
    ) # table to count the number of links to a page

    connectedpages = db.Table('connectedpages',metadata,
    db.Column('url',db.Text(),primary_key=True),
    db.Column('strength',db.Integer())
    ) # a table like rank to measure the relevance of a page 
    
    # creates the tables in the database. does nothing if the tables already exist
    metadata.create_all(conn)

# a helper to maintain references to our tables in code
def _get_table_by_name(name:str)->db.Table:
    conn,engine = get_connection()

    metadata = db.MetaData()
    metadata.reflect(bind=engine)

    conn.close()
    return metadata.tables[name]


# wraps query used to track the progress of the web crawler
def get_current_depth(conn: db.Connection)->int:

    # the 'depth' of the breadth-first web-crawling search where 
    # depth = the most number of link clicks required to get to any page in the database from the starting url
    depth = conn.execute(db.text('SELECT MAX(depth) FROM webpage')).fetchone()[0]

    return depth

# wraps query used to track the progress of the web crawler
def get_current_total_pages(conn: db.Connection)->int:
    # total number of pages
    total = conn.execute(db.text('SELECT COUNT(url) FROM webpage')).fetchone()[0]
    return total
        
# get references to tables
webpage = _get_table_by_name('webpage')
linkmap = _get_table_by_name('linkmap')
rank = _get_table_by_name('rank')
connectedpages = _get_table_by_name('connectedpages')

# a utility function to add new columns on the fly for custom data processing
def add_column_to_table( table:str,column:str,data_type:str,conn:db.Connection|None = None):

    should_close = False
    if(conn==None):
        conn, engine =get_connection()
        should_close = True

    try:
        conn.execute(db.text(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {data_type}'))
        conn.commit()
    except:
        pass

    if(should_close):
        conn.close()

# inserts a webpage into the database, skips on conflict
def insert_webpage_or_do_nothing(url:str,html:str,depth:int,parsed:bool,name:str,conn: db.Connection):

    insert_query = db.dialects.sqlite.insert(webpage).values(url=url,html=html,depth=depth,parsed=parsed,name=name)
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])

    conn.execute(do_nothing_stmt)

# inserts a link into the linkmap table - used when a hyperlink is found on a page
def insert_linkmapping(source_url:str,destination_link:str,conn: db.Connection):
    insert_query = db.insert(linkmap).values(source=source_url,destination=destination_link)
    conn.execute(insert_query)

# updates the status of a row in webpage to indicate it has been parsed in our breadth-first search
def update_urls_as_parsed(urls:list[str],conn:db.Connection):
    if(len(urls)>0):
        update_query = db.update(webpage).where(webpage.c.url.in_(urls)).values(parsed=True)
        conn.execute(update_query)

# gets a certain number of web pages from the database. 
# ideally, BATCH_SIZE is small enough they all fit in memory but
# large enough that we max out the throttler by making enough simultaneous requests
def get_batch_of_webpages(depth:int,conn: db.Connection)->list[any]:
    
    query = webpage.select().where(db.and_(webpage.c.depth==depth,webpage.c.parsed==False)).limit(BATCH_SIZE)
    batch = conn.execute(query).fetchall()
    return batch

# runs query to count how many pages link to a given url and commits the results
def _init_ranks(conn: db.Connection):

    insert_query = db.dialects.sqlite.insert(rank).from_select(['url','rank'],db.select(linkmap.c.destination,db.func.count(linkmap.c.destination)).group_by(linkmap.c.destination))
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])

    conn.execute(do_nothing_stmt)
    conn.commit()

# custom data processing. extracts the page name, page text, and page type / categoy from the html.
# looping on this function takes lots of time, so a nice future update would be to parallelize these processes.
# dask, spark, or custom multiprocessing might be appropriate
def _update_names_text_type(conn: db.Connection):

    n_updates = 0

    for url,html in conn.execute(db.select(webpage.c.url,webpage.c.html).where(db.or_(webpage.c.name == '',webpage.c.pagetype==''))):
        if(not(html==None) and not(html=='')):
            
            try:
                soup = BeautifulSoup(html,'html.parser')
                name = get_page_name(soup)
                text = soup.get_text()
                pagetype = get_pagetype(soup=soup)
                conn.execute(db.update(webpage).where(webpage.c.url == url).values(name=name,text=text,pagetype=pagetype))
                n_updates+=1
                if(n_updates%100==0):
                    print(n_updates, ' pages updated.')
                    #print(pagetype)
                    conn.commit()
                
            except Exception as e:
                #print(e)
                pass
            


# like rank, a single query sets up the connected pages derived table
def _init_connectedpages(conn: db.Connection):


    # alias the table to join it with itself
    linkmap1 = db.orm.aliased(linkmap)
    linkmap2 = db.orm.aliased(linkmap)

    connectedpages_query = db.select(linkmap1.c.source,db.func.count(linkmap1.c.source)).join(linkmap2,linkmap2.c.source == linkmap1.c.destination).where(linkmap1.c.source==linkmap2.c.destination).group_by(linkmap1.c.source)

    insert_query = db.dialects.sqlite.insert(connectedpages).from_select(['url','strength'],connectedpages_query)
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])
    
    conn.execute(do_nothing_stmt)
    conn.commit()

# update the derived tables after the raw request data has finished populating from the search
def init_downstream_cache(conn: db.Connection):
    _init_ranks(conn)
    _update_names_text_type(conn)
    _init_connectedpages(conn)

# clear out the derived tables
def breakdown_generated_tables(conn: db.Connection):
    rank.drop(conn)
    connectedpages.drop(conn)
    metadata = db.MetaData()
    _init_db(metadata,conn)

# a background connection to a cache of previously fetched pages, in case you update the program, add columns, or so on...
# and you don't want to clog up the site you are crawling again for pages you already downloaded 
conn_c,engine_c = get_connection('sqlite:///cache.sqlite')

# makes a throttled web request
async def fetch_page_cached(session:aiohttp.ClientSession,throttler:Throttler,url:str)->str: 
    html = None
    try:
        html_query_result = conn_c.execute(db.text(f'SELECT html FROM webpage WHERE url==\'{url}\'')).fetchall()
        #print(html_query_result)
        if(len(html_query_result)==1):
            html = html_query_result[0][0] 
        # make GET request using session
        async with throttler,session.get(url) as response:
            # return HTML content
            if(html==None):
                html = await response.text()

        return html
    
    except Exception as e:
        pass

# maybe not needed, but we can close the cache connection on the crawler if we want
def close_cache():
    conn_c.close()

if __name__=='__main__':

    close_cache()


