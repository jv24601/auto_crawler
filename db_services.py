import sqlite3
import sqlalchemy as db
from urllib.parse import urljoin,urlparse
from sqlalchemy_utils import database_exists, create_database
from utils import BATCH_SIZE

# initialize database: this will connect to an sqlite3 db by default or connect to a db url

def get_connection(url: str|None = None)->db.Connection|None:

    if(url==None):
        url = 'sqlite:///auto_crawler.sqlite'
    
    if(not(database_exists(url))):
       create_database(url)

    engine = db.create_engine(url, echo=False)
    conn = engine.connect()
    metadata = db.MetaData()

    _init_db(metadata,conn)

    conn = engine.connect()

    return conn,engine


def _init_db(metadata:db.MetaData,conn:db.Connection):

    webpage = db.Table('webpage', metadata, 
                       db.Column('url',db.Text(),primary_key=True),
                       db.Column('html',db.Text()),
                       db.Column('depth',db.Integer()),
                       db.Column('parsed',db.Boolean())
                       ) #Table object
    
    linkmap = db.Table('linkmap',metadata,
                    db.Column('source',db.Text()),
                    db.Column('destination',db.Text())
    )
    
    metadata.create_all(conn)

def get_table_by_name(name:str):
    conn,engine = get_connection()

    metadata = db.MetaData()
    metadata.reflect(bind=engine)

    conn.close()
    return metadata.tables[name]


def get_current_depth(conn)->int:


    depth = conn.execute(db.text('SELECT MAX(depth) FROM webpage')).fetchone()[0]

    return depth

def get_current_total_pages(conn)->int:
    total = conn.execute(db.text('SELECT COUNT(url) FROM webpage')).fetchone()[0]
    return total
        
        


webpage = get_table_by_name('webpage')
linkmap = get_table_by_name('linkmap')

def insert_webpage_or_do_nothing(url,html,depth,parsed,conn):

    insert_query = db.dialects.sqlite.insert(webpage).values(url=url,html=html,depth=depth,parsed=parsed)
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])

    conn.execute(do_nothing_stmt)

def insert_webpages_or_do_nothing(url,html,depth,parsed,conn):

    insert_query = db.dialects.sqlite.insert(webpage).values(url=url,html=html,depth=depth,parsed=parsed)
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])

    conn.execute(do_nothing_stmt)

def insert_linkmapping(source_url,destination_link,conn):
    insert_query = db.insert(linkmap).values(source=source_url,destination=destination_link)
    conn.execute(insert_query)

def update_urls_as_parsed(urls,conn):
    if(len(urls)>0):
        update_query = db.update(webpage).where(webpage.c.url.in_(urls)).values(parsed=True)
        conn.execute(update_query)

def get_batch_of_webpages(depth,conn):
    
    query = webpage.select().where(db.and_(webpage.c.depth==depth,webpage.c.parsed==False)).limit(BATCH_SIZE)
    batch = conn.execute(query).fetchall()
    return batch

if __name__=='__main__':
    conn,engine = get_connection()

    print(repr(get_table_by_name('webpage')))

    #update_urls_as_parsed(['asf','asdfwef','https://en.wikipedia.org/wiki/List_of_jazz_albums'],None)
    print('batch',get_batch_of_webpages(0,conn))

    print(conn.execute(db.text('SELECT COUNT(url) FROM webpage')).fetchone())

    conn.close()