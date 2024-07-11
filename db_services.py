import sqlite3
import sqlalchemy as db
from urllib.parse import urljoin,urlparse
from sqlalchemy_utils import database_exists, create_database
from utils import BATCH_SIZE,get_page_name
from bs4 import BeautifulSoup

# initialize database: this will connect to an sqlite3 db by default or connect to a db url

def get_connection(url: str | None = None)->db.Connection | None:

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
                       db.Column('parsed',db.Boolean()),
                       db.Column('name',db.Text()),
                       db.Column('text',db.Text())
                       ) #Table object
    
    linkmap = db.Table('linkmap',metadata,
                    db.Column('source',db.Text()),
                    db.Column('destination',db.Text())
    )

    rank = db.Table('rank',metadata,
        db.Column('url',db.Text(),primary_key=True),
        db.Column('rank',db.Integer())
    )

    connectedpages = db.Table('connectedpages',metadata,
    db.Column('url',db.Text(),primary_key=True),
    db.Column('strength',db.Integer())
    )
    
    
    metadata.create_all(conn)

def _get_table_by_name(name:str)->db.Table:
    conn,engine = get_connection()

    metadata = db.MetaData()
    metadata.reflect(bind=engine)

    conn.close()
    return metadata.tables[name]


def get_current_depth(conn: db.Connection)->int:


    depth = conn.execute(db.text('SELECT MAX(depth) FROM webpage')).fetchone()[0]

    return depth

def get_current_total_pages(conn: db.Connection)->int:
    total = conn.execute(db.text('SELECT COUNT(url) FROM webpage')).fetchone()[0]
    return total
        

webpage = _get_table_by_name('webpage')
linkmap = _get_table_by_name('linkmap')
rank = _get_table_by_name('rank')
connectedpages = _get_table_by_name('connectedpages')

def insert_webpage_or_do_nothing(url:str,html:str,depth:int,parsed:bool,name:str,conn: db.Connection):

    insert_query = db.dialects.sqlite.insert(webpage).values(url=url,html=html,depth=depth,parsed=parsed,name=name)
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])

    conn.execute(do_nothing_stmt)

def insert_linkmapping(source_url:str,destination_link:str,conn: db.Connection):
    insert_query = db.insert(linkmap).values(source=source_url,destination=destination_link)
    conn.execute(insert_query)

def update_urls_as_parsed(urls:list[str],conn:db.Connection):
    if(len(urls)>0):
        update_query = db.update(webpage).where(webpage.c.url.in_(urls)).values(parsed=True)
        conn.execute(update_query)

def get_batch_of_webpages(depth:int,conn: db.Connection)->list[any]:
    
    query = webpage.select().where(db.and_(webpage.c.depth==depth,webpage.c.parsed==False)).limit(BATCH_SIZE)
    batch = conn.execute(query).fetchall()
    return batch

def _init_ranks(conn: db.Connection):

    insert_query = db.dialects.sqlite.insert(rank).from_select(['url','rank'],db.select(linkmap.c.destination,db.func.count(linkmap.c.destination)).group_by(linkmap.c.destination))
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])

    conn.execute(do_nothing_stmt)
    conn.commit()

def _update_names_and_text(conn: db.Connection):

    n_updates = 0

    for url,html in conn.execute(db.select(webpage.c.url,webpage.c.html).where(webpage.c.name == '')):
        if(not(html==None) and not(html=='')):
            
            try:
                soup = BeautifulSoup(html,'html.parser')
                name = get_page_name(soup)
                text = soup.get_text()
                conn.execute(db.update(webpage).where(webpage.c.url == url).values(name=name,text=text))
                n_updates+=1
            except:
                pass
            if(n_updates%100==0):
                print(n_updates, ' page names updated.')
                conn.commit()

def _init_connectedpages(conn: db.Connection):


    linkmap1 = db.orm.aliased(linkmap)
    linkmap2 = db.orm.aliased(linkmap)

    connectedpages_query = db.select(linkmap1.c.source,db.func.count(linkmap1.c.source)).join(linkmap2,linkmap2.c.source == linkmap1.c.destination).where(linkmap1.c.source==linkmap2.c.destination).group_by(linkmap1.c.source)

    insert_query = db.dialects.sqlite.insert(connectedpages).from_select(['url','strength'],connectedpages_query)
    do_nothing_stmt = insert_query.on_conflict_do_nothing(index_elements=['url'])
    
    conn.execute(do_nothing_stmt)
    conn.commit()

def init_downstream_cache(conn: db.Connection):
    _init_ranks(conn)
    _update_names_and_text(conn)
    _init_connectedpages(conn)

def breakdown_generated_tables(conn: db.Connection):
    rank.drop(conn)
    connectedpages.drop(conn)
    metadata = db.MetaData()
    _init_db(metadata,conn)

if __name__=='__main__':
    conn,engine = get_connection()

    #print(rank.insert().from_select())
    #_init_connectedpages(conn)
    #res = conn.execute(db.text('SELECT T1.source, COUNT(T1.source) as rank FROM linkmap T1 LEFT JOIN linkmap T2 ON T1.destination = T2.source WHERE T1.source = T2.destination AND T1.source != T1.destination GROUP BY T1.source ORDER BY rank DESC')).fetchall()
    #breakdown_generated_tables(conn)
    
    #init_downstream_cache(conn)
    print(len(conn.execute(connectedpages.select().order_by(connectedpages.c.strength.desc())).fetchall()))

    #print(res)
    #print(len(res))
    #print(conn.execute(webpage.select().limit(10)).fetchall())
    #print(conn.execute(db.text('SELECT * FROM rank ORDER BY rank DESC limit 100')).fetchall())

    conn.close()