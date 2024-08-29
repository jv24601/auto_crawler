from utils import *
from db_services import *

import sqlalchemy as db
from throttler import Throttler
from bs4 import BeautifulSoup
import time

# processes a batch of pages of the same depth in a breadth-first web crawling algorithm
async def process_batch(depth:int,throttler: Throttler,batch:list[any],conn:db.Connection):
    t = time.time()

    # fetches the html data from the urls in the batch in an async session
    async with aiohttp.ClientSession() as session:
        
        source_urls = [row[0] for row in batch]

        update_urls_as_parsed(source_urls,conn)

        destination_links = []
        fetch_urls = []

        
        for i in range(len(batch)):
            source_url = batch[i][0]
            source_html = batch[i][1]

            destination_links = get_links_from_html(source_html)

            for destination_link in destination_links:
                insert_linkmapping(source_url,destination_link,conn)

            fetch_urls += destination_links

        tasks = []

        # loop through URLs and append tasks
        for url in fetch_urls:
            tasks.append(fetch_page(session,throttler,url)) 

        # group and Execute tasks concurrently
        htmls = await asyncio.gather(*tasks)

        # todo: batch insert, skip names gracefully
        for url, html in zip(fetch_urls, htmls):
            insert_webpage_or_do_nothing(url,html,depth+1,False,'',conn)

        conn.commit()
        print(f'Split time:{time.time()-t}')

# the main program
async def main():

    throttler = Throttler(get_throttle_limit()) # throttle network requests - wikipedia allows web crawling at small, slow scales
    async with aiohttp.ClientSession() as session:

        iresponse = await fetch_page(session,throttler,get_starting_url()) 

    depth = 0

    name = get_page_name(BeautifulSoup(iresponse,'html.parser'))

    conn,engine = get_connection()

    # inserts our seed/initial page into the database at depth 0
    insert_webpage_or_do_nothing(get_starting_url(),iresponse,depth,False,name,conn) 

    batch = get_batch_of_webpages(depth,conn)
    total_pages = get_current_total_pages(conn)

    # the loop that performs the breadth-first search
    while(depth < get_max_depth() and total_pages < get_absolute_page_limit()):

        # get a batch of pages to parse for links at the current depth
        batch = get_batch_of_webpages(depth,conn)
        
        # parse the batch for links and get the pages at the newly discovered links
        await process_batch(depth,throttler,batch,conn)

        total_pages = get_current_total_pages(conn)
        
        # checks if the depth-layer is fully processed
        if(len(batch)==0):
            depth += 1
       
        print(depth,total_pages)

    # update derived tables
    init_downstream_cache(conn)  

    # close database connections
    conn.close()
    close_cache()

if __name__=='__main__':
    asyncio.run(main())

    

