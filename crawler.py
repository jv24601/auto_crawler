from utils import *
from db_services import *

import sqlalchemy as db
from throttler import Throttler
from bs4 import BeautifulSoup
import time

async def process_batch(depth:int,throttler: Throttler,batch:list[any],conn:db.Connection):
    t = time.time()
    async with aiohttp.ClientSession() as session:
        
        source_urls = [row[0] for row in batch]

        update_urls_as_parsed(source_urls,conn)

        destination_links = []
        fetch_urls = []

        
        for i in range(len(batch)):
            source_url = batch[i][0]
            source_html = batch[i][1]

            destination_links = get_links_from_html(source_html)

            #todo: update linkmap table with sourceurl, destination links
            #destination_links = destination_links[0:min(5,len(destination_links))]

            for destination_link in destination_links:
                insert_linkmapping(source_url,destination_link,conn)

            fetch_urls += destination_links

        tasks = []

        # loop through URLs and append tasks
        for url in fetch_urls:
            tasks.append(fetch_page_cached(session,throttler,url)) 

        # group and Execute tasks concurrently3
        htmls = await asyncio.gather(*tasks)

        # todo: batch insert, skip names gracefully
        for url, html in zip(fetch_urls, htmls):
            if(html!=None and False):
                name = get_page_name(BeautifulSoup(html,'html.parser'))
            else:
                name=''
            insert_webpage_or_do_nothing(url,html,depth+1,False,name,conn)

        conn.commit()
        print(f'Split time:{time.time()-t}')

async def main():

    throttler = Throttler(get_throttle_limit())
    async with aiohttp.ClientSession() as session:

        iresponse = await fetch_page_cached(session,throttler,get_starting_url())

    depth = 0

    name = get_page_name(BeautifulSoup(iresponse,'html.parser'))

    conn,engine = get_connection()

    insert_webpage_or_do_nothing(get_starting_url(),iresponse,depth,False,name,conn)

    batch = get_batch_of_webpages(depth,conn)
    total_pages = get_current_total_pages(conn)

    while(depth < get_max_depth() and total_pages < get_absolute_page_limit()):

        batch = get_batch_of_webpages(depth,conn)
        
        await process_batch(depth,throttler,batch,conn)

        total_pages = get_current_total_pages(conn)
        
        if(len(batch)==0):
            depth += 1
       
        print(depth,total_pages)

    init_downstream_cache(conn)  

    conn.close()

if __name__=='__main__':
    asyncio.run(main())

    

