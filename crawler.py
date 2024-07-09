from utils import *
from db_services import get_connection,insert_webpage_or_do_nothing,get_current_total_pages,get_batch_of_webpages,update_urls_as_parsed,insert_linkmapping
import sqlalchemy as db


async def process_batch(depth,throttler,batch,conn):
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
            #destination_links = destination_links[0:min(10,len(destination_links))]

            for destination_link in destination_links:
                insert_linkmapping(source_url,destination_link,conn)

            fetch_urls += destination_links

        tasks = []

        # loop through URLs and append tasks
        for url in fetch_urls:
            tasks.append(fetch_page(session,throttler,url)) 

        # group and Execute tasks concurrently3
        htmls = await asyncio.gather(*tasks)

        # todo: batch insert
        for url, html in zip(fetch_urls, htmls):
            insert_webpage_or_do_nothing(url,html,depth+1,False,conn)

        conn.commit()


async def main():

    throttler = Throttler(get_throttle_limit())
    async with aiohttp.ClientSession() as session:

        iresponse = await fetch_page(session,throttler,get_starting_url())
        #isoup = BeautifulSoup(iresponse,'html.parser')
        #name = get_page_name(isoup)

    depth = 0

    #loopify starting here, need the database
    conn,engine = get_connection()


    insert_webpage_or_do_nothing(get_starting_url(),iresponse,depth,False,conn)

    #urls = get_links_from_soup(isoup)

    total_pages = get_current_total_pages(conn)
    batch = get_batch_of_webpages(depth,conn)
    conn.close()

        
        #Todo: modularize into parse-batch
    while(depth < get_max_depth() and total_pages < get_absolute_page_limit()):

        conn,engine = get_connection()

        batch = get_batch_of_webpages(depth,conn)
        print(len(batch))
        
        await process_batch(depth,throttler,batch,conn)

        total_pages = get_current_total_pages(conn)
        
        if(len(batch)==0):
            depth += 1

       
        print(depth,total_pages)
        conn.commit()    
        conn.close()





if __name__=='__main__':
    asyncio.run(main())