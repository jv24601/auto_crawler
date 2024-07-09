settings = {
    'starting_url':'https://en.wikipedia.org/wiki/List_of_jazz_albums',
    'internal_links_only' : True,
    'depth' : 2,
    'request_throttle_limit':(1000,1), #requests per period,
    'batch_size':1000, # default batch size to throttle limit
    'absolute_page_limit':500000
}
