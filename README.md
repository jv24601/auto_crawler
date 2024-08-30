This is a work in progress repository for an automatic web-crawler and page-rank builder!

You are welcome to use it as-is. Just update the base url you want to crawl in the settings file, 
and make sure your throttling settings respect the robots.txt on whatever site you want to crawl.

QUICKSTART: 

0. Run "python3 -m pip install -r requirements.txt" from the root repository folder (skippable if you have the requirements)
1. Modify the settings in crawler_settings.py according to your needs, mainly the starting_url and throttle limit. (optional)
2. Run "python3 crawler.py"
3. Check out the vizu.ipynb for example visualizaitons via a Jupyter kernel. 

The program will do a breadth-first search through the links on webpages, follow each one. 

Note that some columns like pagetype may not fill with values for non-music pages. Modify according to your needs!

Here are some things still to-do:

Provide dockerfile for containerized crawling
Offer built-in easy configuration/filtering for the web crawler beyond what the current settings file does
Pre-built exploratory data analysis for the crawl
Bulk-update the SQL queries + add multi-processing so the rows can be processed in parallel


