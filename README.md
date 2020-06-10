# shpee_scrpy


# How to Run
There are two ways to run the code, either by running the shopee_environ.py or the shopee_arg.py. The former accept environment variables as arguments (with an example within), the latter used a command argument
However using the environ.py is preferable at the moment

# Prerequisites
This script was made on python 3.7.6, with the following modules // versions:
* scrapy // 2.0.1
* sqlalchemy // 1.3.13
* pandas // 1.0.1
* numpy // 1.18.1
* twisted // 20.3.0
* cffi
* json // 2.0.9
* re // 2.2.1
* random // 2.0.9
* psycopg

# To run on a  fresh ubuntu machine
1. upgrade to python 3.7
    * sudo apt update -y
    * sudo apt install software-properties-common
    * sudo apt install python3.7
    * sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1
    * sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 2
    * sudo update-alternatives --config python3
    
    then choose python 3.7 as the standard

2. Installing packages
    * Pip3 install scrapy
    * Pip3 install sqlalchemy
    * Pip3 install cffis
    * Pip3 install pandas
    * Pip3 install numpy

3. configure the details in the shopee_environ.py
4. run 'python3 shopee_environ.py' on the terminal

# Input and Output
This script accepts input parameter as a JSON object either as environment variables or arguments as mentioned.
Then it output three kinds of files
* Details for the response as a JSON object for every page scrapped, named 'data_q_(pagenumber).json' in the parallel/raw_shopee/raw_shopee_(id)/(name) folder
* A CSV file, which compiles all of the corresponding JSONs, named 'complete_shopee_main_(name).csv' in the parallel folder
* PostgreSQL database rows on the current PostgreSQL database, in the 'shopee_raw' database, named 'public.raw_test_(name)' However is still unstable and req further development

NOTE that the PostgreSQL database needs to be defined first before running the script (to be fixed)

# Input Paramater & syntax
An example JSON object for the parameter is as follows, it includes two titles which are deodorant and perfumes

TO DO: simplify by removing the url_1/url_2 and referer_1/referer_2 by looking at the full URL/referrer, also to differentiate URL between if the URL used 'search box' or by clicking category links (different URLs are produced), then the API URL could be deduced internally instead as a parameter

```json
{
    "platform": "shopee",
    "items": [
        {
            "name" : "shopee_id_eyeliner",
            "referer" : "https://shopee.co.id/search?facet=14887",
            "max_page" : 160,
            "max_batch" : 10,
            "category" : "cat_eyeliner",
            "subcategory": "subcat_eyeliner",
            "subsubcategory": "subsubcat_eyeliner",
            "output": "tralala"
            
        },
        {
            "name" : "shopee_id_ikan",
            "referer" : "https://shopee.co.id/search?keyword=ikan",
            "max_page" : 160,
            "max_batch" : 10,
            "category" : "cat_ikan",
            "subcategory": "subcat_ikan",
            "subsubcategory": "subsubcat_ikan",
            #"output": "trilili"
        },
        
    ]
}
```

explanations:
* platform: platform associated witht these json, in this case this scraping engine is for Shopee
* referer: the main first-page URL to be scrapped. the format should be shopee.(domain)/search?facet=(id)&page=0 for category searches or shopee.(domain)/search?keyword=(id)&page=0 for keyword searches (shopee pages start from 0). You can also add how the items are listed by adding '&sortBy=relevancy', '&sortBy=ctime', or '&sortBy=sales' to the referrer, however for most intents and purposes the sortBy=sales is the one you need  and is the standard one if none are given.
* name: the name of the engine that you want, preferably it is unique and contains the name of the things to be scrapped and their corresponding regions and their platform
* max_page: the maximum page that you want to scrap, for Shopee the amount of max page scrappable is from 1 page to 160 pages
* category, subcategory, and subsubcategory: the corresponding categories for the items inside the URL
* max_batch: the engine would divide the number of pages to be scrapped and assigned it to multiple spiders per url, hitting the page list evenly with a lot of spiders instead of one independent and parallel of each others. For example, if you're going to scrap 100 pages with 5 batch, then the engine would create 20 spiders, with the first spiders scraping page 1 to 20, the second spider from 21 to 40 and so on, thus multipliying the speed and efficiency. However, the max_batch should not be greater than max_page. If you only want to scrap without this parallel feature, you can assign it to 1.
* output: the output folder you want
