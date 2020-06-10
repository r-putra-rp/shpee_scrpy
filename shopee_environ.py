import os
#import pprint 
import shopee_run
import ast
import re
#for testing purposes, the environment variables are stated here
params_start = {
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
        {
            "name" : "shopee_vn_toner",
            "referer" : "https://shopee.vn/search?facet=8545&page=0&sortBy=sales",
            "max_page" : 160,
            "max_batch" : 10,
            "category" : "cat_ikan",
            "subcategory": "subcat_ikan",
            "subsubcategory": "subsubcat_ikan",
            #"output": "trilili"
        },
        {
            "name" : "shopee_th_primer",
            "referer" : "https://shopee.co.th/search?facet=9238&page=0&sortBy=sales",
            "max_page" : 160,
            "max_batch" : 10,
            "category" : "cat_ikan",
            "subcategory": "subcat_ikan",
            "subsubcategory": "subsubcat_ikan",
            #"output": "trilili"
        },
        {
            "name" : "shopee_my_concealer",
            "referer" : "https://shopee.com.my/search?facet=6619&page=0&sortBy=sales",
            "max_page" : 160,
            "max_batch" : 10,
            "category" : "cat_ikan",
            "subcategory": "subcat_ikan",
            "subsubcategory": "subsubcat_ikan",
            #"output": "trilili"
        },
        {
            "name" : "shopee_ph_shampoo",
            "referer" : "https://shopee.ph/search?facet=7233&page=0&sortBy=sales",
            "max_page" : 160,
            "max_batch" : 10,
            "category" : "cat_ikan",
            "subcategory": "subcat_ikan",
            "subsubcategory": "subsubcat_ikan",
            #"output": "trilili"
        },
    ]
}
#pprint.pprint(dict(params_start), width = 1) 

#Loading the parameters to the environment, for testing
os.environ['scrapy_params'] = str(params_start)


#pprint.pprint(os.environ['scrapy_params_start'], width = 1)
#print(os.environ['scrapy_params'])

#loading the loadad environment params to the script itself
params = os.environ['scrapy_params']
shopee_run.shopee_run(params)

'''params = params_start
for i in range(0, len(params['items'])):
    domain = re.search(r'shopee\.(.*?)/', params['items'][i]['referer']).group(1)
    region = domain[-2:]
    params['items'][i]['region'] = region
    
    params['items'][i]['output'] = 'raw_' + params['platform'] +'_'+ params['items'][i]['region'] +'_'+ str(id)
#cleaning.parallel_clean(params)'''
