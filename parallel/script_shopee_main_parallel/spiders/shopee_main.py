import scrapy #'2.0.1'
import json

import os.path
import random #ver '2.0.9'

import time
import datetime
import os
import sys
import re # ver '2.2.1'
import pandas as pd #ver '1.0.1'
from pandas.io.json import json_normalize 
import numpy as np # '1.18.1'
import ast
import shutil

import scrapy #ver '2.0.1'
from scrapy.exceptions import CloseSpider
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings
#from scrapy.crawler import CrawlerRunner
#from scrapy.utils.log import configure_logging

from twisted.internet import reactor #ver '20.3.0'

from sqlalchemy import create_engine #ver '1.3.13'

from multiprocessing import Process
from multiprocessing.dummy import Pool as ThreadPool 
from multiprocessing import Pool  




    

def cleaning(name,  region, category, subcategory, subsubcategory):
        '''
        this function preprocess some data, it loads the JSON that is written in the steps on the main script and loads it into the 2 outputs 
        1. a CSV containing all the data
        2. A connection to the current PostgreSQL database

        Note: The script would reread the JSON file to compile
        Note: the SQL database needs to be pre-made

        To do: the script should run without re-reading the JSON files, instead store in some self dataframe
            edit: Using batched parallel multiprocessing, using self.dataframe is not possible outside of the class and using dynamic naming. maybe there are other ways?
                  otherwise if you're not using page batches, storing data in a dataframe is possible, thus calling a single process for cleaning the data, check the single cleaning method, at the end of the spider class
        '''
        data =pd.DataFrame()

        start_time =datetime.datetime.now()

        i = 0
        k = 0

        #loop through the written JSON files to compile them
        #To do: the script should run without re-reading the JSON files, instead store in some dataframe, rad the note above
        print( '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(' items length ================================== ', len(os.listdir('raw_shopee/raw_shopee_' + region+ '/' +name) ))
        print( '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        for i in range(0, len(os.listdir('raw_shopee/raw_shopee_' + region+ '/' +name))):
            filename = 'data_q_'+str(i)+'.json'
            #print(obj['items'] is not None)
            print('')
            print('')
            print('')
            print('')
            print(' =========== cleaning_' + name + ' =========== ')
            print('loading json ======================== ', filename)
            print("time since start ==================== ", datetime.datetime.now() - start_time)
            try:    
                with open ('raw_shopee/raw_shopee_' + region+ '/' +name+ '/' +filename, encoding='utf8') as d:
                    obj = json.load(d)
                #print('items is none ======================== ', obj['items'] is None)

                if obj['items'] is None:
                    break
                k=0
                for j in obj['items']:
                    j.update({'page_number':str(i)})
                    #j.update({'rating_star': obj['items'][k]['item_rating']['rating_star']})
                    #j.update({'timestamp':pd.datetime.now().replace(microsecond=0)})
                    #for m in range(0,6):
                    #   j.update({'star_' + str(5-m): obj['items'][k]['item_rating']['rating_count'][m]})
                    data = data.append(j, ignore_index = True) 
                    k+=1

                i += 1
            except:
                break
        print('max data = data_q_'+str(i-1)+'.json')   


        #printing some static parameters     
        #if 'timestamp' not in data:
        #    data.insert(0, 'timestamp', pd.datetime.now().replace(microsecond=0))
        if 'rank' not in data:
            data['rank'] = np.arange(len(data))
            
        if 'category' not in data:
            data['category'] = category

        if 'subcategory' not in data:
            data['subcategory'] = subcategory

        if 'subsubcategory' not in data:
            data['subsubcategory'] = subsubcategory
        
        if 'platform' not in data:
            data['platform'] = 'shopee'
        if 'region' not in data:
            data['region'] = region

        if 'engine_ver' not in data:
            data['engine_ver'] = 'v0.4.2'

        if 'timestamp' not in data:
            data['timestamp'] = pd.datetime.now().replace(microsecond=0)

        #extracting the review star from its json form
        for i in range (0,6):
            data['star_' + str(5-i)] = [func(x, i) for x in data['item_rating']]

        data['star_rating'] = [func(x,'rating_star') for x in data['item_rating']]
        # In[4]:

        #printing data to CSV
        data.to_csv('complete_' + name + '.csv', index=False)

        #exporting data to PostgreSQL Database
        DATABASE_URI = 'postgres+psycopg2://scrape:lupalagi@147.139.165.196:5432/shopee_raw'
        engine = create_engine(DATABASE_URI)
        data = pd.DataFrame(data, index = None)
        for col in data.columns:
            if str(data[col].dtype) == 'object':
                data[col] = data[col].astype(str)
        try:
            tablename = 'public.raw_test_' + name
            #data.to_sql(tablename, engine, if_exists='append', index = False)
        except:
            print(' === table not found in database! === ')


def func(x, i):
    '''
    this function extract the star from its raw dict/json
    '''
    if type(i) is str:
        #print(x['rating_star'])
        star = x['rating_star']
    elif type(i) is int:
        star = (x['rating_count'][i])
    return star

######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################






#main scraper class
class shopee_main(scrapy.Spider):
    #name = "shopee_main_deodorant"
    #download_delay = 1.0
    AUTOTHROTTLE_ENABLED = True
    #start_urls = ["https://shopee.co.id/api/v2/search_items/?by=sales&categoryids=14887&limit=50&newest=0&order=desc&page_type=search&version=2",
    #]
    #folder = 'data_shopee_main_deodorant/'
    #page = 1
    breaker = 0
    attempts = 0
    corrupt = 0
    iters = 0
    #def __in
    start_time = datetime.datetime.now()
    
    
    def __init__(self, name, output, region, url, referer, category, subcategory, subsubcategory, page_0, page_1,  page_max =160):
        '''
        instantiating  variables
        '''
        self.name = name
        self.output = output
        self.region = region
        self.start_urls = url.replace('__pagenum__', '0')
        self.url = url
        self.referer = referer
        self.page_0 = page_0
        self.page_1 = page_1
        self.page_max = page_max
        self.category = category
        self.subcategory = subcategory
        self.subsubcategory = subsubcategory
        self.df =pd.DataFrame()

        #removing any previous JSON files/create new folder
        try:
            os.mkdir('raw_shopee/')
        except:
            pass
        try:
            os.mkdir('raw_shopee/raw_shopee_' + region+ '/')
        except:
            pass
        try:
            os.mkdir('raw_shopee/raw_shopee_' + region+ '/' +name)
        except:
            pass
               
                 
    def check_ip(self, response):
        '''
        for debugging purposes - check request IPs whether our Proxies work
        note that it will slow down the scraping sequence SIGNIFICANTLY and may return errors, use with caution
        '''   
        pub_ip = response.xpath('//body/text()').re('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')[0]
        print("My public IP is ================ " + pub_ip)
    
    def check_corrupt(self, data, zeroes, pagenum):
        '''
        this func check whether there are errors in the returned data
        one major giveaways of a corrupt data is in price data
        in shopee, price data always have some form of extra zeros, for example for a product that costs Rp10.000, the price data would be 1000000000, ten thousand plus five extra zeroes
        if the data is corrupt, then the last five number would not be zeroes, for example 1000012345
        the script checks a number of items (pagenum), if error is raised then the self.error variable would be raised, so the scraper would retry this page later in the script
        
        
        note: the number of tailing zeroes and max item page might be different per region, need to check the pattern before production!
        '''
        zeroes = int(zeroes)
        pagenum = int(pagenum)
        for x in range(0, pagenum):
            try:
                a = str(data['items'][x]['price'])
               # print("price string number " + str(x) + " ====================== ", a)
               # print("last five price string number " + str(x) + " ============= ", a[-zeroes:])
                if a[-(zeroes):] != zeroes*'0':
                    #print(' ==== last 5 string does not match ==== ')
                    self.corrupt = 1
                    break
                elif a[-(zeroes):] == zeroes*'0':
                    self.corrupt = 0
                else:
                    self.corrupt = 1
                    break
            except IndexError:
                print(' ==== data not found IndexError ==== ')
                self.corrupt = 1
                break
            except:
                print(' === other errors! === ')
                self.corrupt = 1
                break

    def start_requests(self):
        '''
        this def instantiate the first-page crawler
        '''
        url = self.url.replace('__pagenum__', str(self.page_0))
        self.page =  self.page_0

        #randomizes the user agents to make detection harder
        ua_files = open('ua_files.txt').read().splitlines()
        user_agents =random.choice(ua_files)
        
        print("")
        print("")
        print(" ======== starting " + self.name + " page " +str(self.page_0)+ " to " +str(self.page_1)+ " ========")
        #print("page ============================ 0")
        #print("page ============================ 0")

        #uncomment below to check IP one by one 
        #yield scrapy.Request('http://checkip.dyndns.org/', callback=self.check_ip, dont_filter = True) #uncomment to check IP one by one 
        print("user agent ====================", user_agents)

        #headers for the request, might need checking once in a while whether it match the actual request headers
        headers = {
            'accept'          : '*/*'
            ,'accept-encoding': 'gzip, deflate, br'
            ,'accept-language': 'en-US,en;q=0.9'
            #,'if-none-match-' : '55b03-872cf4410a68f289ed472228c7829e2b'
            ,'referer'        : self.referer.replace('__pagenum__', str(self.page_0))
            ,'sec-fetch-dest' : 'empty'
            ,'sec-fetch-mode' : 'cors'
            ,'sec-fetch-site' : 'same-origin'
            ,'user-agent'     : user_agents
            ,'x-api-source'   : 'pc'
            ,'x-requested-with': 'XMLHttpRequest'
            ,'Connection': 'close'
        }
        
        yield scrapy.Request(url=url, method='GET',  headers=headers, dont_filter = True)
    
    
    
    def parse(self, response):
        '''
        this part parses the response, then call the request again for the next pages and so on
        '''
        
        print("")
        print("")
        print("")
        print("")
        print(" ======== " + self.name + " from " +str(self.page_0)+ " to " +str(self.page_1)+ "  ========")
        print("page ============================ ", str(self.page))
        print("page ============================ ", str(self.page))
        print("iterations =======================", str(self.iters))
        print("timestamp ======================= ", datetime.datetime.now())
        print("time since start ==================== ", datetime.datetime.now() - self.start_time)

        #uncomment below to check IP one by one 
        #yield scrapy.Request('http://checkip.dyndns.org/', headers = {'Connection': 'close'}, callback=self.check_ip, dont_filter = True) #uncomment to check IP one by one
        
        #randomizes the user agents to make detection harder
        ua_files = open('ua_files.txt').read().splitlines()
        user_agents =random.choice(ua_files)


        url = self.url.replace('__pagenum__', str((self.page*50))) 



        print('attempts on this page ============================', str(self.attempts+1))
        
        print("user agent ====================", user_agents)

        #headers for the request, might need checking once in a while whether it match the actual request headers
        headers = {
        'accept'          : '*/*'
        ,'accept-encoding': 'gzip, deflate, br'
        ,'accept-language': 'en-US,en;q=0.9'
        #,'if-none-match-' : '55b03-20443a68390f59aa1bc448bc3b42fa6e'
        ,'referer'        : self.referer.replace('__pagenum__', str(self.page))
        ,'sec-fetch-dest' : 'empty'
        ,'sec-fetch-mode' : 'cors'
        ,'sec-fetch-site' : 'same-origin'
        ,'user-agent'     : user_agents
        ,'x-api-source'   : 'pc'
        ,'x-requested-with': 'XMLHttpRequest'
        ,'Connection': 'close'
    }

        yield scrapy.Request(url=url, callback=self.parse, headers=headers, dont_filter = True)

        data = json.loads(response.text)
        #print(data)

        #every region has different error patterns, as shown below. Need to be checked manually
        #give up after 50 tries, also the possibility of the category have less than 160 pages
        if self.attempts <= 10:
            if self.region in ['id', 'vn', 'th']:
                self.check_corrupt(data = data, zeroes = 5, pagenum = 50)
            elif self.region == 'ph':
                self.check_corrupt(data = data, zeroes = 5, pagenum = 45)
            elif self.region == 'my':
                self.check_corrupt(data = data, zeroes = 3, pagenum = 50)
            print("data corrupted ================ ", self.corrupt)
        else:
            print("data corrupted ================ but gave up trying on this page")
            self.corrupt = 0
            self.breaker = 1
            pass

        print("data corrupted ================ ", self.corrupt)
        
        self.iters +=1

        #if hit max page, that call the cleaning function    

        ##if you're using unbatched pagination and wants to use single process cleaning, uncomment the cleaning func
        ##if you're using unbatched pagination and wants to use single process cleaning with an integrated dataframe (ie not reading the entire printed JSON object), uncomment the df processs
        ##the integrated df method should be more pythonic and efficient. It is still somewhat unstable though, use with caution

        if self.page >= self.page_max+1:
            #if you're using unbatched pagination and wants to use single process cleaning, uncomment this
            #cleaning(self.name, self.output, self.region, self.category, self.subcategory, self.subsubcategory)    
            
            ##if you're using unbatched pagination and wants to use single process cleaning with an integrated dataframe (ie not reading the entire printed JSON object), uncomment this
            '''if 'rank' not in self.df:
                self.df['rank'] = np.arange(len(self.df))

            print(' =================== raw' + self.name + '.csv')
            self.df.to_csv('raw' + self.name + '.csv', index=False)     
            print(' =================== raw' + self.name + '.csv')'''

            raise CloseSpider("====MAX PAGE HAS BEEN REACHED!==== ")    

        ##if you're using unbatched pagination and wants to use single process cleaning, you can comment out this entire elif part as it becomes redundant
        elif self.page >= self.page_1:
            #self.cleaning(self.name, self.output, self.region, self.category, self.subcategory, self.subsubcategory)    

            '''if 'rank' not in self.df:
                self.df['rank'] = np.arange(len(self.df))

            print(' =================== raw' + self.name + '.csv')
            self.df.to_csv('raw' + self.name + '.csv', index=False)     
            print(' =================== raw' + self.name + '.csv')'''

            raise CloseSpider("====MAX PAGE HAS BEEN REACHED!==== ")


        #if error occurs, and max threshold is hit, print JSON as-is
        elif self.corrupt == 0 and self.breaker == 1:
            cleaning(self.name, self.output, self.region, self.category, self.subcategory, self.subsubcategory)        
            if data['items'] is None:
                raise CloseSpider("====NO DATA IS RETURNED!==== ") 
            elif data['query_rewrite'] is None:
                raise CloseSpider("====DATA CORRUPTED!====") 
            else:
                raise CloseSpider("==== UNKNOWN ERROR ==== ")   
        #if error occurs, print out the corresponding error types, then loop to scrapthe same page again 
        elif  self.corrupt == 1:
            self.attempts += 1
            print("Something went wrong!, retry attempts ===== ", self.attempts)
            if data['items'] is None:
                print("Error =========== data[item] is None, no data is returned!")
                time.sleep(5) 
            elif self.corrupt == 1:
                print("Error =========== data is corrupted!, retrying in 5 secs")
                time.sleep(5)       
        #if OK, then print the acquired JSON data to a JSON file to be compiled later by the cleaning function
        else:
            with open(os.path.join('raw_shopee/raw_shopee_' + self.region+ '/' +self.name ,'data_q_' + str(self.page) + '.json'), 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            

            #data = data['items']
            #data.update({'page':self.page})
            #data.update({'rating_star': obj['items'][k]['item_rating']['rating_star']})
            #data.update({'timestamp':pd.datetime.now().replace(microsecond=0)})
            
            #df = pd.DataFrame

            ##if you're using unbatched pagination and wants to use single process cleaning with an integrated dataframe (ie not reading the entire printed JSON object), uncomment this section
            '''k=0
            for j in data['items']:
                j.update({'page_num':self.page})
                j.update({'rating_star': data['items'][k]['item_rating']['rating_star']})
                j.update({'timestamp':pd.datetime.now().replace(microsecond=0)})
                for m in range(0,6):
                    j.update({'star_' + str(5-m): data['items'][k]['item_rating']['rating_count'][m]})
                self.df = self.df.append(j, ignore_index = True) 
                k+=1

            #if 'rank' not in df:
             #   df['rank'] = np.arange(len(data))
                
            if 'category' not in self.df:
                self.df['category'] = self.category

            if 'subcategory' not in self.df:
                self.df['subcategory'] = self.subcategory

            if 'subsubcategory' not in self.df:
                self.df['subsubcategory'] = self.subsubcategory
            
            if 'platform' not in self.df:
                self.df['platform'] = 'shopee'
            if 'region' not in data:
                self.df['region'] =self.region

            if 'engine_ver' not in self.df:
                self.df['engine_ver'] = 'v0.4.2'


            #print(self.df)'''

            self.page +=1
            self.attempts = 0

######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################





######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################

#for testing purposes, manual parameters
'''
parameters = {
        'deodorant':
        {
            'name'          : 'shopee_main_deodorant'
            ,'folder'       : 'data_shopee_main_deodorant/'
            ,'max_page'     : '160'
            ,'url'          : 'https://shopee.co.id/api/v2/search_items/?by=sales&categoryids=14887&limit=50&newest=0&order=desc&page_type=search&version=2'
            ,'url_1'        : 'https://shopee.co.id/api/v2/search_items/?by=sales&categoryids=14887&limit=50&newest='
            ,'url_2'        :                                                                                       '&order=desc&page_type=search&version=2'
            ,'referer'      :'https://shopee.co.id/search?facet=14887&page=0&sortBy=sales'
            ,'referer_1'    :'https://shopee.co.id/search?facet=14887&page='
            ,'referer_2'    :                                               '&sortBy=sales'
        }
        ,'perfume':
        {
            'name'          : 'shopee_main_perfume'
            ,'folder'       : 'data_shopee_main_perfume/'
            ,'max_page'     : '160'
            ,'url'          : 'https://shopee.co.id/api/v2/search_items/?by=sales&keyword=perfume&limit=50&newest=0&order=desc&page_type=search&version=2'
            ,'url_1'        : 'https://shopee.co.id/api/v2/search_items/?by=sales&keyword=perfume&limit=50&newest='
            ,'url_2'        :                                                                                       '&order=desc&page_type=search&version=2'
            ,'referer'      :'https://shopee.co.id/search?keyword=perfume&page=0&sortBy=sales'
            ,'referer_1'    :'https://shopee.co.id/search?keyword=perfume&page='
            ,'referer_2'    :                                               '&sortBy=sales'
        }        
    }

'''



######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################
######################################################################################################################################################################################################


#for testing purposes, manual parameters
#params = ['deodorant', 'perfume'] # for tests
#max_page = 5 # for tests
#max_batch = 10 # for tests

#delete any remaining files
try:
    shutil.rmtree('raw_shopee')
except:
    pass


print('===================================Argument List:', str(sys.argv))
print('')
print('')
print('===================================Argument List:', str(sys.argv[2]))
print('')
print('')
'''for i in range(0, len(sys.argv)):
    print( ' ================= ', sys.argv[i])'''

#params['items'][i]['max_page'] = 
#printing the script argument from 
params = ast.literal_eval(sys.argv[2])
#for i in params:
    #for j in params[i]:
    #    print ("name ===" + j +" ===data===   "+ params[i][j])
    #print('')
    #print('')
for i in range(0, len(params['items'])):
    print(' ===== ===== parameter number ' + str(i + 1) + ' ===== ===== ')
    print(' ===== ===== parameter number ' + str(i + 1) + ' ===== ===== ')
    print('parameter name ===== ', params['items'][i]['name'])
    print('parameter folder ===== ', params['items'][i]['output'])
    print('parameter max_page ===== ', params['items'][i]['max_page'])
    print('parameter max_batch ===== ', params['items'][i]['max_batch'])
    print('parameter url ===== ', params['items'][i]['url'])
    print('parameter referer ===== ', params['items'][i]['referer'])
    for j in ['category' , 'subcategory', 'subsubcategory']:
                print('parameter '+j+' ===== ', params['items'][i][j])
    print('')
    print('')


#instantiate the spider instance
process = CrawlerProcess(get_project_settings())

for i in range(0, len(params['items'])):
    page_batch = int(params['items'][i]['max_page'])/int(params['items'][i]['max_batch'])
    for j in range(0, params['items'][i]['max_batch']): 
        page_0 = int(page_batch*(j))
        page_1 = int(page_batch*(j+1)) 
        print('')
        print('')
        print('0000000000000000000000000000000000000000')
        print(page_0)
        print(page_1)
        print('0000000000000000000000000000000000000000')
        print('')
        print('')
        process.crawl(shopee_main, name = params['items'][i]['name']
                                            , output = params['items'][i]['output']
                                            , region = params['items'][i]['region']
                                            , page_0 = page_0
                                            , page_1 = page_1
                                            , page_max = int(params['items'][i]['max_page'])
                                            , url = params['items'][i]['url']
                                            , referer = params['items'][i]['referer']
                                            , category = params['items'][i]['category']
                                            , subcategory = params['items'][i]['subcategory']
                                            , subsubcategory = params['items'][i]['subsubcategory']
                                            )
process.start() # the script will block here until all crawling jobs are finished
print(' ================================================================================================================================================================== ')
print(' ===================================================================================================================================================================')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(' ================================================================================================================================================================== ')
print(__name__)

'''for i in range(0, len(params['items'])):
    cleaning(params['items'][i]['name'], params['items'][i]['region'], params['items'][i]['category'], params['items'][i]['subcategory'], params['items'][i]['subsubcategory'])'''

#parallel multiprocess cleaning. If you don't want to use it, please comment this entire section

name = []
region = []
category = []
subcategory = []
subsubcategory = []
for i in range(0, len(params['items'])):
    j = params['items'][i]['name']#, params['items'][i]['region'], params['items'][i]['category'], params['items'][i]['subcategory'], params['items'][i]['subsubcategory']
    name.append(j)
    j = params['items'][i]['region']
    region.append(j)
    j = params['items'][i]['category']
    category.append(j)
    j = params['items'][i]['subcategory']
    subcategory.append(j)
    j = params['items'][i]['subsubcategory']
    subsubcategory.append(j)
    #print(process[i])
    print('')
print(name)
print(region)

'''def cleaning_2(name,  region, category, subcategory, subsubcategory):  
    print(name + region+ category+ subcategory+ subsubcategory)
    #print(region)
    #print(category)
    #print(subcategory)
    #print(subsubcategory)
    print('')'''

pool = ThreadPool(6)
pool.starmap(cleaning, zip(name, region, category, subcategory, subsubcategory)) 
pool.close() 
pool.join()


























'''os.system('cls')
clear = lambda: os.system('cls')
clear()'''


