

import os
import sys
import json #ver 2.0.9
import ast
import re #ver 2.2.1
from datetime import datetime
from scrapy.exceptions import CloseSpider #'2.0.1'
from sqlalchemy import create_engine #sqlalchemy ver 1.3.13
from multiprocessing import Process
from multiprocessing import Pool  
import pandas as pd



def shopee_run(params_arg):
    start_time = datetime.now()
    print('')
    print('')
    print('this script begins on ====== ', start_time)
    print('')
    #print(' params_arg ================', params_arg)
    print('')
    print('')
    params = ast.literal_eval(params_arg)
    #print(params)
    print('')   
    
    ############################################################################
    #output_folder = 'shopee_raw' #+ region
    ############################################################################



    for i in range(0, len(params['items'])):
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' platform ======== ', params['platform'])

        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################

        try:
            print(' name ======== ', params['items'][i]['name'])
        except:
            raise KeyError(" ========== the item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")

        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################

        try:
            referer = params['items'][i]['referer']
            if 'page=' not in referer:
                referer = referer + '&page=__pagenum__'
            elif re.sub('page\=(.*?)\&','page=__pagenum__', referer, flags=re.DOTALL) != referer:
                referer = re.sub('page\=(.*?)\&','page=__pagenum__&', referer, flags=re.DOTALL)
                
            elif re.sub('page\=..','page=__pagenum__', referer, flags=re.DOTALL) != referer:
                referer = re.sub('page\=..','page=__pagenum__', referer, flags=re.DOTALL)
                
            elif re.sub('page\=.','page=__pagenum__', referer, flags=re.DOTALL) != referer:
                referer = re.sub('page\=.','page=__pagenum__', referer, flags=re.DOTALL)
        
            #if 'sortBy'not in referer:
            #if 'sortBy' in referer:
            #    re.sub('sortBy\=.*?','sortBy=sales', referer, flags=re.DOTALL)
            if not any(x in referer for x in ['&sortBy=relevancy', '&sortBy=ctime', '&sortBy=sales']):
                referer = referer + '&sortBy=sales'
            #referer = 
            #Sprint(referer)
            id = re.search(r'search\?.*\=(.*?)\&page', referer).group(1)
            #print(id)
            #print('')
            domain = re.search(r'shopee\.(.*?)/', params['items'][i]['referer']).group(1)
            if 'facet' in referer:
                url = "https://shopee."+domain+"/api/v2/search_items/?by=sales&categoryids="+id+"&limit=50&newest=__pagenum__&order=desc&page_type=search&version=2"
            elif 'keyword' in referer:
                url = "https://shopee."+domain+"/api/v2/search_items/?by=sales&keyword="+id+"&limit=50&newest=__pagenum__&order=desc&page_type=search&version=2"
            params['items'][i]['referer'] = referer
            params['items'][i]['url'] = url
            print(' referer ======== ', params['items'][i]['referer'])
            print(' url ======== ', params['items'][i]['url'])
        except (IndexError, KeyError, TypeError):
            raise KeyError(" ========== the url at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")

        except:
            raise ValueError(" ========== the URL at item name "+ params['items'][i]['name']+" doesn't seem to match the URL format ========== ")

        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################

        try:
            domain = re.search(r'shopee\.(.*?)/', params['items'][i]['referer']).group(1)
            region = domain[-2:]
            #print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++'+ region+ '++++++++++++++++++++++')
            if  str(region) in  ['id', 'my', 'vn', 'th', 'ph']:
                params['items'][i]['region'] = region
                print(' region ======== ', params['items'][i]['region'])
                #params['items'][i]['output'] = output_folder +'_'+ region
                #print(' output_folder ======== ', params['items'][i]['output'])
            else:
                raise ValueError(" ========== the region at item name "+ params['items'][i]['name']+"  is not on the scrapped countries ========== ")
        except:
            raise KeyError(" ========== the region at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")

            '''if 'shopee.vn' in params['items'][i]['name']:
                then params['items'][i]['region'] = 'vn'
            elif 'shopee.co.th' in params['items'][i]['name']:
                then params['items'][i]['region'] = 'th'
            elif 'shopee.co.id' in params['items'][i]['id']:
                then params['items'][i]['region'] = 'th'
            elif 'shopee.com.my' in params['items'][i]['name']:
                then params['items'][i]['region'] = 'my'
            elif 'shopee.ph' in params['items'][i]['name']:
                then params['items'][i]['region'] = 'ph'
            else:
                raise ValueError(" ========== the region at item name "+ params['items'][i]['name']+"  is not a valid scrappable countries ========== ")
            
        except:
            raise KeyError(" ========== the region at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")'''


        '''try:
            if params['items'][i]['region'] in ['id', 'my', 'vn', 'th', 'ph']:
                print(' region ======== ', params['items'][i]['region'])
                params['items'][i]['output'] = output_folder +'_'+ params['items'][i]['region']
                print(' output_folder ======== ', params['items'][i]['output'])
            else:
                raise ValueError(" ========== the region at item name "+ params['items'][i]['name']+"  is not on the scrapped countries ========== ")
        except:
            raise KeyError(" ========== the region at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")
'''
        

        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################

        try:
            if params['platform'] not in params['items'][i]['output']:
                params['items'][i]['output'] = params['items'][i]['output'] + '_' + params['platform']

            if params['items'][i]['region'] not in params['items'][i]['output']:
                params['items'][i]['output'] = params['items'][i]['output'] + '_' + params['items'][i]['region']

            '''if str(id) not in params['items'][i]['output']:
                params['items'][i]['output'] = params['items'][i]['output'] + '_' + str(id)'''
        except:
            params['items'][i]['output'] = 'raw_' + params['platform'] +'_'+ params['items'][i]['region'] +'_'+ str(id)
        finally:
            print(' output_folder ======== ', params['items'][i]['output'])
     


        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        #################################################################################################################################################################################################################################### 


        try: 
            if 1 <= params['items'][i]['max_page'] <= 160:
                print(' max_page ======== ', params['items'][i]['max_page'])
            else:
                raise ValueError(" ========== the max page at item name "+ params['items'][i]['name']+" is not between 1 and 160 ========== ")
        except:
            raise KeyError(" ========== the max_page at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")

        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################

        try:
            if 1 <= params['items'][i]['max_batch'] <= params['items'][i]['max_page']:
                print(' max_batch ======== ', params['items'][i]['max_batch'])
            else:
                raise ValueError(" ========== the max batch at item name "+ params['items'][i]['name']+" is not between 1 and max_page = "+ params['items'][i]['max_page'] +"  ========== ")
        except:
            raise KeyError(" ========== the max batch at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")
        
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################

        try:
            for j in ['category' , 'subcategory', 'subsubcategory']:
                 print(' '+j+' ======== ', params['items'][i][j])
        except:
            raise KeyError(" ========== the "+j+" at item name "+ params['items'][i]['name']+" doesn't seem to exist ========== ")

        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        
        '''try:
            DATABASE_URI = 'postgres+psycopg2://scrape:lupalagi@147.139.165.196:5432/' + shopee_raw
            engine = create_engine(DATABASE_URI)       
            con = engine.connect()
            con.execute('SELECT * FROM ' + params['items'][i]['name'])
            con.close()
        except:
            raise ValueError(" ========== the tablename "+params['items'][i]['output']+" at item name "+ params['items'][i]['name']+" doesn't seem to match the URL format ========== ")
        '''
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        ####################################################################################################################################################################################################################################
        print('')
        print('')
    
    for i in range(0, len(params['items'])):
        '''print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')
        print(' ======================================== item number ' + str(i+1) +  ' ======================================== ')'''
        '''print(' platform ======== ', params['platform'])
        #print(' region ======== ', params['items'][i]['region'])
        #print(' name ======== ', params['items'][i]['name'])
        print(' referer ======== ', params['items'][i]['referer'])
        print(' url ======== ', params['items'][i]['url'])
        #print(' max_page ======== ', params['items'][i]['max_page'])
        print(' category ======== ', params['items'][i]['category'])
        print(' subcategory ======== ', params['items'][i]['subcategory'])
        print(' subsubcategory ======== ', params['items'][i]['subsubcategory'])'''
        print('')
        print('')
        

    print('scrapy crawl shopee_main "'+str(params)+'"')
    print('')
    print('')
    os.chdir("parallel/")

    #os.chdir("parallel/script_shopee_main_parallel/spiders") #for tests
    #os.system('python script_shopee_main_parallel/spiders/shopee_main.py "deodorant" 5') #for tests

    #run through shopee scrapy commands
    #to do: is it possible by calling the python file directly? Seems the twisted reactor always returns an arror
    os.system('scrapy crawl shopee_main "'+str(params)+'"')
    os.chdir("..")
    time_1 = datetime.now() 
    '''os.system('cls')
    clear = lambda: os.system('cls')'''
    #os.chdir("..")
    

    print('')
    os.system('cls')
    clear = lambda: os.system('cls')
    clear()
    time_2 = datetime.now()
    CloseSpider()

    #parallel_clean(params)

    print('this script ends on ====== ', datetime.now())
    print('this script was ====== ' + str(datetime.now() - start_time) + ' long')
    print('1st part was ====== ' + str(time_1 - start_time) + ' long' )
    print('2nd part was ====== ' + str(time_2 - start_time) + ' long' )



