import parameters
from parameters import *

class Scraper:
    def __init__(self, **args):
        self.all_urls_lst = []
        self.d_all_options = {}
        self.d_valid_postal_code = {}
        self.valid_url_lst = []
        self.d_error_1 = {}
        self.d_error_2 = {}
        self.df = pd.DataFrame(columns=['blk','road','building','address','postal','lat','lon'])
        self.max_workers = max_workers
        
    def extract_data(self, url):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.get(url)
        data = response.json()
        return data
    
    def generate_all_options(self, n):
        code_lst = list(map(lambda x: "0" * (6 - len(str(x))) + str(x), list(range(n))))
        urls = [f"https://developers.onemap.sg/commonapi/search?searchVal={code}&returnGeom=Y&getAddrDetails=Y&pageNum=1" 
                for code in code_lst]
        self.all_urls_lst = urls
    
    def iterate_through_all_options(self, urls_lst):
        max_workers = self.max_workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(self.extract_data,url): url for url in urls_lst}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                code = url.partition("searchVal=")[2].partition("&returnGeom=Y")[0]
                try:
                    data = future.result()
                    nos_results = data['found']
                    nos_pages = data['pageNum']
                    self.d_all_options[code] = np.array((nos_results, nos_pages))
                except Exception as e:
                    self.d_error_1[code] = e
                    print('%r generated an exception: %s' % (code, e))
        
    def extract_valid_postal_code(self, d_all_options):
        valid_postal_code_lst = list(filter(lambda x: x[1][0] != 0, list(d_all_options.items())))
        d_valid_postal_code = {k:v for k, v in [[x[0], x[1][1]] for x in valid_postal_code_lst]}
        self.d_valid_postal_code = d_valid_postal_code
    
    def generate_valid_urls_lst(self, d_valid_postal_code):
        single_page_lst = list(filter(lambda x: x[1] == 1, list(d_valid_postal_code.items())))
        multiple_page_lst = list(filter(lambda x: x[1] != 1, list(d_valid_postal_code.items())))
        multiple_page_lst = list(map(lambda x: [(x[0], i) for i in range(1,x[1]+1)], multiple_page_lst))
        multiple_page_lst = [entry for entry_set in multiple_page_lst for entry in entry_set]
        valid_url_data_lst = single_page_lst + multiple_page_lst
        valid_url_lst = list(map(lambda x:
                      f"https://developers.onemap.sg/commonapi/search?searchVal={x[0]}&returnGeom=Y&getAddrDetails=Y&pageNum={x[1]}",
                      valid_url_data_lst))
        self.valid_url_lst = valid_url_lst
        
    def iterate_through_all_valid_options(self, valid_url_lst):
        df = pd.DataFrame(columns=['blk','road','building','address','postal','lat','lon'])
        max_workers = 2 * multiprocessing.cpu_count() + 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(self.extract_data,url): url for url in valid_url_lst}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    for j in range(data['found']):
                        blk_no = data['results'][j]['BLK_NO']
                        road_name = data['results'][j]['ROAD_NAME']
                        building = data['results'][j]['BUILDING']
                        address = data['results'][j]['ADDRESS']
                        postal = data['results'][j]['POSTAL']
                        lat = data['results'][j]['LATITUDE']
                        lon = data['results'][j]['LONGTITUDE']
                        index = len(self.df)
                        self.df.loc[index,:] = [blk_no,road_name,building,address,postal,lat,lon]
                except Exception as e:
                    code = url.partition("searchVal=")[2].partition("&returnGeom=Y")[0]
                    self.d_error_2[code] = e
                    print('%r generated an exception: %s' % (code, e))
                    
    def get_error_postal_code_list(self):
        return list(self.d_error_1.keys()) + list(self.d_error_2.keys())

    def get_scraped_address_dataframe(self):
        return self.df

    def save_scraped_address(self, name_of_file):
        return self.df.to_csv(name_of_file)
                

scraper = Scraper()
scraper.generate_all_options(postal_code_range)
scraper.iterate_through_all_options(scraper.all_urls_lst)
scraper.extract_valid_postal_code(scraper.d_all_options)
scraper.generate_valid_urls_lst(scraper.d_valid_postal_code)
scraper.iterate_through_all_valid_options(scraper.valid_url_lst)
scraper.get_scraped_address_dataframe()
scraper.get_error_postal_code_list()
scraper.save_scraped_address(name_of_scraped_address_file)
