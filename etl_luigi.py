import luigi
import pandas as pd
import requests
import data_source.code.function_module as fm
from sqlalchemy import create_engine
import re
from bs4 import BeautifulSoup
# from helper.db_connector import source_db_engine, dw_db_engine
# from helper.data_validator import validatation_process
from pangres import upsert

SOURCE_DB_USERNAME = 'postgres'
SOURCE_DB_PASSWORD = 'password123'
SOURCE_DB_HOST = 'localhost'
SOURCE_DB_PORT = '8082'
SOURCE_DB_NAME = 'etl_test_extract'

WAREHOUSE_DB_USERNAME = 'postgres'
WAREHOUSE_DB_PASSWORD = 'password123'
WAREHOUSE_DB_HOST = 'localhost'
WAREHOUSE_DB_PORT = '8082'
WAREHOUSE_DB_NAME = 'etl_test_result'

class ExtractSalesData(luigi.Task):

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("pacman_de_test/data/raw/extract_sales_data.csv")
    
    def run(self):
        # create connection with postgres
        conn = create_engine("postgresql://postgres:password123@localhost:5555/etl_db")
        query = "SELECT * FROM public.amazon_sales_data"
        extract_sales_data = pd.read_sql(query, conn)

        extract_sales_data.to_csv(self.output().path, index = False)

class ExtractMarketingData(luigi.Task):
    
    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("pacman_de_test/data/raw/extract_marketing_data.csv")
    
    def run(self):
        extract_marketing_data = pd.read_csv("./data/ElectronicsProductsPricingData.csv")
        extract_marketing_data.to_csv(self.output().path, index = False)

class ExtractJournalData(luigi.Task):
    
    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget("pacman_de_test/data/raw/extract_journal_data.csv")
    
    #Function to get html text from page
    def access_webpage(url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        return soup

    def run(self):
        #Related starter link
        main = "https://garuda.kemdikbud.go.id"
        url_journal = "https://garuda.kemdikbud.go.id/journal/view/1242"

        #Calculate number of pages for url_journal
        response = requests.get(url_journal)
        soup_journal= BeautifulSoup(response.text, "html.parser")

        page_info = soup_journal.find(class_="pagination-info").text
        page = int(re.search(r'.*of (.*?) ', page_info).group(1))
        print("There are " + str(page) + " for the journal")

        #Collect general information and link to the publication of journal
        links =[]

        for i in range(1,page+1):
            url_page = url_journal + "?page=" + str(i)
            response = requests.get(url_page)
            soup_page= BeautifulSoup(response.text, "html.parser")
            head = soup_page.find(class_="ui segment padded article-box")
            items = head.find_all(class_="article-item")
            print("Collecting page " + str(i))
            for item in items:
                address = item.find(class_="title-article", href=True)
                links.append(main + address['href'])
        print("There are a total of " + str(len(links)) + ' publication for the journal')

        #Collect information about every publication in the specified journal
        data_temp = []
        count = 1
        for link in links:
            response = requests.get(link)
            soup_link= BeautifulSoup(response.text, "html.parser")
            head = soup_link.find(class_="ui segment article-display")
            # journal = head.find("div", attrs={'style': 'text-align:center;'} )
            date = head.find("p").text[13:]
            journal_desc = head.find_all("xmp")
            vol = journal_desc[0].text + ' ' + journal_desc[1].text
            title = journal_desc[2].text
            abstract = journal_desc[-1].text
            writers = journal_desc[3].text

            if len(journal_desc)-4 > 1:
                for i in range(4,len(journal_desc)-1):
                    writers += ', ' +  journal_desc[i].text
            
            data_temp.append([vol, date, title, writers, abstract, link])

            print("Journal no " + str(count) + " has been collected")
            count += 1
        
        extract_journal_data = pd.DataFrame(data_temp, columns=['journal', 'datePublished', 'publication', 'writers', 'abstract', 'link'])
        extract_journal_data.to_csv(self.output().path, index = False)

class ValidateData(luigi.Task):

    def requires(self):
        return [ExtractSalesData(),
                ExtractMarketingData(),
                ExtractJournalData()]
    
    def output(self):
        pass

    def run(self):
        # read sales data
        validate_sales_data = pd.read_csv(self.input()[0].path)

        # read marketing data
        validate_marketing_data = pd.read_csv(self.input()[1].path)

        # read journal data
        validate_journal_data = pd.read_csv(self.input()[2].path)

        # validate data
        fm.validation_process(df = validate_sales_data,
                             table_name = "sales")
        
        fm.validation_process(df = validate_marketing_data,
                             table_name = "marketing")
        
        fm.validation_process(df = validate_journal_data,
                             table_name = "journal")

class TransformSalesData(luigi.Task):

    def requires(self):
        return ExtractSalesData()
    
    def output(self):
        return luigi.LocalTarget("pacman_de_test/data/transform/transform_sales_data.csv")
    
    def run(self):
        # read data from previous source
        sales_data = pd.read_csv(self.input().path)

        # impute missing value of no_of_ratings
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].fillna(0)

        #Make a new currency column
        sales_data['currency'] = sales_data['actual_price'].iloc[0][:1]

        #Remove currency value in price
        sales_data['actual_price'] = sales_data['actual_price'].str.replace('₹', '')
        sales_data['discount_price'] = sales_data['discount_price'].str.replace('₹', '')

        #Convert price value to float
        sales_data['actual_price'] = sales_data['actual_price'].str.replace(',', '')
        sales_data['discount_price'] = sales_data['discount_price'].str.replace(',', '')

        sales_data['actual_price'] = sales_data['actual_price'].replace('', float('nan'))
        sales_data['discount_price'] = sales_data['discount_price'].replace('', float('nan'))

        # Convert non-empty strings to floats
        sales_data['actual_price'] = pd.to_numeric(sales_data['actual_price'], errors='coerce')
        sales_data['discount_price'] = pd.to_numeric(sales_data['discount_price'], errors='coerce')

        # Convert non-empty strings to floats
        sales_data['ratings'] = sales_data['ratings'].str.replace(',', '')
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].str.replace(',', '')

        sales_data['ratings'] = sales_data['ratings'].replace('', float('nan'))
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].replace('', 0)

        sales_data['ratings'] = pd.to_numeric(sales_data['ratings'], errors='coerce')
        sales_data['no_of_ratings'] = pd.to_numeric(sales_data['no_of_ratings'], errors='coerce')

        # remove irrelevant last column
        sales_data = sales_data.iloc[:, :-1]

        # save the output to csv
        sales_data.to_csv(self.output().path, index = False)

class TransformMarketingData(luigi.Task):

    def requires(self):
        return ExtractMarketingData()
    
    def output(self):
        return luigi.LocalTarget("pacman_de_test/data/transform/transform_marketing_data.csv")
    
    def run(self):
        # read data from previous source
        marketing_data = pd.read_csv(self.input().path)

        #Remove columns with missing values under threshold
        marketing_data = fm.remove_missing_value_col(marketing_data, 50)

        #Fill missing value
        marketing_data['prices.shipping'] = marketing_data['prices.shipping'].fillna('No Information')

        #Create Weight Unit
        marketing_data['weight_unit'] = ''
        if marketing_data['weight'].str.contains('pounds').any():
            marketing_data['weight_unit'] = 'pounds'
        else:
            marketing_data['weight_unit'] = 'other'
        
        #Convert Weight to Float
        if len(marketing_data['weight_unit'].unique()) == 1:
            marketing_data['weight'] = marketing_data['weight'].str.replace('pounds', '')
            marketing_data['prices.amountMax'] = marketing_data['prices.amountMax'].astype('float')

        #Convert Date Column
        marketing_data['dateAdded'] = pd.to_datetime(marketing_data['dateAdded'])
        marketing_data['dateUpdated'] = pd.to_datetime(marketing_data['dateUpdated'])

        dict_avail = {'Yes' : 'In Stock', 
                    'In Stock' : 'In Stock', 
                    'TRUE' : 'In Stock', 
                    'undefined' : 'No Information', 
                    'yes' : 'In Stock', 
                    'Out Of Stock' : 'Out Of Stock', 
                    'Special Order' : 'In Stock', 
                    'No' : 'Out Of Stock', 
                    'More on the Way' : 'In Stock', 
                    'sold' : 'Out Of Stock', 
                    'FALSE' : 'Out Of Stock', 
                    'Retired' : 'Out Of Stock', 
                    '32 available' : 'In Stock', 
                    '7 available' : 'In Stock'
                    }

        dict_condition = {'New' : 'New', 
                        'new' : 'New', 
                        'Seller refurbished' : 'Refurbished', 
                        'Used' : 'Used', 
                        'pre-owned' : 'Used', 
                        'Refurbished' : 'Refurbished', 
                        'Manufacturer refurbished': 'Refurbished', 
                        'New other (see details)': 'New', 
                        }
        
        # Replace values based on the dictionary
        marketing_data['prices.availability'] = marketing_data['prices.availability'].replace(dict_avail)
        marketing_data['prices.condition'] = marketing_data['prices.condition'].replace(dict_condition)

        # Fill missing values in column 'A' with 'No Information'
        marketing_data['prices.condition'] = marketing_data['prices.condition'].fillna('No Information')
        
        #Convert price to float
        marketing_data['prices.amountMin'] = marketing_data['prices.amountMin'].astype('float')
        marketing_data['prices.amountMax'] = marketing_data['prices.amountMax'].astype('float')

        # save the output to csv
        marketing_data.to_csv(self.output().path, index = False)

class TransformJournalData(luigi.Task):

    def requires(self):
        return ExtractJournalData()
    
    def output(self):
        return luigi.LocalTarget("pacman_de_test/data/transform/transform_journal_data.csv")
    
    def run(self):
        # read data from previous source
        journal_data = pd.read_csv(self.input().path)

        #Convert Date Column
        journal_data['datePublished'] = pd.to_datetime(journal_data['datePublished'])
        # save the output to csv
        journal_data.to_csv(self.output().path, index = False)

class LoadData(luigi.Task):

    def requires(self):
        return [TransformSalesData(),
                TransformMarketingData(),
                TransformJournalData()]
    
    def output(self):
        return [luigi.LocalTarget("pacman_de_test/data/load/load_sales_data.csv"),
                luigi.LocalTarget("pacman_de_test/data/load/load_marketing_data.csv"),
                luigi.LocalTarget("pacman_de_test/data/load/load_journal_data.csv"),]
    
    def run(self):
        # read data from previous task
        load_sales_data = pd.read_csv(self.input()[0].path)
        load_marketing_data = pd.read_csv(self.input()[1].path)
        load_journal_data = pd.read_csv(self.input()[2].path)

        # init data warehouse engine
        dw_engine = create_engine(f"postgresql://{WAREHOUSE_DB_USERNAME}:{WAREHOUSE_DB_PASSWORD}@{WAREHOUSE_DB_HOST}:{WAREHOUSE_DB_PORT}/{WAREHOUSE_DB_NAME}")

        dw_table_sales = "etl_sales_table"
        dw_table_marketing = "etl_marketing_table"
        dw_table_journal = "etl_journal_table"

        # insert data to data warehouse
        load_sales_data.to_sql(name = dw_table_sales,
                               con = dw_engine,
                               if_exists = "append",
                               index = False)
        
        load_marketing_data.to_sql(name = dw_table_marketing,
                               con = dw_engine,
                               if_exists = "append",
                               index = False)

        load_journal_data.to_sql(name = dw_table_journal,
                               con = dw_engine,
                               if_exists = "append",
                               index = False)
        
        values = {
            "name": "Testing Product",
            "main_category": "Testing Category",
            "sub_category": "Testing Sub Category",
            "image": "https://sekolahdata-assets.s3",
            "link": "https://pacmann.io/",
            "ratings": 5,
            "no_of_ratings": 30,
            "discount_price": 450,
            "actual_price": 1000
        }
        keys = ["name"]
        # Perform the upsert operation
        upsert(dw_engine, dw_table_sales, values, keys=keys)
        
        # upsert(con = dw_engine,
        #        df = load_marketing_data,
        #        table_name = dw_table_marketing,
        #        if_row_exists = "update")

        # # insert data to data warehouse
        # load_hotel_data.to_sql(name = dw_table_name,
        #                        con = dw_engine,
        #                        if_exists = "append",
        #                        index = False)

        # save the output
        load_sales_data.to_csv(self.output()[0].path, index = False)
        load_marketing_data.to_csv(self.output()[1].path, index = False)
        load_journal_data.to_csv(self.output()[2].path, index = False)

if __name__ == "__main__":
    
    luigi.build([ExtractSalesData(),
                ExtractMarketingData(),
                ExtractJournalData(),
                ValidateData(),
                TransformSalesData(),
                TransformMarketingData(),
                TransformJournalData(),
                LoadData()])