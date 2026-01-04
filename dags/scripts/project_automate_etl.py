

# A FUNCTION THAT LOGS PROCESSS 


def logingprocess():
    import logging
    logging.basicConfig(    filename  ="process_logs",
                            level     =logging.DEBUG,
                            format    ="%(asctime)s - %(levelname)s - %(message)s")
    return logging
	
	
	
	
# DATA SOURCE: API EXTRACTION FROM BOOK A BOOK WEBSITE


def book_api(base_url):
        loggingsteps = logingprocess()

# Import neccessary libaries
        from bs4 import BeautifulSoup
        import requests
        import pandas as pd


        header = {'Accept': 'application/html'}

# LIST AND DICTIONARY to store all book details
        title = []
        genre = []
        rating = []
        price = []
        UPC = []
        
        all_books = {}

# Iterate through all pages
        page_num = 1
        while True:
            page_url = base_url + 'catalogue/page-{}.html'.format(page_num)
            response = requests.get(page_url, headers=header)
            
            if response.status_code != 200:
                loggingsteps.error("Failed API response")
                break
            loggingsteps.info("Valid API response secured-OK")
            page_soup = BeautifulSoup(response.content, 'html.parser')
            
# Get all book URLs from the current page
            book_urls = [base_url + 'catalogue/' + a['href'].replace('../../../', '') for a in page_soup.select('h3 > a')]
            

# Extract details for each book and add to the list
            for book_url in book_urls:
                book_response = requests.get(book_url)
                books = BeautifulSoup(book_response.content, 'html.parser')
                
                title.append(books.find('h1').text)
                genre.append(books.find('ul', class_='breadcrumb').find_all('a')[2].text)
                rating.append(books.find('p', class_='star-rating')['class'][1])
                price.append(books.find('p', class_='price_color').text)
                UPC.append(books.find('th', string='UPC').find_next_sibling('td').text)
            
# Increment page number
            page_num += 1
            loggingsteps.info("Book Records extracted successfully")


# dictionary to store all book details
        all_books = {'Title': title,'Genre': genre,'Rating': rating,'Price': price,'UPC': UPC}


# DataFrame to print all the books scrape and save to CSV
        if all_books:
                try:
                    books_df = pd.DataFrame(all_books)
                    books_df.to_csv('books.csv', index=False)
                    loggingsteps.info(f"Scraped {len(title)} books and saved to 'books.csv'")
                    
                except Exception as e:
                        loggingsteps.error(f"Saving Book records encountered and error{e}")
        else:
                loggingsteps.info("No data found in object to continue procedure")
                print('Empty object')
        
        return books_df
		
		
		
# DATA TRANSFORMATION FUNCTION

	
def data_wrangling(df):
        import pandas as pd
        import tabulate
        if not df.empty:
                for column in df.columns:
                        if pd.api.types.is_object_dtype(df[column]):
                            if df[column].isna().any():
                                df.fillna({column: df[column].mode()[0]}, inplace=True)
                            if column == "Title":
                                df[column] = df[column].str.replace(r"[^A-Za-z\d(),.# ]+", "", regex=True)
                            df[column] = df[column].str.replace(r"\s+"," ",regex=True).str.strip()
                            
                            
                            if column == "Price":
                                df[column] = df[column].str.replace(r"[^\d.]", "", regex=True).astype(float)
                print(f"Books Retrieved are:\n{tabulate.tabulate(df.set_index('Title'),headers=df.columns.tolist(),tablefmt='heavy_grid')}")
                df.info()
                return df
                        
                                         


# BATCH DATA INTO 100 ROWS PER INSERT FOR FAST LOADING INTO ORACLE DB


def batch(df):
    loggingsteps = logingprocess()
    if not df.empty:
        batch_size = 100
        data = [tuple(rows) for rows in df.itertuples(index = False, name = None)]
        for i in range(0,len(data),batch_size):
            batch = data[i:i+batch_size]
            loggingsteps.info(f"Batch record of lenght: {len(batch)} successfully structured")
            yield batch



# FUNCTION THAT SECURE CONNECTION TO ORACLE SERVER

def ora_conn():
    import oracledb
    from airflow.hooks.base import BaseHook
    loggingsteps = logingprocess()
    c = BaseHook.get_connection("ora_connect")
    hostname    = c.host
    password    = c.password
    server      = c.schema
    user        = c.login
    port        = c.port
    dsn = f"{hostname}:{port}/{server}"
    
    try:
        connection = oracledb.connect(user=user,password=password, dsn=dsn)
        loggingsteps.info("Connected to Oracle Server Successfully")
        return connection
    except Exception as e:
        loggingsteps.error(f"Unable to secure connection to sever: {e}") 



# BULK INSERT DATA OR DATA LOADING FUNCTION

def ora_loading(conn, batch_rec):
    loggingsteps = logingprocess()
    if conn:    
            try:
                conn.ping()
                loggingsteps.info("Connection secure and database services active")
            except Exception as e:
                loggingsteps.error(f"Database service inactive: {e}")
                return
            
            if not batch_rec:
                loggingsteps.warning("No records to insert")
                return
            
            try:
                cursor = conn.cursor()
                BooksObjType   = conn.gettype('BOOK_DATA_REC_TYPE')
                BooksTableType = conn.gettype('BOOK_DATA_TBL_TYPE')
                obj_table      = BooksTableType.newobject()
                failed_rows    = BooksTableType.newobject()
                
                # preparing object for the oracle database package
                for title, genre, ratings, price, ups in batch_rec:
                    obj = BooksObjType.newobject()
                    obj.TITLE   = str(title)
                    obj.GENRE   = str(genre)
                    obj.RATINGS = str(ratings)
                    obj.PRICE   = round((price),2)
                    obj.UPS     = str(ups)
                    obj_table.append(obj)
                
                # Calling the bulk insert procedure once
                cursor.callproc('BOOKS_PKG.bulk_upsert_books', [obj_table, failed_rows])
                conn.commit()
                loggingsteps.info(f"Database Package ran successfully, {len(batch_rec)} insert per batch performed")
            
            except Exception as e:
                conn.rollback()
                loggingsteps.error(f"Runtime error: {e}")


# MAIN FUNCTION TO RUN THE APPLICATION AS AN INTEGRATED PACKAGE

def main():
    extracted_df = book_api('http://books.toscrape.com/')
    clean_df = data_wrangling(extracted_df)
    conn = ora_conn()
    for batch_rec in batch(clean_df):
        ora_loading(conn,batch_rec)





										 
		
