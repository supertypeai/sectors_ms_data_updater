from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import os
from supabase import create_client
from dotenv import load_dotenv
import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")
# Load environment variables
load_dotenv()

# Initialize Supabase client
url_supabase = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, key)

# Fetch data from Supabase
# ms_code_data = supabase.table("idx_company_profile").select("symbol").neq("morningstar_code", 'null').eq("current_source", 2).execute()
ms_code_data = supabase.table("idx_company_profile").select("symbol").neq("morningstar_code", 'null').execute()
symbols = [d['symbol'].lower().replace('.jk', '') for d in ms_code_data.data]

# List to hold symbols with no data
no_data_urls = []
avail_data = []

# Loop through the symbols
for symbol in symbols:

    # Configure WebDriver options
    options = Options()
    options.add_argument("--headless") 

    # Initialize WebDriver with options
    driver = webdriver.Chrome(options=options)

    try:
        # Define the URL to check
        url = f"https://www.morningstar.com/stocks/xidx/{symbol}/financials"
        driver.get(url)
        time.sleep(10)  # Wait for the page to load

        try:
            no_data_element = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[2]/div[3]/section/div[2]/main/div[2]/div/div/div/div[1]/sal-components/div/sal-components-stocks-financials/div/div/div/div/div/div/div[2]/div[1]/div[1]/div/div/div/div[2]/div/div/div[2]/div/div/div[2]/span')
            if no_data_element:
                no_data_urls.append(symbol)
                print(len(no_data_urls), "Symbols with no data, are", no_data_urls)
        except NoSuchElementException:
            avail_data.append(symbol)
            print(len(avail_data), "Symbols with avail data, are", avail_data)

    except Exception as e:
        print(f"An error occurred for symbol {symbol}: {e}")
    
    finally:
        driver.quit()

    time.sleep(2)

# Print the symbols with no data
print(len(no_data_urls), "Symbols with no data, are", no_data_urls)
