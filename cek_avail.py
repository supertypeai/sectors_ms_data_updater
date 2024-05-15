from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import os
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

url_supabase = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, key)

ms_code_data = supabase.table("idx_company_profile").select("symbol").neq("morningstar_code", 'null').eq("current_source", 2).execute()
symbols = [d['symbol'].lower().replace('.jk', '') for d in ms_code_data.data]

# List to hold symbols with no data
no_data_urls = []

for symbol in symbols[:10]:
    # Initialize the WebDriver (here using Chrome)
    print(symbol)
    driver = webdriver.Chrome()

    try:
        # Define the URL to check
        url = f"https://www.morningstar.com/stocks/xidx/{symbol}/financials"
        driver.get(url)
        time.sleep(10)  

        try:
            no_data_element = driver.find_element(By.XPATH, '//*[@id="__layout"]/div/div/div[2]/div[3]/section/div[2]/main/div[2]/div/div/div/div[1]/sal-components/div/sal-components-stocks-financials/div/div/div/div/div/div/div[2]/div[1]/div[1]/div/div/div/div[2]/div/div/div[2]/div/div/div[2]/span')
            if no_data_element:
                print(f"No data available on the page for symbol: {symbol}")
                no_data_urls.append(symbol)
        except NoSuchElementException:
            print(f"Data is available on the page for symbol: {symbol}")

    except Exception as e:
        print(f"An error occurred for symbol {symbol}: {e}")
    
    finally:
        driver.quit()

    time.sleep(2)

# Print the symbols with no data
print(len(no_data_urls), "Symbols with no data, are", no_data_urls)

