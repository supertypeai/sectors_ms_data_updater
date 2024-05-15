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
ms_code_data = supabase.table("idx_company_profile").select("symbol").neq("morningstar_code", 'null').eq("current_source", 2).execute()
symbols = [d['symbol'].lower().replace('.jk', '') for d in ms_code_data.data]

# List to hold symbols with no data
no_data_urls = []

# Loop through the symbols
for symbol in symbols[:2]:
    print(symbol)

    # Configure WebDriver options
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    # options.add_argument("--disable-gpu")  # Disable GPU rendering
    # options.add_argument("--no-sandbox")  # Bypass OS security model
    # options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")  # Set user agent
    # options.add_argument("accept-language=en-US,en;q=0.9")  # Set accept language

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
