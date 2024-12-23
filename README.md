# Script for Retrieve IDX  Financial Data
This script retrieve financial data that not being retrieved from yahoo scraper. Source of this scraper is from morning star and we used rapid api to retrieve the data. The scraper is not automated in github action, there was a problem from the rapid api regarding it's time inference (so far we couldn't find the solution for this) and thats why this scraper should be run locally.

# How to run this Script?
- First of all, you need an env file that contains: supabase_url, supabase_key, and rapid_api token
- [cek_avail.py](./cek.py) is used to check the ticker availability in the Morningstar (you don't need to run it to retrieve the data)
- The main script to retrieve the financial data is [ms_scrap.py](./ms_scrap.py) and it is the only script you need to retrieve the financial data
- there are args for running this script: "a" to retrieve annual data, "q" to retrieve quarterly data