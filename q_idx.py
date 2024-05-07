import requests
import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client

url_supabase = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, key)

def fetch_form_responses(form_types, url, headers, performanceId):
    responses = {}

    for form in form_types:
        querystring = {
            "performanceId": performanceId,
            "dataType": "Q", #ganti A untuk annually, dibuat parameter aja nanti kalo smisal berhasil
            "reportType": "A",
            "type": form
        }
        response = requests.get(url, headers=headers, params=querystring)
        responses[form] = response.json()

    return responses

url_ms = "https://morning-star.p.rapidapi.com/stock/v2/get-financial-details"
headers = {
	"X-RapidAPI-Key": str(os.environ.get("rapid_api")),
	"X-RapidAPI-Host": "morning-star.p.rapidapi.com"
}

# Define form types
form_types = ['incomeStatement', 'balanceSheet', 'cashFlow']

# Retrieve Morning Star code from idx company profile
ms_code_data = supabase.table("idx_company_profile").select("morningstar_code").neq("morningstar_code", 'null').execute()
ms_code = list({d['morningstar_code'] for d in ms_code_data.data})
print(ms_code)

# Next phase would be moving the code into for loop to process all ms code
# for code in ms_code:
# 	responses = fetch_form_responses(form_types, url_ms, headers, code)

responses = fetch_form_responses(form_types, url_ms, headers, "0P0000BTGU")

is_data = responses['incomeStatement']
bs_data = responses['balanceSheet']
cf_data = responses['cashFlow']

def flatten_sublevel(data, parent_id=''):
    flattened_data = []

    for sublevel in data:
        flattened_sublevel = {
            'parent_id': parent_id,
            'label': sublevel['label'],
            'dataPointId': sublevel['dataPointId'],
        }
        if 'datum' in sublevel:
            flattened_sublevel['datum'] = sublevel['datum']
        flattened_data.append(flattened_sublevel)

        if 'subLevel' in sublevel:
            flattened_data.extend(flatten_sublevel(sublevel['subLevel'], sublevel['dataPointId']))

    return flattened_data

quarter = is_data['columnDefs'][-2]
quarter_index = is_data['columnDefs'].index(quarter)

# EBITA, FCF , Total Debt diatas

# Quarterly
expected_value = ['Total Revenue','Net Income Available to Common Stockholders','EBITDA',
                  'Diluted Weighted Average Shares Outstanding','Gross Profit','Pretax Income',
                  'Provision for Income Tax', 'Interest Expense Net of Capitalized Interest','Total Operating Profit/Loss',

                  'Cash Flows from/Used in Operating Activities, Direct', 'Free Cash Flow',
                    
                  'Total Assets', 'Total Liabilities', 'Total Current Liabilities', 'Total Equity',
                  'Total Debt','Equity Attributable to Parent Stockholders','Total Cash, Cash Equivalents and Short Term Investment']

# Annually ( JANGAN LUPA GANTI SESUAI NAMA KOLOM YANG UDAH DI LIST)
# expected_value = ['Total Revenue','Net Income Available to Common Stockholders','EBITDA',
#                   'Diluted Weighted Average Shares Outstanding','Gross Profit','Pretax Income',
#                   'Provision for Income Tax', 'Interest Expense Net of Capitalized Interest','Total Operating Profit/Loss',

#                   'Cash Flows from/Used in Operating Activities, Direct', 'Free Cash Flow',
                    
#                   'Total Assets', 'Total Liabilities', 'Total Current Liabilities', 'Total Equity',
#                   'Total Debt','Equity Attributable to Parent Stockholders','Total Cash, Cash Equivalents and Short Term Investment']

latest_quarters_data = {}
all_data = [is_data, bs_data, cf_data]
for data in all_data:
  for row in data['rows']:
    flattened_data = flatten_sublevel(row['subLevel'])
    print(flattened_data)
    for i in range(len(flattened_data)):
      row_label = flattened_data[i]['label']
      if row_label in expected_value:
        row_data = flattened_data[i]['datum'] 
        quarter_value = row_data[quarter_index]
        label = f"{row_label}"
        latest_quarters_data[label] = quarter_value

print("Data for the latest quarters:")
for label, value in latest_quarters_data.items():
    print(f"{label}: {value}")

