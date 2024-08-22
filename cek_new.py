import requests
import os
from dotenv import load_dotenv
import os
from supabase import create_client
import json
load_dotenv()

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

print(str(os.environ.get("rapid_api")))
# "X-RapidAPI-Key": str(os.environ.get("rapid_api")),
url_ms = "https://morning-star.p.rapidapi.com/stock/v2/get-financial-details"
headers = {
	"X-RapidAPI-Key": str(os.environ.get("rapid_api")),
	"X-RapidAPI-Host": "morning-star.p.rapidapi.com"
}

ms_code_data = supabase.table("idx_company_profile").select("morningstar_code","symbol").in_("current_source", [-1, 2]).neq("morningstar_code", 'null').execute()
ms_code_dict = {d['symbol']: d['morningstar_code'] for d in ms_code_data.data}
# print(ms_code_dict)
print(f"Got {len(ms_code_dict)} data")

# items = list(ms_code_dict.items())

# # Find the index of 'PURA.JK'
# index_of_pura = next((i for i, (key, value) in enumerate(items) if key == 'PURA.JK'), None)

# # Slice the list from the index of 'PURA.JK' onwards
# filtered_items = items[index_of_pura + 1:]

# # Convert back to dictionary
# filtered_data = dict(filtered_items)

# print(filtered_data)

max_index = len(ms_code_dict)
available_data = []
none_data = []
count = 0
# ms_code_dict = {"CASH.JK":"0P00013MK8"}
for symbol, code in ms_code_dict.items():
    if (count < max_index):
      try:
          responses = fetch_form_responses(["incomeStatement"], url_ms, headers, code)
          is_data = responses['incomeStatement']
          # print(is_data)
          quarter = is_data['columnDefs'][-2]
          print(f"[SUCCESS] Successfully fetch data for {symbol}")
          available_data.append(symbol)
      except Exception as e:
          print(f"[ERROR] Error fetching data for {symbol} with Morningstar code {code}: {e}")
          none_data.append(symbol)
      count += 1

print(f"Got {len(available_data)} available data")
print(f"Got {len(none_data)} None data")

DATA_DIR = os.path.join(os.getcwd(), "data")
available_filename = os.path.join(DATA_DIR, "available_data.json")
none_filename = os.path.join(DATA_DIR, "none_data.json")

with open(available_filename, 'w') as f:
    json.dump(available_data, f, indent=2)
with open(none_filename, 'w') as f:
    json.dump(none_data, f, indent=2)
