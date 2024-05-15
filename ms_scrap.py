import requests
import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client
import pandas as pd
import logging
import numpy as np
import argparse
import requests
import json

url = 'https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json'

response = requests.get(url)
data = response.json()
rate = float(data['USD']['IDR'])

url_supabase = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url_supabase, key)

def fetch_form_responses(url, headers, performanceId, get_data):
    responses = {}
    form_types = ['incomeStatement', 'balanceSheet', 'cashFlow']

    if get_data == "financials_details":

        for form in form_types:
            querystring = {
                "performanceId": performanceId,
                "dataType": 'Q' if args.quarter else 'A',
                "reportType": "A",
                "type": form
            }
            response = requests.get(url, headers=headers, params=querystring)
            responses[form] = response.json()

        return responses
    
    elif get_data == "financials":
        querystring = {
            "performanceId": performanceId,
            "interval": 'quarterly' if args.quarter else 'annual',
            "reportType": "A",
        }
        response = requests.get(url, headers=headers, params=querystring)
        
        return response.json()

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

def convert_df_to_records(df):
    temp_df = df.copy()
    temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
    temp_df = temp_df.replace({np.nan: None})
    for cols in temp_df.columns:
        if temp_df[cols].dtype == "datetime64[ns]":
            temp_df[cols] = temp_df[cols].astype(str)
        elif temp_df[cols].dtype == "float64":
            temp_df[cols] = temp_df[cols].astype(int)
    records = temp_df.to_dict("records")
    return records

def quarter_to_date(quarter_str):
    quarter_end_dates = {
        'Q1': '03-31',
        'Q2': '06-30',
        'Q3': '09-30',
        'Q4': '12-31'
    }
    
    quarter, year = quarter_str.split()
    year = int(year)
    end_date = quarter_end_dates.get(quarter)
    return f"{year}-{end_date}"

def main(args):
    url = "https://morning-star.p.rapidapi.com/stock/v2/get-financials"
    url_ms = "https://morning-star.p.rapidapi.com/stock/v2/get-financial-details"
    headers = {
        "X-RapidAPI-Key": str(os.environ.get("rapid_api")),
        "X-RapidAPI-Host": "morning-star.p.rapidapi.com"
    }

    # Retrieve Morning Star code from idx company profile
    ms_code_data = supabase.table("idx_company_profile").select("morningstar_code", "symbol").neq("morningstar_code", 'null').execute()
    ms_code_dict = {d['symbol']: d['morningstar_code'] for d in ms_code_data.data}

    data_list = []
    no_data = []
    logging.basicConfig(filename="log_error.log", level=logging.INFO)

    # for testing the looping is working correctly, notes: 3 of the codes have different wsj format
    # dict = {"AALI.JK":"0P0000BTGU","ASRM.JK":"0P0000CETF","BBCA.JK":"0P0000EP1E"}
    dict = {"AALI.JK":"0P0000BTGU"}
    # dict = {"ADRO.JK":"0P0000KTH0"}

    # for symbol, code in ms_code_dict.items():
    for symbol, code in dict.items():
        try:
            responses = fetch_form_responses(url, headers, code, "financials")
            responses_detail = fetch_form_responses(url_ms, headers, code, "financials_details")

            is_data = responses_detail['incomeStatement']
            bs_data = responses_detail['balanceSheet']
            cf_data = responses_detail['cashFlow']

            print(is_data)
            print(bs_data)
            print(cf_data)

            expected_value = [
            'Total Revenue','Net Income Available to Common Stockholders','EBITDA',
            'Diluted Weighted Average Shares Outstanding','Gross Profit','Pretax Income',
            'Provision for Income Tax', 'Interest Expense Net of Capitalized Interest','Total Operating Profit/Loss',

            'Cash Flows from/Used in Operating Activities, Direct', 'Free Cash Flow',
                        
            'Total Assets', 'Total Current Assets', 'Total Liabilities', 'Total Current Liabilities', 'Total Equity',
            'Total Debt','Equity Attributable to Parent Stockholders','Total Cash, Cash Equivalents and Short Term Investment', 'Cash']

            all_data = [is_data, bs_data, cf_data]
            quarter = is_data['columnDefs'][-2]
            quarter_index = is_data['columnDefs'].index(quarter)

            latest_quarters_data = {
                "symbol" : symbol,
                "date" : f'{quarter}-12-31' if args.annual else quarter_to_date(quarter),
                "source" : 3
            }
            
            for data in all_data:
                for row in data['rows']:
                    flattened_data = flatten_sublevel(row['subLevel'])
                    for i in range(len(flattened_data)):
                        row_label = flattened_data[i]['label']
                        if row_label in expected_value:
                            row_data = flattened_data[i]['datum'] 
                            if is_data['footer']['currency'] == 'USD':
                                quarter_value = (int(row_data[quarter_index]*rate*1000000) if row_data[quarter_index] is not None else np.nan)
                            else:
                                quarter_value = (int(float(row_data[quarter_index]) * 1000000) if row_data[quarter_index] is not None else np.nan)
                            label = f"{row_label}"
                            latest_quarters_data[label] = quarter_value
                            print(label, quarter_value)

            expected_financials = [
            "Free Cash Flow", "Total Operating Profit/Loss", "Total Debt", "EBITDA", "Net Income Available to Common Stockholders"
            ]
            tabs = ['incomeStatement', 'balanceSheet', 'cashFlow']
            for tab in tabs:
                for row in responses[tab]["rows"]:
                    if row["label"] in expected_financials:
                        if is_data['footer']['currency'] == 'USD':
                            latest_quarters_data[row["label"]] = (int(float(row["datum"][-2]) * rate * 1000000000) if row["datum"][-2] is not None else np.nan)
                        else:
                            latest_quarters_data[row["label"]] = (int(float(row["datum"][-2]) * 1000000000) if row["datum"][-2] is not None else np.nan)
            

            for value in expected_value:
                if value not in latest_quarters_data.keys():
                    latest_quarters_data[value] = np.nan
                    
            data_list.append(latest_quarters_data)
            print(f"data with symbol {symbol} successfully retrieved")

        except Exception as e:
            """
            the error should be happened because the data is not available in morning star
            """
            logging.error(f"retrieved failed in data with morning star code: {code} \nwith error: {e}")
            print(f"retrieved failed in data with morning star code: {code} \nwith error: {e}")
            no_row = {}
            no_row["symbol"] = symbol
            no_row["error"] = e
            no_data.append(no_row)

    df = pd.DataFrame(data_list)
    df["Total Assets - Total Current Assets"] = df["Total Assets"] - df["Total Current Assets"]
    df["EBIT"] = df["Pretax Income"] - df["Interest Expense Net of Capitalized Interest"]

    columns_rename = {
        "Cash Flows from/Used in Operating Activities, Direct": "net_operating_cash_flow",
        "Total Assets": "total_assets",
        "Total Liabilities": "total_liabilities",
        "Total Current Liabilities": "total_current_liabilities",
        "Total Equity": "total_equity",
        "Total Revenue": "total_revenue",
        "Net Income Available to Common Stockholders": "net_income",
        "Total Debt": "total_debt",
        "Equity Attributable to Parent Stockholders": "stockholders_equity",
        "EBIT":"ebit",
        "EBITDA": "ebitda",
        "Total Cash, Cash Equivalents and Short Term Investment": "cash_and_short_term_investments",
        "Cash": "cash_only",
        "Diluted Weighted Average Shares Outstanding": "diluted_shares_outstanding",
        "Gross Profit": "gross_income",
        "Pretax Income": "pretax_income",
        "Provision for Income Tax": "income_taxes",
        "Total Assets - Total Current Assets": "total_non_current_assets",
        "Free Cash Flow": "free_cash_flow",
        "Interest Expense Net of Capitalized Interest": "interest_expense_non_operating",
        "Total Operating Profit/Loss": "operating_income"
    }
    df = df.rename(columns=columns_rename).drop(["Total Current Assets"], axis = 1)
    df[['income_taxes', 'interest_expense_non_operating']] *= -1

    records = convert_df_to_records(df)

    table_name = 'idx_financials_quarterly' if args.quarter else 'idx_financials_annual'
    df.to_csv(f"{table_name}.csv", index = False)

    try:
        supabase.table(f"{table_name}").upsert(records, returning='minimal').execute()
        print("Upsert operation successful.")
    except Exception as e:
        print(f"Error during upsert operation: {e}")

    # no_data = pd.DataFrame(no_data)
    # no_data.to_csv("no_data.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update financial data. If no argument is specified, the annual data will be updated.")
    parser.add_argument("-a", "--annual", action="store_true", default=False, help="Update annual financial data")
    parser.add_argument("-q", "--quarter", action="store_true", default=False, help="Update quarter financial data")

    args = parser.parse_args()
    if args.annual and args.quarter:
        print("Error: Please specify either -a or -q, not both.")
        raise SystemExit(1)
    main(args)