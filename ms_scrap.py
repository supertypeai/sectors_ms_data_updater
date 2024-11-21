import requests
import os
from dotenv import load_dotenv
import os
from supabase import create_client
import pandas as pd
import logging
import numpy as np
import argparse
import requests
import json
from datetime import datetime
load_dotenv()

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

def process(url, url_ms, headers, avail_dict):
    failed_symbols = []
    for symbol, code in avail_dict.items():
        try:
            data_list = []
            responses = fetch_form_responses(url, headers, code, "financials")
            responses_detail = fetch_form_responses(url_ms, headers, code, "financials_details")

            is_data = responses_detail['incomeStatement']
            bs_data = responses_detail['balanceSheet']
            cf_data = responses_detail['cashFlow']

            expected_value = [
            'Total Revenue','Net Income Available to Common Stockholders','EBITDA',
            'Diluted Weighted Average Shares Outstanding','Gross Profit','Pretax Income',
            'Provision for Income Tax', 'Interest Expense Net of Capitalized Interest','Total Operating Profit/Loss',

            'Cash Flows from/Used in Operating Activities, Direct', 'Free Cash Flow',

            'Total Assets', 'Total Current Assets', 'Total Non-Current Assets', 'Total Liabilities', 'Total Current Liabilities', 'Total Equity',
            'Equity Attributable to Parent Stockholders','Cash, Cash Equivalents and Short Term Investments', 'Cash', "Debt and Capital Lease Obligations",
            'Cash and Cash Equivalents', 'Current Debt And Capital Lease Obligation', 'Long Term Debt And Capital Lease Obligation']

            # 'Total Non-Current Assets', 

            all_data = [is_data, bs_data, cf_data]
            last_quarter = [
                (lambda x: x.remove('TTM') or x[-1] if 'TTM' in x else x[-1])(lst['columnDefs'])
                for lst in [is_data, bs_data, cf_data]
            ]
            indices = [lst['columnDefs'].index(value) for lst, value in zip([is_data, bs_data, cf_data], last_quarter)]

            latest_quarters_data = {
                "symbol" : symbol,
                "date" : f'{last_quarter[0]}-12-31' if args.annual else quarter_to_date(last_quarter[0]),
                "source" : 3
            }

            for data, index in zip(all_data, indices):
                for row in data['rows']:
                    flattened_data = flatten_sublevel(row['subLevel'])
                    # print(flattened_data)
                    for i in range(len(flattened_data)):
                        row_label = flattened_data[i]['label']
                        if row_label in expected_value:
                            row_data = flattened_data[i]['datum'] 
                            if data['footer']['currency'] == 'USD' and row_label != 'Diluted Weighted Average Shares Outstanding':
                                if data["footer"]["orderOfMagnitude"] == "Million":
                                    quarter_value = (int(row_data[index]*rate*1000000) if row_data[index] is not None else np.nan)
                                elif data["footer"]["orderOfMagnitude"] == "Billion":
                                    quarter_value = (int(row_data[index]*rate*1000000000) if row_data[index] is not None else np.nan)
                                else:
                                    print(f"data {symbol} in tab {data} have no order of magnitude")
                                    logging.error(f"{today_date}: data {symbol} in tab {data} have no order of magnitude")
                            else:
                                if data["footer"]["orderOfMagnitude"] == "Million":
                                    quarter_value = (int(float(row_data[index]) * 1000000) if row_data[index] is not None else np.nan)
                                elif data["footer"]["orderOfMagnitude"] == "Billion":
                                    quarter_value = (int(float(row_data[index]) * 1000000000) if row_data[index] is not None else np.nan)
                                else:
                                    print(f"data {symbol} in tab {data} have no order of magnitude")
                                    logging.error(f"{today_date}: data {symbol} in tab {data} have no order of magnitude")
                            label = f"{row_label}"
                            latest_quarters_data[label] = quarter_value
                            # print(label, quarter_value)

            expected_financials = ["Free Cash Flow", "Total Operating Profit/Loss", "Total Debt", "EBITDA"]
            tabs = ['incomeStatement', 'balanceSheet', 'cashFlow']
            for tab in tabs:
                for row in responses[tab]["rows"]:
                    if row["label"] in expected_financials:
                        if responses[tab]['footer']['currency'] == 'USD':
                            if responses[tab]["footer"]["orderOfMagnitude"] == "Million":
                                value = (int(float(row["datum"][-2]) * rate * 1000000) if row["datum"][-2] is not None else np.nan)
                                latest_quarters_data[row["label"]] = value
                            elif responses[tab]["footer"]["orderOfMagnitude"] == "Billion":
                                value = (int(float(row["datum"][-2]) * rate * 1000000000) if row["datum"][-2] is not None else np.nan)
                                latest_quarters_data[row["label"]] = value
                            else:
                                print(f"data {symbol} in tab {tab} have no order of magnitude")
                                logging.error(f"{today_date}: data {symbol} in tab {tab} have no order of magnitude")
                        else:
                            if responses[tab]["footer"]["orderOfMagnitude"] == "Million":
                                value = (int(float(row["datum"][-2]) * 1000000) if row["datum"][-2] is not None else np.nan)
                                latest_quarters_data[row["label"]] = value
                            elif responses[tab]["footer"]["orderOfMagnitude"] == "Billion":
                                value = (int(float(row["datum"][-2]) * 1000000000) if row["datum"][-2] is not None else np.nan)
                                latest_quarters_data[row["label"]] = value
                            else:
                                print(f"data {symbol} in tab {tab} have no order of magnitude")
                                logging.error(f"{today_date}: data {symbol} in tab {tab} have no order of magnitude")

                        # print(row["label"], value)
                        

            for value in expected_value:
                if value not in latest_quarters_data.keys():
                    latest_quarters_data[value] = np.nan
                                
            data_list.append(latest_quarters_data)
            print(f"data with symbol {symbol} successfully retrieved")

            df = pd.DataFrame(data_list)

            df["EBIT"] = df["Pretax Income"] - df["Interest Expense Net of Capitalized Interest"]
            if df["Total Debt"].isnull().sum() == 1:
                df["Total Debt"] = df['Debt and Capital Lease Obligations']
            if df["Total Debt"].isnull().sum() == 1:
                df["Total Debt"] = df['Current Debt And Capital Lease Obligation'] + df['Long Term Debt And Capital Lease Obligation']
            if df["Total Non-Current Assets"].isnull().sum() == 1:
                df["Total Non-Current Assets"] = df["Total Assets"] - df["Total Current Assets"]

            df["total_cash_and_due_from_banks"] = df["Cash and Cash Equivalents"] - df["Cash"]
            if df["total_cash_and_due_from_banks"][0] == 0:
                df.loc[0, "total_cash_and_due_from_banks"] = np.nan

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
                "Cash, Cash Equivalents and Short Term Investments": "cash_and_short_term_investments",
                "Cash": "cash_only",
                "Diluted Weighted Average Shares Outstanding": "diluted_shares_outstanding",
                "Gross Profit": "gross_income",
                "Pretax Income": "pretax_income",
                "Provision for Income Tax": "income_taxes",
                "Total Non-Current Assets": "total_non_current_assets",
                "Free Cash Flow": "free_cash_flow",
                "Interest Expense Net of Capitalized Interest": "interest_expense_non_operating",
                "Total Operating Profit/Loss": "operating_income",
            }
            df = df.rename(columns=columns_rename).drop(["Total Current Assets", 'Cash and Cash Equivalents', 'Current Debt And Capital Lease Obligation', 'Long Term Debt And Capital Lease Obligation', "Debt and Capital Lease Obligations"], axis = 1)
            df[['income_taxes', 'interest_expense_non_operating']] *= -1

            records = convert_df_to_records(df)

            table_name = 'idx_financials_quarterly' if args.quarter else 'idx_financials_annual'
            # df.to_csv(f"{symbol}.csv", index = False)
            df.to_csv(f"{table_name}_testing.csv", index = False)

            try:
                supabase.table(f"{table_name}").upsert(records, returning='minimal').execute()
                supabase.table(f"{table_name}").upsert(records[0], returning='minimal').execute()
                print(table_name)
                print(records[0])
                print("Upsert operation successful.")
            except Exception as e:
                print(f"Error during upsert operation: {e}")
        except Exception as e:
            """
            the error should be happened because the data is not available in morning star
            """
            logging.error(f"{today_date}: retrieved failed in data with morning star code: {symbol} {code} with error: {e}")
            print(f"retrieved failed in data with morning star code: {symbol} {code} with error: {e}")
            failed_symbols.append(symbol)
    
    return failed_symbols

def save_failed_symbols(failed_symbols, filename):
    with open(filename, 'w') as file:
        for item in failed_symbols:
            file.write(f"{item}\n")

def load_failed_symbols(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file]


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

    no_data = []
    logging.basicConfig(filename="log_error.log", level=logging.INFO)

    avail_data = [
        'GEMS.JK', 'AYAM.JK', 'BBTN.JK', 'ROCK.JK', 'GUNA.JK', 'PTPS.JK', 'HALO.JK', 'TRON.JK', 'GOLD.JK', 'HADE.JK', 'KRAH.JK', 'BOAT.JK',
        'PNBN.JK', 'PKPK.JK', 'RONY.JK', 'DPNS.JK', 'MHKI.JK', 'DAAZ.JK', 'SAPX.JK', 'IPPE.JK', 'GHON.JK', 'GSMF.JK', 'AMIN.JK', 'ASLI.JK',
        'ETWA.JK', 'WIFI.JK', 'KOKA.JK', 'SOFA.JK', 'KOBX.JK', 'BAIK.JK', 'MCOR.JK', 'ASHA.JK', 'RGAS.JK', 'MDIA.JK', 'CNTX.JK', 'JMAS.JK',
        'ACRO.JK', 'MAMI.JK', 'AXIO.JK', 'ADMF.JK', 'CRAB.JK', 'DIGI.JK', 'ALKA.JK', 'TNCA.JK', 'ITIC.JK', 'PACK.JK', 'INDX.JK', 'LMAS.JK',
        'KBRI.JK', 'APII.JK', 'MREI.JK', 'HITS.JK', 'TGUK.JK', 'TOOL.JK', 'BINA.JK', 'BABP.JK', 'BOBA.JK', 'SSTM.JK', 'TOSK.JK', 'TRUS.JK',
        'NAIK.JK', 'BKSW.JK', 'PEVE.JK', 'ABDA.JK', 'BUMI.JK', 'UFOE.JK', 'POLY.JK', 'BMRI.JK', 'NTBK.JK', 'ALTO.JK', 'LRNA.JK', 'VINS.JK',
        'VIVA.JK', 'BLUE.JK', 'CBRE.JK', 'VERN.JK', 'PNSE.JK', 'SMLE.JK', 'TBMS.JK', 'DATA.JK', 'PSDN.JK', 'ZATA.JK', 'ZBRA.JK', 'BOSS.JK',
        'RANC.JK', 'DOSS.JK', 'ASJT.JK', 'HBAT.JK', 'PTMR.JK', 'JKSW.JK', 'KMDS.JK', 'KRYA.JK', 'KICI.JK', 'BABY.JK', 'KOCI.JK', 'PUDP.JK',
        'NSSS.JK', 'TYRE.JK', 'SOUL.JK', 'LMPI.JK', 'GRPH.JK', 'NEST.JK', 'DNAR.JK', 'HAIS.JK', 'PNGO.JK', 'SMGA.JK', 'FINN.JK', 'TURI.JK',
        'TRJA.JK', 'AWAN.JK', 'BTON.JK', 'BEEF.JK', 'NCKL.JK', 'HOPE.JK', 'SNLK.JK', 'OBMD.JK', 'COAL.JK', 'TAMA.JK', 'FLMC.JK', 'MEGA.JK',
        'COCO.JK', 'POOL.JK', 'EPAC.JK', 'GDYR.JK', 'ALII.JK', 'GAMA.JK', 'MAGP.JK', 'SDRA.JK', 'HATM.JK', 'BSIM.JK', 'MMIX.JK', 'FIMP.JK',
        'BNLI.JK', 'UNIT.JK', 'AYLS.JK', 'COWL.JK', 'SCNP.JK', 'ERTX.JK', 'SKYB.JK', 'BBMD.JK', 'ESIP.JK', 'SKRN.JK', 'MKAP.JK', 'INCF.JK',
        'BDMN.JK', 'LCKM.JK', 'SPRE.JK', 'PGJO.JK', 'BACA.JK', 'TALF.JK', 'TUGU.JK', 'MANG.JK', 'PTPW.JK', 'PART.JK', 'SOLA.JK', 'LEAD.JK',
        'BTPS.JK', 'MARI.JK', 'AMAG.JK', 'BJBR.JK', 'SWAT.JK', 'AIMS.JK', 'NOBU.JK', 'HYGN.JK', 'AMAR.JK', 'LAJU.JK', 'MEJA.JK', 'BBSI.JK',
        'LIVE.JK', 'RCCC.JK', 'MITI.JK', 'MPIX.JK', 'BCIC.JK', 'OPMS.JK', 'PLAN.JK', 'ASMI.JK', 'CASH.JK', 'BVIC.JK', 'BINO.JK', 'BRMS.JK',
        'CBPE.JK', 'MGLV.JK', 'AREA.JK', 'RELI.JK', 'ZYRX.JK', 'TRGU.JK', 'OCAP.JK', 'FITT.JK', 'CHIP.JK', 'NIPS.JK', 'MGNA.JK', 'BNGA.JK',
        'FOOD.JK', 'IMAS.JK', 'PNIN.JK', 'NISP.JK', 'BNBA.JK', 'PDES.JK', 'HDIT.JK', 'LIFE.JK', 'UDNG.JK', 'BPFI.JK', 'TIRA.JK', 'SATU.JK',
        'PNLF.JK', 'CBMF.JK', 'SBAT.JK', 'DAYA.JK', 'LABS.JK', 'SUGI.JK', 'ISEA.JK', 'BBCA.JK', 'EAST.JK', 'DUCK.JK', 'AGRS.JK', 'IKAN.JK',
        'NANO.JK', 'FORZ.JK', 'JSKY.JK', 'OLIV.JK', 'LUCK.JK', 'FWCT.JK', 'PLAS.JK', 'BBYB.JK', 'BAPI.JK', 'ATAP.JK', 'BGTG.JK', 'CLAY.JK',
        'BBRI.JK', 'BATR.JK', 'AMMN.JK', 'KPAS.JK', 'MABA.JK', 'TRUE.JK', 'BRIS.JK', 'AGRO.JK', 'AHAP.JK', 'SBMA.JK', 'KLAS.JK', 'BSML.JK',
        'SWID.JK', 'MTRA.JK', 'IBOS.JK', 'KBAG.JK', 'BUAH.JK', 'BIKE.JK', 'KBLM.JK', 'EKAD.JK', 'SDMU.JK', 'RAFI.JK', 'HILL.JK', 'KAYU.JK',
        'PPGL.JK', 'KETR.JK', 'PAMG.JK', 'EURO.JK', 'KKES.JK', 'MDKI.JK', 'BMAS.JK', 'BSBK.JK', 'INRU.JK', 'SRAJ.JK', 'LION.JK', 'NAYZ.JK',
        'IRSX.JK', 'PRAS.JK', 'KDSI.JK', 'CANI.JK', 'MPXL.JK', 'ARTO.JK', 'TMPO.JK', 'LOPI.JK', 'PURI.JK', 'DEAL.JK', 'AKKU.JK', 'AGAR.JK',
        'SINI.JK', 'ASRM.JK', 'DEFI.JK', 'HKMU.JK', 'HOME.JK', 'DSFI.JK', 'CINT.JK', 'KJEN.JK', 'BEER.JK', 'PURE.JK', 'BELL.JK', 'PSKT.JK',
        'APLI.JK', 'TRIS.JK', 'NASI.JK', 'SCPI.JK', 'SHIP.JK', 'BEKS.JK', 'INTD.JK', 'IKBI.JK', 'GLVA.JK', 'WAPO.JK', 'KOPI.JK', 'LMSH.JK',
        'SMIL.JK', 'SMKL.JK', 'CPRI.JK', 'IBFN.JK', 'DEWI.JK', 'ENZO.JK', 'MFMI.JK', 'LPGI.JK', 'CHEM.JK', 'SOTS.JK', 'CASA.JK', 'SMKM.JK',
        'ESTA.JK', 'GPSO.JK', 'BHAT.JK', 'NINE.JK', 'BBKP.JK', 'GOLL.JK', 'DOID.JK', 'SOSS.JK', 'ESTI.JK', 'INET.JK', 'MKNT.JK', 'BANK.JK',
        'FUTR.JK', 'FIRE.JK', 'KUAS.JK', 'CSIS.JK', 'MMLP.JK', 'ASPI.JK', 'RMBA.JK', 'INCI.JK', 'JAST.JK', 'TRAM.JK', 'LPIN.JK', 'LMAX.JK',
        'PURA.JK', 'DCII.JK', 'LUCY.JK', 'INOV.JK', 'PNBS.JK', 'DADA.JK', 'KLIN.JK', 'BBHI.JK', 'BIMA.JK', 'RMKO.JK', 'BPTR.JK', 'ATIC.JK',
        'MTSM.JK', 'RBMS.JK', 'MTWI.JK', 'CSRA.JK', 'NICK.JK', 'FORU.JK', 'ASBI.JK', 'BIKA.JK', 'RUNS.JK', 'ARTA.JK', 'CCSI.JK', 'JATI.JK',
        'BSWD.JK', 'MEDS.JK', 'BJTM.JK', 'IDEA.JK', 'KPAL.JK', 'SUNI.JK', 'UVCR.JK', 'GLOB.JK', 'KDTN.JK', 'TDPM.JK', 'PANR.JK', 'MBTO.JK',
        'BTPN.JK', 'BBSS.JK', 'BMBL.JK', 'SIMA.JK', 'BAUT.JK', 'CUAN.JK', 'BOLA.JK', 'IPAC.JK', 'DGNS.JK', 'OILS.JK', 'RIMO.JK', 'OKAS.JK',
        'ASDM.JK', 'ICON.JK', 'NELY.JK', 'TECH.JK', 'REAL.JK', 'SEMA.JK', 'WICO.JK', 'BAYU.JK', 'NUSA.JK', 'WOWS.JK', 'PTDU.JK', 'NZIA.JK',
        'CAKK.JK', 'OASA.JK', 'SMMA.JK', 'TRST.JK', 'BBNI.JK', 'MYRX.JK', 'AKSI.JK', 'TRUK.JK', 'UANG.JK', 'LFLO.JK', 'VAST.JK', 'VOKS.JK',
        'VRNA.JK', 'IKPM.JK', 'WGSH.JK', 'YELO.JK', 'ZONE.JK', 'INPC.JK', 'MPOW.JK', 'INDO.JK', 'BNII.JK', 'GJTL.JK', 'PBRX.JK', 'RSCH.JK',
        'TELE.JK', 'HAJJ.JK', 'KING.JK', 'MUTU.JK', 'ISAP.JK', 'GRPM.JK', 'PPRI.JK', 'HUMI.JK', 'ARMY.JK', 'RELF.JK', 'MAXI.JK', 'MENN.JK',
        'NPGF.JK', 'TAYS.JK', 'AMMS.JK', 'LCGP.JK', 'IOTF.JK', 'PMMP.JK', 'LABA.JK', 'TRIL.JK', 'BESS.JK', 'GTRA.JK', 'TOYS.JK', 'ELIT.JK',
        'CRSN.JK', 'AEGS.JK', 'SICO.JK', 'FOLK.JK', 'WINR.JK', 'GRIA.JK', 'GULA.JK', 'WIDI.JK', 'MSIE.JK', 'PADA.JK'
    ]

    # avail_data =  [ 'bbca', 'amar', 'maba', 'cowl', 'btps', 'agrs', 'agro', 'life','bmas', 'bvic', 'mega', 'bsim', 'arto', 
    #                 'mrei', 'asrm', 'bmri', 'bbri', 'home', 'bpfi', 'smil', 'lpgi', 'bbkp', 'bris', 'bina', 'inet', 'bank', 
    #                 'krah', 'dnar', 'amag', 'bcic', 'dcii', 'hill', 'plas', 'beks', 'hatm', 'pnbs', 'bbhi', 'nips', 'irsx', 
    #                 'mcor', 'bbtn', 'bgtg', 'maya', 'bhat', 'nisp', 'nobu', 'goll', 'bnga', 'imas', 'pnbn', 'bswd', 'pnin', 
    #                 'bbyb', 'bjtm', 'babp', 'bbmd', 'abda', 'admf', 'kbri', 'jsky', 'baca', 'sdra', 'miti', 'tram', 'buah', 
    #                 'btpn', 'bksw', 'bnba', 'bbsi', 'cuan', 'bnli', 'gsmf', 'asdm', 'casa', 'bdmn', 'pnlf', 'nusa', 'beef', 
    #                 'skyb', 'sugi', 'smma', 'asmi', 'tugu', 'myrx', 'bbni', 'inpc', 'bnii', 'bjbr', 'hotl', 'army', 'duck', 
    #                 'magp', 'npgf', 'lcgp', 'tril', 'forz', 'rimo']
    
    # avail_data = [item.upper() + '.JK' for item in avail_data]
    avail_dict = {key: value for key, value in ms_code_dict.items() if key in avail_data}

    failed_symbols = process(url, url_ms, headers, avail_dict)
    save_failed_symbols(failed_symbols, 'no_data.txt')

    # Retry failed symbols
    # print("Retrying failed symbols")
    # failed_symbols = load_failed_symbols('no_data.txt')
    # retry_symbols = {key: value for key, value in ms_code_dict.items() if key in failed_symbols}
    # failed_symbols = process(url, url_ms, headers, retry_symbols)
    # save_failed_symbols(failed_symbols, 'fix_no_data.txt')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update financial data. If no argument is specified, the annual data will be updated.")
    parser.add_argument("-a", "--annual", action="store_true", default=False, help="Update annual financial data")
    parser.add_argument("-q", "--quarter", action="store_true", default=False, help="Update quarter financial data")

    args = parser.parse_args()
    if args.annual and args.quarter:
        print("Error: Please specify either -a or -q, not both.")
        raise SystemExit(1)
    
    url_currency = 'https://raw.githubusercontent.com/supertypeai/sectors_get_conversion_rate/master/conversion_rate.json'

    response = requests.get(url_currency)
    data = response.json()
    rate = float(data['USD']['IDR'])

    url_supabase = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase = create_client(url_supabase, key)

    today_date = datetime.today().strftime('%Y-%m-%d')

    main(args)



