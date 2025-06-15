"""
DHIS2 Data Value Sets Sync Tool (Corrected)
==========================================

Properly handles data element groups for data value sets extraction.
"""

import requests
from requests.auth import HTTPBasicAuth
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import time
import json
import pandas as pd
from typing import List, Dict, Tuple, Optional, Set

# ======================
# CONFIGURATION SECTION
# ======================

# SOURCE DHIS2 # Replace with your DHIS2 instance parameters
SOURCE_URL = "source_url"
SOURCE_USERNAME = "xxxx"
SOURCE_PASSWORD = "xxxxx"

# TARGET DHIS2 # Replace with your DHIS2 instance parameters
TARGET_URL = "target_url"
TARGET_USERNAME = "xxxx"
TARGET_PASSWORD = "xxxxxx"

# Sync Parameters
DATA_ELEMENT_IDS = ["hJGEI95aFvT", "NTidDdqjEde","vTWSTwSpRcY","GYdTlN6Nc6R","lzhOxCHJMOV","lDa3SlEIWf1","QNndaeXVwkT","RyqiYTTblPQ","xQ36xicgmPg","PYXHwqL1mo0","Na3L65hr8Z4","lS3wbJFYokN","huIXzLMLsYU","jm0Ij4Cc0Lo","g1qHnEWvVg4","pbsBlUxd3hb","KgrydWwLnJK","xnV3dBsEFxS","vVTGS36Nr31","TCP0MseOg1j","PsOeFUyiPJ9","cqzerVmSI4c","GYRsg3I1gDU","t5cNd91LjM9","uCglZyh9T9U","SMl8JNFRtiR","wWIlCKHt0eG","PZSrdnlE85Y","Q4NL1LGUg5q","vuAIMDDVMFZ","IMk3E81xtn9","G0Nojd4lzXv"]
ORG_UNIT_GROUP_SET_ID = "UGrESb3REpI"
HEALTH_CENTER_IDS = ["Sb8LdrwZyY0","Yrv8w03wUp5","Iz3VUgMrbXq","VkrLHcp36Q2","eYYhIBKDGcR","sGFTHrzvmwg","CampNNHTKaX","icXQkY8QO0F","oZbLKB9YGjB","dVA5G7mTEhY","poVdSsH0ay5","LoWmMse432f","STHKrYQLg1C","JczrIvawhnh","yw3bM5H44In","G7MQUOujCYj","DiGMYK42H4M","fs5WUPFGKW9","zB4hYK2TDYG","i0quVWA9jpj","ecnuq319OHB","p07JSE35hYO","NLiK1VteA1p","HNy9MRjFwbL","afbaQBzN0Y2","SfpIaXFaEsn","PvqBlscjnd6","AIZVEhmpNxE","Hb3xh1AZ4ko","JgKd8QhwGSk","HxLyYRTTnrJ","AvFXR4II7Cf","EA54qxUIeka","GWsNIUlgQkp","NJ1eyPaE2xB","HAMC7f7M8ZH","ccb4Z5uRc38","pTY2ItSIMxX","ASaPHCwZDpc","NSMBwSl0Z45","WpejTddIS1r"]
HOSPITAL_IDS = ["VzYLSoWGyfv","m1km7CzhQCS","OW31YdsQj28","KXrfc0qAOE3","CWuvD3XhG3v"]

# Performance Settings
MAX_ORG_UNITS_PER_BATCH = 10
SEND_BATCH_SIZE = 1000
MAX_RETRIES = 2
RETRY_DELAY = 1

# ======================
# CORE FUNCTIONS
# ======================

def fetch_data_value_sets(data_element_ids: List[str],
                         periods: List[str],
                         org_units: List[str]) -> Optional[Dict]:
    """
    Fetch data using dataValueSets API with proper data element group handling.
    """
    try:
        params = {
            "paging": "false",  # Use group IDs here
            "includeDeleted": "false"
        }

        params_list = [(k, v) for k, v in params.items()]

        # Add each org unit, period, dataelement as a separate parameter
        params_list.extend([("orgUnit", ou) for ou in org_units])
        params_list.extend([("period", pe) for pe in periods])
        params_list.extend([("dataElement", de) for de in data_element_ids])

        response = requests.get(
            f"{SOURCE_URL}/api/dataValueSets",
            params=params_list,
            auth=HTTPBasicAuth(SOURCE_USERNAME, SOURCE_PASSWORD),
            timeout=30
        )

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                print("Invalid JSON response. Content:")
                print(response.text[:500])
                return None
        
        print(f"Error {response.status_code}: {response.text[:200]}")
        if response.status_code in [401, 403]:
            return
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        
    
    return None

def post_data(data : pd.DataFrame):

    # Split the dataframe into batches
    try:

        for i in range(0, len(data), SEND_BATCH_SIZE):
            batch = data.iloc[i:i+SEND_BATCH_SIZE]
                    
            # Create a list of datavalues
            data_values = []
            for index, row in batch.iterrows():
                value = {'dataElement': row["dataElement"], 
                    'period': row["period"],
                    'orgUnit': row["orgUnit"],
                    'categoryOptionCombo': row["categoryOptionCombo"],
                    'value': row["value"]
                    }
                data_values.append(value)

            values = {
                'dataValues': data_values
            }

            #Push values 
            response = requests.post(
                f'{TARGET_URL}/api/dataValueSets',
                auth=(TARGET_USERNAME, TARGET_PASSWORD),
                headers={'Content-Type': 'application/json'},
                data=json.dumps(values, allow_nan=True)
            )
            print(f"Susscessfully posted a batch {i//SEND_BATCH_SIZE + 1} of {len(data_values)} values to PBF DHIS2")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")

# ======================
# MAIN WORKFLOW
# ======================

def main():
    """Execute the complete sync workflow"""
    print("DHIS2 Data Value Sets Sync (Corrected)")
    print("=" * 50)
    start_time = time.time()
    
    try:
        # 1. Generate periods (last 6 months)
        today = date.today() - relativedelta(years=8)
        first = today.replace(day=1)
        last_month = first - timedelta(days=1)
        first_of_last_month = last_month.replace(day=1)
        last_2_month = first_of_last_month - timedelta(days=1)
        last_month_3 = last_2_month - timedelta(days=31)    
        last_month_4 = last_month_3 - timedelta(days=31)    
        last_month_5 = last_month_4 - timedelta(days=31)    
        last_month_6 = last_month_5 - timedelta(days=31)
        periods = [last_month_6.strftime("%Y%m"),last_month_5.strftime("%Y%m"),last_month_4.strftime("%Y%m"),last_month_3.strftime("%Y%m"), last_2_month.strftime("%Y%m"), last_month.strftime("%Y%m")]
        print("Periods included in query")
        print(periods)
        # 2. Get organization units
        org_units = HEALTH_CENTER_IDS + HOSPITAL_IDS
        
        # 3. Fetch data in batches
        all_data = []
        for i in range(0, len(org_units), MAX_ORG_UNITS_PER_BATCH):
            batch_ous = org_units[i:i + MAX_ORG_UNITS_PER_BATCH]
            print(f"\nFetching batch {i//MAX_ORG_UNITS_PER_BATCH + 1}")
            print("Fetch is starting")
            data = fetch_data_value_sets(DATA_ELEMENT_IDS, periods, batch_ous)
            print("Fetch is done")
            if data and "dataValues" in data:
                all_data.extend(data["dataValues"])
                print(f"Retrieved batch {i//MAX_ORG_UNITS_PER_BATCH + 1} of {len(data['dataValues'])} values from the HMIS")

        df = pd.DataFrame(all_data)
        post_data(df)
        
        if not all_data:
            print("No data values found")
            return
        
        # 4. Import to target (keep existing implementation)
        # ... [rest of your existing code]
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")

if __name__ == "__main__":
    main()