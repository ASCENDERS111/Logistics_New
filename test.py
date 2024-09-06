import json
import gspread # type: ignore
import pandas as pd # type: ignore
import re
import numpy as np # type: ignore
import requests
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials # type: ignore
from delivery_date_fetcher import (
    get_ups_access_token, 
    fetch_ups_delivery_date, 
    get_fedex_access_token, 
    fetch_fedex_delivery_date
)
import concurrent.futures
import threading

# Load credentials from credentials.json
with open('credentials.json') as f:
    credentials = json.load(f)

# Extract credentials
fedex_params = credentials.get('fedex_params')
ups_params = credentials.get('ups_params')
zoho_params = credentials.get('zoho_params')

# FedEx API Credentials
CLIENT_ID = fedex_params.get('CLIENT_ID')
CLIENT_SECRET = fedex_params.get('CLIENT_SECRET')

# UPS API Credentials
client_key = ups_params.get('client_key')
client_secret = ups_params.get('client_secret')

# Step 1: Fetch data from Zoho
def fetch_data_from_zoho():
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "client_id": zoho_params.get("client_id"),
        "client_secret": zoho_params.get("client_secret"),
        "grant_type": zoho_params.get("grant_type"),
        "scope": zoho_params.get("scope"),
        "soid": zoho_params.get("soid")
    }

    response = requests.post(url, params=params)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print("Zoho Token obtained successfully")
    else:
        print(f"Failed to obtain Zoho token. Status code: {response.status_code}")
        exit()

    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Logistic 2?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rows_data = []

        for row in root.findall('.//row'):
            row_data = {}
            for column in row:
                column_name = column.attrib.get('name')
                row_data[column_name] = column.text
            rows_data.append(row_data)

        df = pd.DataFrame(rows_data)

        # Apply the filters after fetching data from Zoho
        df = df[df['Version Sheet.Stage'] != 'ORDER CLOSED']
        df = df[~df['Version Sheet.Stage'].str.contains('TRACK 2') & ~df['Version Sheet.Stage'].str.contains('TRACK 3')]
        df = df[~df['Tracking Courier Details.Tracking Destination'].isin(['Track 3', 'Track 2'])]

        # Add necessary columns with default values
        df[['Pic', 'Item Pics', 'Weight_LBS', 'Dimensions_Inches', 'Remark by Robert', 'Remark by Logistic team', 'Delivery Date','book','comp']] = None
        df['SNo'] = df['SNo'].astype(int)
        df['Purchase Cost'] = df['Purchase Cost'].astype(float)
        df['Pic'] = df['Purchase Cost'].apply(lambda x: 'Yes' if pd.isna(x) or x > 250 else 'No')
        df['Item Pics'] = df['Purchase Cost'].apply(lambda x: 'Yes' if pd.isna(x) or x > 1000 else 'No')
        df['Purchase Cost'] = df['Purchase Cost'].astype(str)
        df['Version Sheet.Urgent Shipment'] = df['Version Sheet.Urgent Shipment'].map({'Urgent Shipment - Logistics':'Urgent','Urgent Shipment - OP':'Urgent'})
        df['SNo'] = np.where(df['Version Sheet.Urgent Shipment'].fillna('') != '', 
                             df['SNo'].astype(str) + "|" + df['Version Sheet.Urgent Shipment'].astype(str), 
                             df['SNo'].astype(str))
        df['EUC Upload'] = df['EUC Upload'].map({'Yes': 'EucUploaded', 'No': ''})
        df['Destination Point'] = df['Version Sheet.Destination Point'] + '-' + df['EUC Upload']
        df.loc[df['Version Sheet.Destination Point'].str.contains('EXW', na=False), 'Item Pics'] = 'Yes'
        
        df.loc[df['Supplier (Grainger / Non-Grainger)'].str.contains('ebay', case=False, na=False), 'Item Pics'] = 'Yes'
        df['Version Sheet.Payment Method'] = df['Version Sheet.Payment Method'].fillna('')
        df['Stage_new'] = df['Version Sheet.Stage'] + '--' + df['Version Sheet.Payment Method']
        df['Date tracking Enter'] = pd.to_datetime(df['Date tracking Enter'], format='%d/%m/%Y %H:%M:%S')

        # Reorder columns as needed
        ordered_columns = [
            'Parent ID', 'SNo', 'Destination Point', 'Batch', 'Invoice', 
            'Version Sheet.Order Payment Received Status', 'Stage_new', 'Date tracking Enter',
            'Supplier Name', 'Supplier (Grainger / Non-Grainger)', 'Version Sheet.Placed the Order with Supplier',
            'Tracking Number', 'Delivery Date', 'Pic', 'Item Pics', 'QTY', 
            'Weight_LBS', 'Dimensions_Inches', 'Remark by Robert', 'Remark by Logistic team',
            'comp', 'book', 'Purchase Cost', 'Version Sheet.Destination Point', 
            'Tracking Courier Details.Tracking Destination', 'Tracking Courier Details.Courier  API List'
        ]
        df = df.reindex(columns=ordered_columns, fill_value=None)
        print(f"Fetched {len(df)} rows from Zoho.")
        return df
    else:
        print(f"Failed to fetch data from Zoho. Status code: {response.status_code}")
        return pd.DataFrame()

# Step 2: Fetch data from Google Sheets
def fetch_data_from_gsheets(sheet_name, worksheet_name, json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)
    print(f"Fetched {len(df)} rows from Google Sheets.")
    return df

# Step 3: Identify missing rows in Google Sheets
def identify_missing_rows(zoho_df, gsheets_df):
    print("Zoho columns:", zoho_df.columns.tolist())
    print("Google Sheets columns:", gsheets_df.columns.tolist())

    common_columns = list(set(zoho_df.columns) & set(gsheets_df.columns))
    print("Common columns:", common_columns)

    zoho_df = zoho_df[common_columns]
    gsheets_df = gsheets_df[common_columns]
    
    if 'Parent ID' not in common_columns:
        print("Error: 'Parent ID' column is missing from one or both datasets.")
        return pd.DataFrame()

    gsheets_df.loc[:, 'Parent ID'] = gsheets_df['Parent ID'].astype(str).str.strip()
    zoho_df.loc[:, 'Parent ID'] = zoho_df['Parent ID'].astype(str).str.strip()
    
    merged_df = zoho_df.merge(gsheets_df, on='Parent ID', how='left', indicator=True, suffixes=('_zoho', '_gsheets'))
    
    missing_rows = merged_df[merged_df['_merge'] == 'left_only'].copy()

    zoho_columns = [col for col in missing_rows.columns if not col.endswith('_gsheets') and col != '_merge']
    missing_rows = missing_rows[zoho_columns]

    missing_rows.columns = [col[:-5] if col.endswith('_zoho') else col for col in missing_rows.columns]

    print(f"Identified {len(missing_rows)} missing rows.")
    return missing_rows

# Step 4: Get delivery dates for missing rows (with multithreading)
def get_delivery_date_for_row(row, fedex_access_token, ups_access_token):
    tracking_numbers_str = row.get('Tracking Number')
    if not tracking_numbers_str:
        return row

    tracking_numbers = tracking_numbers_str.split(';')
    dates = []
    
    for tracking_number in tracking_numbers:
        if row['Tracking Courier Details.Courier  API List'] == 'FEDEX':
            delivery_info = fetch_fedex_delivery_date(tracking_number, fedex_access_token)
        elif row['Tracking Courier Details.Courier  API List'] == 'UPS':
            delivery_info = fetch_ups_delivery_date(tracking_number, ups_access_token)
        else:
            delivery_info = None

        if delivery_info and delivery_info not in ["Unknown", "Error"]:
            dates.append(str(delivery_info))
    
    if dates:
        cleaned_dates = convert_dates(' | '.join(dates))
        row['Delivery Date'] = cleaned_dates

    return row

def get_delivery_dates_threaded(df, client_id, client_secret, ups_client_key, ups_client_secret):
    fedex_access_token = get_fedex_access_token(client_id, client_secret)
    ups_access_token = get_ups_access_token(ups_client_key, ups_client_secret)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(get_delivery_date_for_row, row, fedex_access_token, ups_access_token): idx 
                         for idx, row in df.iterrows()}
        
        for future in concurrent.futures.as_completed(future_to_row):
            idx = future_to_row[future]
            try:
                df.iloc[idx] = future.result()
            except Exception as exc:
                print(f'Row {idx} generated an exception: {exc}')

    return df

def convert_dates(date_str):
    if pd.isna(date_str) or date_str == '':
        return date_str
    
    date_count_pairs = date_str.split(' | ')
    formatted_pairs = []
    
    for pair in date_count_pairs:
        match = re.match(r'(.*)\s*\[(\d+)\]', pair.strip())
        if match:
            date, count = match.groups()
            try:
                # Remove the time component if it exists and format the date
                date_only = re.sub(r'T.*', '', date)
                formatted_date = pd.to_datetime(date_only, errors='raise').date().strftime('%Y-%m-%d')
                formatted_pairs.append(f"{formatted_date} [{count}]")
            except ValueError:
                # If date conversion fails but count is not zero, keep the count
                if count != '0':
                    formatted_pairs.append(f"[{count}]")
        else:
            try:
                # Handle dates without brackets
                date_only = re.sub(r'T.*', '', pair.strip())
                formatted_date = pd.to_datetime(date_only, errors='raise').date().strftime('%Y-%m-%d')
                formatted_pairs.append(formatted_date)
            except ValueError:
                # If the date is invalid, keep the original string
                formatted_pairs.append(pair.strip())
    
    return ' | '.join(filter(None, formatted_pairs))

# Step 5: Clear and append data to Google Sheets
def clear_and_append_to_gsheets(sheet_name, worksheet_name, df, json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)

    worksheet.clear()

    data = [df.columns.values.tolist()] + df.values.tolist()

    worksheet.append_rows(data, value_input_option='RAW')
    print(f"Replaced all rows in Google Sheets with {len(df)} rows.")

# Step 6: Sort and append data to Google Sheets
# ... (previous code remains the same)

# Step 6: Sort and append data to Google Sheets (Updated)
def sort_and_append_to_gsheets(gsheets_df, missing_rows_df, sheet_name, worksheet_name, json_credentials_path):
    combined_df = pd.concat([gsheets_df, missing_rows_df], ignore_index=True)

    combined_df['Date tracking Enter'] = pd.to_datetime(combined_df['Date tracking Enter'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    combined_df = combined_df.dropna(subset=['Date tracking Enter'])

    # Sort the DataFrame by 'Date tracking Enter' in ascending order
    df_sorted = combined_df.sort_values(by='Date tracking Enter', ascending=True)

    # Extract the earliest instance of each unique 'Invoice'
    earliest_invoices = df_sorted.drop_duplicates(subset='Invoice', keep='first')

    # Extract the remaining rows with 'Invoice' values
    remaining_invoices = df_sorted[df_sorted.duplicated(subset='Invoice', keep='first')]

    # Create an empty DataFrame to store the final sorted result
    df_final = pd.DataFrame(columns=combined_df.columns)

    # Append remaining invoices just below their respective earliest invoices
    for invoice in earliest_invoices['Invoice']:
        # Append the earliest invoice row
        df_final = pd.concat([df_final, earliest_invoices[earliest_invoices['Invoice'] == invoice]])
        # Append the remaining invoice rows
        df_final = pd.concat([df_final, remaining_invoices[remaining_invoices['Invoice'] == invoice]])

    # Append rows without 'Invoice' values (if needed)
    df_no_invoices = df_sorted[df_sorted['Invoice'].isna()]
    df_final = pd.concat([df_final, df_no_invoices])

    # Reset index for the final DataFrame
    df_final = df_final.reset_index(drop=True)

    new_order = [
        'Parent ID', 'SNo', 'Destination Point', "Batch", "Invoice",
        'Version Sheet.Order Payment Received Status', 'Stage_new', 'Date tracking Enter',
        'Supplier Name', 'Supplier (Grainger / Non-Grainger)', 'Version Sheet.Placed the Order with Supplier',
        'Tracking Number', 'Delivery Date', 'Pic', 'Item Pics', 'QTY',
        'Weight_LBS', 'Dimensions_Inches', 'Remark by Robert', 'Remark by Logistic team',
        "comp", "book", 'Purchase Cost', 'Version Sheet.Destination Point',
        'Tracking Courier Details.Tracking Destination', 'Tracking Courier Details.Courier  API List'
    ]
    df_final = df_final[new_order]

    df_final['Date tracking Enter'] = df_final['Date tracking Enter'].astype(str)

    clear_and_append_to_gsheets(sheet_name, worksheet_name, df_final, json_credentials_path)

# Main script execution
if __name__ == "__main__":
    # Fetch data from Zoho
    zoho_df = fetch_data_from_zoho()

    # Fetch data from Google Sheets
    gsheets_df = fetch_data_from_gsheets('Tracking Sheet for Charlotte.xlsx', 'Test_new', 'divine-arcade-406611-e0729e40870d.json')

    # Identify missing rows 
    missing_rows_df = identify_missing_rows(zoho_df, gsheets_df)

    if not missing_rows_df.empty:
        # Get delivery dates for missing rows using multithreading
        missing_rows_df = get_delivery_dates_threaded(missing_rows_df, CLIENT_ID, CLIENT_SECRET, client_key, client_secret)

        # Convert and clean the 'Delivery Date' column in the missing_rows_df DataFrame
        missing_rows_df['Delivery Date'] = missing_rows_df['Delivery Date'].apply(convert_dates)

        # Sort, combine, and replace all rows in Google Sheets
        sort_and_append_to_gsheets(gsheets_df, missing_rows_df, 'Tracking Sheet for Charlotte.xlsx', 'Test_new', 'divine-arcade-406611-e0729e40870d.json')

        print("Replaced all rows in Google Sheets with sorted and updated data.")
    else:
        print("No missing rows to append.")