import json
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import numpy as np
import time
from gspread_formatting import *

# Helper function to convert a column index to an Excel column letter
def col_idx_to_letter(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# Function to load credentials from a JSON file
def load_credentials(json_file):
    with open(json_file, 'r') as file:
        credentials = json.load(file)
    return credentials

# Function to obtain Zoho OAuth token
def get_zoho_oauth_token(client_id, client_secret, scope, soid):
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": scope,
        "soid": soid
    }
    response = requests.post(url, params=params)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print(f"Failed to obtain token. Status code: {response.status_code}")
        return None

# Function to fetch data from Zoho CRM Analytics
def fetch_zoho_data(access_token, api_url):
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rows_data = []
        for row in root.findall('.//row'):
            row_data = {column.attrib.get('name'): column.text for column in row}
            rows_data.append(row_data)
        return pd.DataFrame(rows_data)
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Function to clean and process the Zoho data
def process_data(df):
    df = df[~df['Tracking Courier Details.Tracking Destination'].isin(['Track 3', 'Track 2'])]
    df[['Pic', 'Item Pics', 'Weight_LBS', 'Dimensions_Inches', 'Remark by Robert', 'Remark by Logistic team', 'Delivery Date', 'book', 'comp']] = None
    df['SNo'] = df.SNo.astype(int)
    df['Purchase Cost'] = df['Purchase Cost'].astype(float)
    df['Pic'] = df['Purchase Cost'].apply(lambda x: 'Yes' if pd.isna(x) or x > 250 else 'No')
    df['Item Pics'] = df['Purchase Cost'].apply(lambda x: 'Yes' if pd.isna(x) or x > 1000 else 'No')
    df['Purchase Cost'] = df['Purchase Cost'].astype(str)
    df['Version Sheet.Urgent Shipment'] = df['Version Sheet.Urgent Shipment'].map({'Urgent Shipment - Logistics':'Urgent','Urgent Shipment - OP':'Urgent'})
    df['SNo'] = np.where(df['Version Sheet.Urgent Shipment'].fillna('') != '', df['SNo'].astype(str) + "|" + df['Version Sheet.Urgent Shipment'].astype(str), df['SNo'].astype(str))
    df['EUC Upload'] = df['EUC Upload'].map({'Yes': 'EucUploaded', 'No': ''})
    df['Destination Point'] = df['Version Sheet.Destination Point'] + '-' + df['EUC Upload']
    df.loc[df['Version Sheet.Destination Point'].str.contains('EXW', na=False), 'Pic'] = 'Yes'
    df.loc[df['Supplier (Grainger / Non-Grainger)'].str.contains('ebay', case=False, na=False), 'Item Pics'] = 'Yes'
    df['Stage_new'] = df['Version Sheet.Stage'] + '--' + df['Version Sheet.Payment Method'].fillna('')
    df['Date tracking Enter'] = pd.to_datetime(df['Date tracking Enter'], format='%d/%m/%Y %H:%M:%S')

    df_sorted = df.sort_values(by='Date tracking Enter', ascending=True)
    earliest_invoices = df_sorted.drop_duplicates(subset='Invoice', keep='first')
    remaining_invoices = df_sorted[df_sorted.duplicated(subset='Invoice', keep='first')]

    df_final = pd.DataFrame(columns=df.columns)
    for invoice in earliest_invoices['Invoice']:
        df_final = pd.concat([df_final, earliest_invoices[earliest_invoices['Invoice'] == invoice]], ignore_index=True)
        df_final = pd.concat([df_final, remaining_invoices[remaining_invoices['Invoice'] == invoice]], ignore_index=True)

    df_no_invoices = df_sorted[df_sorted['Invoice'].isna()]
    df_final = pd.concat([df_final, df_no_invoices], ignore_index=True)
    df_final = df_final.reset_index(drop=True)

    df_final = df_final.drop(['Version Sheet.Stage', 'Version Sheet.Urgent Shipment', 'EUC Upload'], axis=1)
    new_order = [
        'Parent ID', 'SNo', 'Destination Point', 'Batch', 'Invoice', 'Version Sheet.Order Payment Received Status', 
        'Stage_new', 'Date tracking Enter', 'Supplier Name', 'Supplier (Grainger / Non-Grainger)', 
        'Version Sheet.Placed the Order with Supplier', 'Tracking Number', 'Delivery Date', 'Pic', 'Item Pics', 
        'QTY', 'Weight_LBS', 'Dimensions_Inches', 'Remark by Robert', 'Remark by Logistic team', 
        'comp', 'book', 'Purchase Cost', 'Tracking Courier Details.Tracking Destination', 'Tracking Courier Details.Courier  API List'
    ]
    return df_final[new_order]

# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets(json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)
    return client

# Function to upload DataFrame to Google Sheets
def upload_to_google_sheets(df, sheet_name, worksheet_name, json_credentials_path):
    try:
        client = authenticate_google_sheets(json_credentials_path)
        sheet = client.open(sheet_name)
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="2000", cols="30")

        # Ensure the DataFrame doesn't have more rows than the worksheet's max row limit
        max_rows = 2000
        if len(df) > max_rows:
            df = df.iloc[:max_rows]  # Truncate to max rows

        # Convert all datetime columns to string format in the DataFrame before uploading to Google Sheets
        df = df.map(lambda x: str(x) if isinstance(x, pd.Timestamp) else x)

        df = df.replace([np.inf, -np.inf], np.nan).fillna('')
        df = df.map(str)
        df_list = df.values.tolist()
        df_list.insert(0, df.columns.tolist())
        worksheet.clear()
        
        chunk_size = 1000
        for i in range(0, len(df_list), chunk_size):
            chunk = df_list[i:i + chunk_size]
            start_row = i + 1
            end_column_letter = col_idx_to_letter(len(df.columns))
            cell_range = f'A{start_row}:{end_column_letter}{start_row + len(chunk) - 1}'
            worksheet.update(chunk, cell_range)  # Fixed argument order
    except Exception as e:
        print(f"An error occurred: {e}")

# Missing append_and_delete_track_data function
def append_and_delete_track_data(df, worksheet, sheet, new_sheet_name):
    filtered_df = df[df['Stage_new'].str.contains('TRACK 2|TRACK 3|CLOSED', na=False)]
    if filtered_df.empty:
        print("No matching data found to append.")
        return

    filtered_data = [filtered_df.columns.values.tolist()] + filtered_df.values.tolist()
    try:
        new_sheet = sheet.worksheet(new_sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        new_sheet = sheet.add_worksheet(title=new_sheet_name, rows="1000", cols="20")

    existing_data = new_sheet.get_all_values()
    start_row = len(existing_data) + 1
    new_sheet.append_rows(filtered_data[1:], value_input_option='RAW')

    # Delete rows from the original sheet
    indices_to_delete = filtered_df.index + 2  # Adjust for header
    for index in sorted(indices_to_delete, reverse=True):
        worksheet.delete_rows(index)
        time.sleep(1)

# Main function
def main():
    # Load credentials from JSON file
    credentials = load_credentials('credentials.json')
    
    zoho_params = credentials['zoho_params']

    # Google Sheets credentials
    json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'
    sheet_name = 'Tracking Sheet for Charlotte.xlsx'
    worksheet_name = 'Test_new'
    new_sheet_name = 'Test_DB'

    # Zoho API URL
    zoho_api_url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Logistic%202?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"

    # Step 1: Fetch Zoho OAuth token
    access_token = get_zoho_oauth_token(
        zoho_params['client_id'], 
        zoho_params['client_secret'], 
        zoho_params['scope'], 
        zoho_params['soid']
    )
    if not access_token:
        return

    # Step 2: Fetch Zoho data
    df = fetch_zoho_data(access_token, zoho_api_url)
    if df is None:
        return

    # Step 3: Process data
    df_processed = process_data(df)

    # Step 4: Upload processed data to Google Sheets
    upload_to_google_sheets(df_processed, sheet_name, worksheet_name, json_credentials_path)

    # Step 5: Append and delete rows containing 'Track 2' or 'Track 3' in 'Stage_new'
    client = authenticate_google_sheets(json_credentials_path)
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)

    append_and_delete_track_data(df_processed, worksheet, sheet, new_sheet_name)

# Call the main function
if __name__ == "__main__":
    main()
