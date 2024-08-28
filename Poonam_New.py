import requests
import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import os
import numpy as np
import json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

# Existing functions from delivery_date_fetcher.py
def get_access_token(client_id, client_secret):
    url = "https://apis.fedex.com/oauth/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        token_info = response.json()
        access_token = token_info['access_token']
        return access_token
    else:
        print(f"Failed to get access token: {response.status_code}")
        print(response.text)
        return None

def track_shipment(tracking_number, access_token):
    url = "https://apis.fedex.com/track/v1/trackingnumbers"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    data = {
        "trackingInfo": [
            {
                "trackingNumberInfo": {
                    "trackingNumber": tracking_number
                }
            }
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        tracking_info = response.json()
        try:
            complete_track_results = tracking_info['output']['completeTrackResults'][0]['trackResults'][0]
            estimated_delivery = complete_track_results.get('estimatedDeliveryTimeWindow', {}).get('window', {}).get('ends')
            if not estimated_delivery:
                for date_time in complete_track_results.get('dateAndTimes', []):
                    if date_time.get('type') == 'ESTIMATED_DELIVERY':
                        estimated_delivery = date_time.get('dateTime')
                        break
            if not estimated_delivery:
                estimated_delivery = complete_track_results.get('standardTransitTimeWindow', {}).get('window', {}).get('ends')
            package_count = complete_track_results.get('packageDetails', {}).get('count', '1')
            if estimated_delivery:
                return f"{estimated_delivery} [{package_count}]"
            else:
                return None
        except KeyError as e:
            print(f"Unexpected response structure or missing key: {e}")
            print(json.dumps(tracking_info, indent=2))
            return None
    else:
        print(f"Failed to track shipment: {response.status_code}")
        print(response.text)
        return None

def process_tracking_numbers(new_rows_df, access_token):
    delivery_dates = []
    for index, row in new_rows_df.iterrows():
        tracking_number = row['Tracking Number']
        if ';' in tracking_number:
            tracking_numbers = tracking_number.split(';')
            dates = []
            for tn in tracking_numbers:
                try:
                    tn = tn.strip()
                    delivery_info = track_shipment(tn, access_token)
                    dates.append(delivery_info if delivery_info else "No data")
                except Exception as e:
                    dates.append(f"Error: {str(e)}")
            delivery_dates.append(';'.join(dates))
        else:
            try:
                delivery_info = track_shipment(tracking_number.strip(), access_token)
                delivery_dates.append(delivery_info if delivery_info else "No data")
            except Exception as e:
                delivery_dates.append(f"Error: {str(e)}")
    new_rows_df['Delivery Date'] = delivery_dates

# Main script from Poonam.py with delivery date fetching integrated
def main():
    # OAuth token request
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = {
        "client_id": "1000.8KP5FVR2CASRD70V5D6EN3EHNZXC0W",
        "client_secret": "2e76b56ef1a4ff32d4ae87f35024be17da3e641f9e",
        "grant_type": "client_credentials",
        "scope": "ZohoAnalytics.data.read",
        "soid": "ZohoCRM.696664214"
    }

    response = requests.post(url, params=params)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print("Token obtained successfully")
    else:
        print(f"Failed to obtain token. Status code: {response.status_code}")
        exit()

    # Make the API request
    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Poonam?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }
    response = requests.get(url, headers=headers)

    # Process XML response and convert to DataFrame
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rows_data = []
        for row in root.findall('.//row'):
            row_data = {column.attrib.get('name'): column.text for column in row}
            rows_data.append(row_data)
        df = pd.DataFrame(rows_data)
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        exit()

    # Preprocess DataFrame (your existing logic)
    df.loc[df['Stage'].isin(['TRACK 2', 'TRACK 3']), 'Supplier Name'] = 'CLT'
    df = df[df['Stage'] != 'ORDER CLOSED']
    df[['Poonam QTY',  'Weight', 'Dimensions', 'Remark by Poonam', 'Remark by Logistic team', 'Delivery Date','book','box']] = ''
    cols = list(df.columns)
    tracking_index = cols.index('Tracking Number')
    cols.insert(tracking_index + 1, cols.pop(cols.index('Delivery Date')))
    df = df[cols]
    df['Urgent'] = df['Urgent'].map({'Urgent Shipment - Logistics':'Urgent','Urgent Shipment - OP':'Urgent'})

    # Sort by 'Date tracking Enter'
    df['Date tracking Enter'] = pd.to_datetime(df['Date tracking Enter'], format='%d/%m/%Y %H:%M:%S')
    df_sorted = df.sort_values(by='Date tracking Enter', ascending=True)

    # Extract the earliest instance of each unique 'Invoice'
    earliest_invoices = df_sorted.drop_duplicates(subset='Invoice', keep='first')
    remaining_invoices = df_sorted[df_sorted.duplicated(subset='Invoice', keep='first')]
    df_final = pd.DataFrame(columns=df.columns)
    for invoice in earliest_invoices['Invoice']:
        df_final = pd.concat([df_final, earliest_invoices[earliest_invoices['Invoice'] == invoice]])
        df_final = pd.concat([df_final, remaining_invoices[remaining_invoices['Invoice'] == invoice]])
    df_no_invoices = df_sorted[df_sorted['Invoice'].isna()]
    df_final = pd.concat([df_final, df_no_invoices])
    df_final = df_final.reset_index(drop=True)

    # Define new column order
    new_order = [
        'Parent ID', 'Urgent', 'Batch Number', 'Destination Point', 'Stage', 
        'Date tracking Enter', "Invoice", 'Supplier Name', 'Tracking Number', 
        'Delivery Date', 'QTY', 'Item', 'Poonam QTY', 'Weight', 'Dimensions', 
        'Remark by Poonam', 'Remark by Logistic team', 'book', 'box', 'API List'
    ]
    df_final = df_final[new_order]

    # Fetch Delivery Dates using FedEx API
    CLIENT_ID = 'l7da4097b7bcfe4440ba7f860d7af9bf89'
    CLIENT_SECRET = 'ccba5622-b0b0-4fa8-9be7-1860b9a39381'
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET)
    if access_token:
        process_tracking_numbers(df_final, access_token)

    # Final processing and Google Sheets upload (your existing logic)
    json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'
    sheet_name = 'Poonam incoming shipments'
    worksheet_name = 'TestNew'
    create_or_update_worksheet(df_final, sheet_name, worksheet_name, json_credentials_path)
# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets(json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)
    return client

# Helper function to convert a column index to an Excel column letter
def col_idx_to_letter(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# Function to create or update a worksheet and upload DataFrame to it in chunks
def create_or_update_worksheet(df, sheet_name, worksheet_name, json_credentials_path):
    try:
        # Authenticate and get the Google Sheets client
        client = authenticate_google_sheets(json_credentials_path)
        print("Authenticated successfully")

        # Open the existing Google Sheet by name
        sheet = client.open(sheet_name)
        print(f"Opened Google Sheet: {sheet_name}")

        # Check if the worksheet already exists
        try:
            worksheet = sheet.worksheet(worksheet_name)
            print(f"Worksheet {worksheet_name} already exists, updating it.")
        except gspread.exceptions.WorksheetNotFound:
            # Add a new worksheet if it does not exist
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="10000", cols="30")
            print(f"Created new worksheet: {worksheet_name}")

        # Handle out of range float values (e.g., NaNs or infinite values)
        df = df.replace([np.inf, -np.inf], np.nan).fillna('')
        df = df.applymap(str)  # Convert all values to strings to avoid JSON serialization issues

        # Convert DataFrame to list of lists
        df_list = df.values.tolist()
        print(f"Converted DataFrame to list of lists")

        # Add header (column names)
        df_list.insert(0, df.columns.tolist())
        print(f"Inserted column headers")

        # Clear the worksheet before updating it
        worksheet.clear()
        print(f"Cleared existing worksheet data")

        # Upload data in chunks
        chunk_size = 1000  # Adjust chunk size as needed
        max_columns = len(df.columns)

        for i in range(0, len(df_list), chunk_size):
            chunk = df_list[i:i + chunk_size]
            start_row = i + 1
            end_row = start_row + len(chunk) - 1
            end_column_letter = col_idx_to_letter(max_columns)
            cell_range = f'A{start_row}:{end_column_letter}{end_row}'
            print(f"Updating worksheet with chunk starting at {cell_range}...")
            worksheet.update(cell_range, chunk)
            print(f"Updated worksheet with chunk starting at {cell_range}")

    except gspread.exceptions.APIError as api_error:
        print(f"Google Sheets API error: {api_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Function to convert and clean up delivery dates
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
                # Remove the time component if it exists
                date_only = re.sub(r'T.*', '', date)
                formatted_date = pd.to_datetime(date_only, errors='raise').date().strftime('%Y-%m-%d')
                formatted_pairs.append(f"{formatted_date} [{count}]")
            except ValueError:
                if count != '0':
                    formatted_pairs.append(f"[{count}]")  # Keeps the count if it's not zero
        else:
            # Handle dates without brackets
            try:
                date_only = re.sub(r'T.*', '', pair.strip())
                formatted_date = pd.to_datetime(date_only, errors='raise').date().strftime('%Y-%m-%d')
                formatted_pairs.append(formatted_date)
            except ValueError:
                # If the date is invalid, keep the original string
                formatted_pairs.append(pair.strip())
    return ' | '.join(filter(None, formatted_pairs))

# Final step: Apply the date conversion to the DataFrame
df_final['Delivery Date'] = df_final['Delivery Date'].apply(convert_dates)

# Example usage in the main script
if __name__ == "__main__":

    # Path to the JSON credentials file
    json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'

    # Name of the existing Google Sheet
    sheet_name = 'Poonam incoming shipments'

    # Name of the worksheet
    worksheet_name = 'TestNew'

    # Upload the DataFrame to the worksheet in the existing Google Sheet
    create_or_update_worksheet(df_final, sheet_name, worksheet_name, json_credentials_path)
    print("Process completed")
