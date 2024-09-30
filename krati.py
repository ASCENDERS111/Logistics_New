import requests # type: ignore
import pandas as pd # type: ignore
import xml.etree.ElementTree as ET
import os
import numpy as np # type: ignore
import json
from datetime import datetime
import gspread # type: ignore
from oauth2client.service_account import ServiceAccountCredentials # type: ignore
import re
from delivery_date_fetcher import get_fedex_access_token, fetch_fedex_delivery_date  # Import necessary functions
import json
import warnings

# Suppress all warnings
warnings.filterwarnings("ignore")

# Function to load credentials from a JSON file
def load_credentials(filepath='credentials.json'):
    with open(filepath, 'r') as file:
        credentials = json.load(file)
    return credentials

# Load the credentials
credentials = load_credentials()

# Extract Zoho parameters
zoho_params = credentials['zoho_params']

# Now you can use `zoho_params` directly in your requests
response = requests.post("https://accounts.zoho.com/oauth/v2/token", params=zoho_params)

if response.status_code == 200:
    access_token = response.json()['access_token']
    print("Token obtained successfully")
else:
    print(f"Failed to obtain token. Status code: {response.status_code}")
    exit()
# OAuth token request to Zoho
def get_zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    params = zoho_params
    response = requests.post(url, params=params)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print("Token obtained successfully")
        return access_token
    else:
        print(f"Failed to obtain token. Status code: {response.status_code}")
        exit()

# Fetch data from Zoho API
def fetch_zoho_data(access_token):
    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Krati_logistic?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        rows_data = []
        for row in root.findall('.//row'):
            row_data = {column.attrib.get('name'): column.text for column in row}
            rows_data.append(row_data)
        df = pd.DataFrame(rows_data)
        return df
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        exit()

# Preprocess the DataFrame
def preprocess_dataframe(df):
    df.loc[df['Stage'].isin(['TRACK 2', 'TRACK 3']), 'Supplier Name'] = 'CLT'
    df = df[df['Stage'] != 'ORDER CLOSED']
    df.loc[:, ['Krati QTY', 'Weight_Kg', 'Dimensions_CMs', 'Remark by Krati', 'Remark by Logistic team', 'Delivery Date', 'Book']] = ''
    cols = list(df.columns)
    tracking_index = cols.index('Tracking Number')
    cols.insert(tracking_index + 1, cols.pop(cols.index('Delivery Date')))
    df = df[cols]

    df['Urgent'] = df['Urgent'].map({'Urgent Shipment - Logistics':'Urgent','Urgent Shipment - OP':'Urgent'})

    df['Date tracking Enter'] = pd.to_datetime(df['Date tracking Enter'], errors='coerce')
    # df['Date tracking Enter'] = pd.to_datetime(df['Date tracking Enter'], errors='coerce', format='%Y-%m-%d %H:%M:%S')



    # Sort and extract the earliest instance of each unique 'Invoice'
    df_sorted = df.sort_values(by='Date tracking Enter', ascending=True)
    earliest_invoices = df_sorted.drop_duplicates(subset='Invoice', keep='first')
    remaining_invoices = df_sorted[df_sorted.duplicated(subset='Invoice', keep='first')]

    # Combine the earliest invoices and remaining invoices
    df_final = pd.concat([earliest_invoices, remaining_invoices], ignore_index=True)

    # Handle rows without 'Invoice' values
    df_no_invoices = df_sorted[df_sorted['Invoice'].isna()]
    df_final = pd.concat([df_final, df_no_invoices]).reset_index(drop=True)

    return df_final

# Authenticate and get Google Sheets data
def fetch_google_sheets_data(sheet_name, worksheet_name, json_credentials_path):
    client = authenticate_google_sheets(json_credentials_path)
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)
    return df

# Filter and compare Zoho and Google Sheets data
def filter_new_entries(df_zoho, df_gsheet):
    df_zoho['Parent ID'] = df_zoho['Parent ID'].astype(str).str.strip()
    df_gsheet['Parent ID'] = df_gsheet['Parent ID'].astype(str).str.strip()

    # Perform a left join using 'Parent_ID' as the key
    merged_df = df_zoho.merge(df_gsheet, on='Parent ID', how='left', indicator=True, suffixes=('', '_gsheet'))

    # Keep only those rows that are present in df_zoho but not in df_gsheet
    filtered_df = merged_df[merged_df['_merge'] == 'left_only']

    # Drop any additional columns that were created due to conflicts
    filtered_df = filtered_df[df_zoho.columns]

    return filtered_df

# Fetch delivery dates for filtered tracking numbers
def fetch_delivery_dates(df, fedex_access_token):
    delivery_dates = []
    for index, row in df.iterrows():
        tracking_number = row['Tracking Number']
        if ';' in tracking_number:
            tracking_numbers = tracking_number.split(';')
            dates = []
            for tn in tracking_numbers:
                try:
                    tn = tn.strip()
                    delivery_info = fetch_fedex_delivery_date(tn, fedex_access_token)
                    dates.append(delivery_info if delivery_info else "No data")
                except Exception as e:
                    dates.append(f"Error: {str(e)}")
            delivery_dates.append(';'.join(dates))
        else:
            try:
                delivery_info = fetch_fedex_delivery_date(tracking_number.strip(), fedex_access_token)
                delivery_dates.append(delivery_info if delivery_info else "No data")
            except Exception as e:
                delivery_dates.append(f"Error: {str(e)}")
    df['Delivery Date'] = delivery_dates

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
        df = df.astype(str)  # Convert all values to strings to avoid JSON serialization issues

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
            worksheet.update(chunk, range_name=cell_range)  # Use named argument for range_name
            print(f"Updated worksheet with chunk starting at {cell_range}")

    except gspread.exceptions.APIError as api_error:
        print(f"Google Sheets API error: {api_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Main process
if __name__ == "__main__":
    # Step 1: Get Zoho access token and fetch data
    zoho_access_token = get_zoho_access_token()
    df_zoho = fetch_zoho_data(zoho_access_token)

    # Step 2: Preprocess Zoho DataFrame
    df_zoho_final = preprocess_dataframe(df_zoho)

    # Step 3: Authenticate and fetch Google Sheets data
    json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'
    sheet_name = 'Raptor UK  shipments_ Krati'
    worksheet_name = 'Automation_New'
    df_gsheet = fetch_google_sheets_data(sheet_name, worksheet_name, json_credentials_path)

    # Step 4: Filter new entries by comparing Zoho and Google Sheets data
    filtered_df = filter_new_entries(df_zoho_final, df_gsheet)

    # Step 5: Fetch delivery dates for filtered rows
    fedex_access_token = get_fedex_access_token("l7da4097b7bcfe4440ba7f860d7af9bf89", "ccba5622-b0b0-4fa8-9be7-1860b9a39381")
    fetch_delivery_dates(filtered_df, fedex_access_token)

    # Step 6: Merge filtered data back into the Google Sheets data
    df_combined = pd.concat([df_gsheet, filtered_df], ignore_index=True)

    # Step 7: Sort the combined DataFrame
    # Convert to string first
    df_combined['Date tracking Enter'] = df_combined['Date tracking Enter'].astype(str)

    # Apply string replacements
    df_combined['Date tracking Enter'] = df_combined['Date tracking Enter'].str.replace('/', '-').str.replace(' :', ':')

    # Convert back to datetime
    df_combined['Date tracking Enter'] = pd.to_datetime(df_combined['Date tracking Enter'], format='%Y-%m-%d %H:%M:%S')

    # Step 8: Reorder the columns according to the specified order
    new_order = [
        'Parent ID',
        'Date tracking Enter',
        'Urgent',
        'Batch Number',
        'Invoice',
        'Stage',
        'Supplier Name',
        'Tracking Number',
        'Delivery Date',
        'Item',
        'Raptor QTY',
        'Krati QTY',
        'Weight_Kg',
        'Dimensions_CMs',
        'Remark by Logistic team',
        'Remark by Krati',
        'Book',
        'Tracking Courier Details.Courier  API List',

    ]
    df_combined = df_combined[new_order]

    # Step 9: Convert and clean up delivery dates
    df_combined['Delivery Date'] = df_combined['Delivery Date'].apply(convert_dates)

    # Step 10: Upload the final DataFrame to Google Sheets
    create_or_update_worksheet(df_combined, sheet_name, worksheet_name, json_credentials_path)

    print("Process completed")
