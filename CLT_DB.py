import requests # type: ignore
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import gspread # type: ignore
from oauth2client.service_account import ServiceAccountCredentials # type: ignore
import time
import json
import time
from gspread_formatting import format_cell_range, CellFormat, Color # type: ignore
with open('credentials.json') as f:
    credentials = json.load(f)

# Extract credentials
fedex_params = credentials.get('fedex_params')
ups_params = credentials.get('ups_params')
params = credentials.get('zoho_params')

# FedEx API Credentials
CLIENT_ID = fedex_params.get('CLIENT_ID')
CLIENT_SECRET = fedex_params.get('CLIENT_SECRET')

# UPS API Credentials
client_key = ups_params.get('client_key')
client_secret = ups_params.get('client_secret')

# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets(json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)
    return client

# Function to fetch all data from the specified worksheet
def fetch_all_data_from_google_sheets(sheet_name, worksheet_name, json_credentials_path):
    try:
        client = authenticate_google_sheets(json_credentials_path)
        print("Authenticated successfully")

        # Open the existing Google Sheet by name
        sheet = client.open(sheet_name)
        print(f"Opened Google Sheet: {sheet_name}")

        worksheet = sheet.worksheet(worksheet_name)
        print(f"Worksheet {worksheet_name} exists.")

        # Fetch all data from the worksheet
        data = worksheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        print(f"Fetched {len(df)} rows from the worksheet.")
        return df, worksheet, sheet

    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet {worksheet_name} does not exist.")
        return None, None, None
    except gspread.exceptions.APIError as api_error:
        print(f"Google Sheets API error: {api_error}")
        return None, None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None, None

# Function to fetch and process data from Zoho Analytics
def fetch_and_process_data_from_zoho():
    token_url = "https://accounts.zoho.com/oauth/v2/token"
    

    response = requests.post(token_url, params=params)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print("Token obtained successfully")
    else:
        print(f"Failed to obtain token. Status code: {response.status_code}")
        return None

    url = "https://analyticsapi.zoho.com/api/ashutosh@raptorsupplies.com/Zoho%20CRM%20Analytics/Logistic 2?ZOHO_ACTION=EXPORT&ZOHO_OUTPUT_FORMAT=XML&ZOHO_ERROR_FORMAT=XML&ZOHO_API_VERSION=1.0"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

    # Parse XML data
    root = ET.fromstring(response.content)
    rows_data = []
    for row in root.findall('.//row'):
        row_data = {col.attrib.get('name'): col.text for col in row}
        rows_data.append(row_data)

    df_final = pd.DataFrame(rows_data)
    df_final['Stage_new'] = df_final['Version Sheet.Stage'] + '--' + df_final['Version Sheet.Payment Method']
    df_final['Destination Point']=df_final['Version Sheet.Destination Point']+'-'+df_final['EUC Upload']
    # Keep only the necessary columns
    # df_final = df_final[['Parent ID', 'Stage']]
    return df_final

# Function to update the Google Sheets worksheet with the modified DataFrame
def update_sheet_with_modified_data(df, df_final):
    # Create a mapping of 'Parent ID' to the index in df_final
    parent_id_to_index_final = {id_: index for index, id_ in enumerate(df_final['Parent ID'])}

    # Iterate over df and update the 'Stage' if Parent ID exists in df_final
    for index, row in df.iterrows():
        parent_id = row['Parent ID']
        if parent_id in parent_id_to_index_final:
            df_final_index = parent_id_to_index_final[parent_id]
            # df.at[index, 'Stage'] = df_final.iloc[df_final_index]['Stage']
            df.at[index, 'Stage_new'] = df_final.iloc[df_final_index]['Stage_new']
            df.at[index, 'ECCN'] = df_final.iloc[df_final_index]['ECCN']

            df.at[index, 'Tracking Number'] = df_final.iloc[df_final_index]['Tracking Number']
            df.at[index, 'Destination Point'] = df_final.iloc[df_final_index]['Destination Point']
            df.at[index, 'Version Sheet.Order Payment Received Status'] = df_final.iloc[df_final_index]['Version Sheet.Order Payment Received Status']


    return df

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

# Function to append data containing 'Track 2' or 'Track 3' in 'Stage' to a new sheet and delete from original sheet


def fetch_worksheet_to_dataframe(sheet_name, worksheet_name, json_credentials_path):
    try:
        # Authenticate and get the Google Sheets client
        client = authenticate_google_sheets(json_credentials_path)
        print("Authenticated successfully")

        # Open the existing Google Sheet by name
        sheet = client.open(sheet_name)
        print(f"Opened Google Sheet: {sheet_name}")

        # Get the worksheet by name
        worksheet = sheet.worksheet(worksheet_name)
        print(f"Accessing worksheet: {worksheet_name}")

        # Fetch all data from the worksheet
        data = worksheet.get_all_values()
        print(f"Fetched data from worksheet: {data[:5]}")  # Print first 5 rows for debugging

        # Check if data is not empty
        if not data:
            print("No data found in the worksheet")
            return None, None, None

        # Convert the data to a DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])
        print(f"Converted data to DataFrame: {df.head()}")

        return df, worksheet, sheet

    except gspread.exceptions.APIError as api_error:
        print(f"Google Sheets API error: {api_error}")
        return None, None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None, None

# Function to append data containing 'Track 2' or 'Track 3' in 'Stage_new' to a new sheet and delete from original sheet
# Function to append data containing 'Track 2' or 'Track 3' in 'Stage_new' to a new sheet and delete from the original sheet
# Function to append data containing 'Track 2' or 'Track 3' in 'Stage_new' to a new sheet and delete from the original sheet
def append_and_delete_track_data(df, worksheet, sheet, new_sheet_name):
    try:
        # Filter rows containing 'Track 2' or 'Track 3' in 'Stage_new'
        filtered_df = df[df['Stage_new'].str.contains('TRACK 2|TRACK 3|CLOSED', na=False)]
        print(f"Filtered data: {filtered_df.head()}")

        if filtered_df.empty:
            print("No matching data found to append.")
            return

        # Convert the filtered DataFrame to a list of lists (to match the column count)
        filtered_data = filtered_df.values.tolist()

        # Check if the new sheet already exists
        try:
            new_sheet = sheet.worksheet(new_sheet_name)
            print(f"Accessing existing worksheet: {new_sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            new_sheet = sheet.add_worksheet(title=new_sheet_name, rows="1000", cols="20")
            print(f"Created new worksheet: {new_sheet_name}")

        # Find the first empty row in the new sheet (using only columns with data)
        existing_data = new_sheet.get_all_values()
        if not existing_data:  # If the sheet is empty, start from the first row
            start_row = 1
        else:
            start_row = len(existing_data) + 1

        # Find the starting column (if required)
        num_existing_columns = len(existing_data[0]) if existing_data else len(filtered_df.columns)
        
        # Pad each row with empty columns if necessary to ensure alignment
        for row in filtered_data:
            while len(row) < num_existing_columns:
                row.append('')

        # Append the filtered data to the new sheet
        new_sheet.append_rows(filtered_data, value_input_option='RAW', table_range=f"A{start_row}")
        print("Data appended successfully")

        # Format the appended rows with a blue background
        format_range = f'A{start_row}:U{start_row + len(filtered_data) - 1}'
        fmt = CellFormat(
            backgroundColor=Color(0.678, 0.847, 0.902)  # Light blue color
        )
        format_cell_range(new_sheet, format_range, fmt)

        # Delete rows from the original sheet
        indices_to_delete = filtered_df.index + 2  # +2 to account for header and 0-based index
        for index in sorted(indices_to_delete, reverse=True):
            worksheet.delete_rows(index)
            time.sleep(1)  # Delay to reduce the frequency of API calls
        print("Rows deleted from the original sheet")

    except Exception as e:
        print(f"An unexpected error occurred while appending and deleting data: {e}")


# Example usage
if __name__ == "__main__":
    # Path to the JSON credentials file
    json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'

    # Google Sheet and worksheet names
    sheet_name = 'Tracking Sheet for Charlotte.xlsx'
    worksheet_name = 'Test_new'
    new_sheet_name = 'Test_DB'

    # Fetch data from Google Sheets
    df_google_sheets, worksheet, sheet = fetch_all_data_from_google_sheets(sheet_name, worksheet_name, json_credentials_path)
    if df_google_sheets is not None:
        print("Fetched Google Sheets data")

    # Fetch processed data from Zoho Analytics
    df_final = fetch_and_process_data_from_zoho()
    if df_final is not None:
        print("Fetched Zoho Analytics data")

    # Update Google Sheets data with the values from Zoho Analytics
    updated_df = update_sheet_with_modified_data(df_google_sheets, df_final)

    # Upload the updated DataFrame back to Google Sheets
    create_or_update_worksheet(updated_df, sheet_name, worksheet_name, json_credentials_path)

    # Append and delete filtered data
    append_and_delete_track_data(updated_df, worksheet, sheet, new_sheet_name)

    print("Process completed")
