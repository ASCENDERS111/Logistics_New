import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Function to authenticate and get the Google Sheets client
def authenticate_google_sheets(json_credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_path, scope)
    client = gspread.authorize(creds)
    return client

# Function to fetch all data from the specified worksheet
def fetch_all_data(sheet_name, worksheet_name, json_credentials_path):
    try:
        # Authenticate and get the Google Sheets client
        client = authenticate_google_sheets(json_credentials_path)
        print("Authenticated successfully")

        # Open the existing Google Sheet by name
        sheet = client.open(sheet_name)
        print(f"Opened Google Sheet: {sheet_name}")

        # Check if the worksheet exists
        try:
            worksheet = sheet.worksheet(worksheet_name)
            print(f"Worksheet {worksheet_name} exists.")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet {worksheet_name} does not exist.")
            return None

        # Fetch all data from the worksheet
        data = worksheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        print(f"Fetched {len(df)} rows from the worksheet.")
        return df

    except gspread.exceptions.APIError as api_error:
        print(f"Google Sheets API error: {api_error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Path to the JSON credentials file
    json_credentials_path = 'divine-arcade-406611-e0729e40870d.json'

    # Name of the existing Google Sheet
    sheet_name = 'Tracking Sheet for Charlotte.xlsx'

    # Name of the worksheet
    worksheet_name = 'Automation_New'

    # Fetch all data from the worksheet
    df = fetch_all_data(sheet_name, worksheet_name, json_credentials_path)

    if df is not None:
        print(df)
        print(df.info())

import pandas as pd
from datetime import datetime

def Date_time(df):
    today = datetime.today()  # Current date as datetime object
    
    # Find rows where 'Date_of_Dims' is empty and 'Dimensions_Inches' is not empty
    mask = (df['Date_of_Dims'] == '') & (df['Dimensions_Inches'] != '')
    
    # Replace empty 'Date_of_Dims' with today's date where 'Dimensions_Inches' is not empty
    df.loc[mask, 'Date_of_Dims'] = today.strftime('%Y-%m-%d')
    
    # Convert 'Date_of_Dims' to datetime
    df['Date_of_Dims'] = pd.to_datetime(df['Date_of_Dims'], errors='coerce')
    
    # Calculate the difference in days between today and 'Date_of_Dims'
    df['comp'] = (today - df['Date_of_Dims']).dt.days
    
    return df

# Apply the function to your DataFrame
df = Date_time(df)

# Show value counts for 'comp' column
print(df['Date_of_Dims'].value_counts())
comp_counts = df['comp'].value_counts()
print("\nValue counts for 'comp':\n", comp_counts)

