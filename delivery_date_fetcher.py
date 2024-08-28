import requests
import base64
from datetime import datetime
import requests
import json  

# Load credentials from credentials.json
with open('credentials.json') as f:
    credentials = json.load(f)

# Extract credentials
fedex_params = credentials.get('fedex_params')
ups_params = credentials.get('ups_params')

# UPS API Credentials
client_key = ups_params.get('client_key')
client_secret = ups_params.get('client_secret')

# FedEx API Credentials
CLIENT_ID = fedex_params.get('CLIENT_ID')
CLIENT_SECRET = fedex_params.get('CLIENT_SECRET')
def get_ups_access_token(client_key, client_secret):
    token_url = "https://wwwcie.ups.com/security/v1/oauth/token"
    auth_header = base64.b64encode(f"{client_key}:{client_secret}".encode()).decode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {auth_header}"
    }
    data = {"grant_type": "client_credentials"}
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"Failed to obtain UPS access token: {response.json()}")
        return None
def get_tracking_details(access_token, inquiry_number):
    url = f"https://onlinetools.ups.com/api/track/v1/details/{inquiry_number}"
    query = {
        "locale": "en_US",
        "returnSignature": "false",
        "returnMilestones": "false",
        "returnPOD": "false"
    }
    headers = {
        "transId": "string",
        "transactionSrc": "testing",
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers, params=query)
    data = response.json()
    if response.status_code == 200:
        return data
    else:
        print(f"Failed to get tracking details for {inquiry_number}: {data}")
        return None

# Step 3: Extract delivery or estimated delivery date from tracking details
def extract_delivery_date(tracking_data):
    if tracking_data:
        try:
            package_info = tracking_data['trackResponse']['shipment'][0]['package'][0]
            
            delivery_date = package_info.get('deliveryDate')
            estimated_delivery_date = package_info.get('estimatedDeliveryDate')
            rescheduled_delivery_date = package_info.get('rescheduledDeliveryDate')
            
            def extract_date(date_field):
                if isinstance(date_field, list) and len(date_field) > 0:
                    date_field = date_field[0]
                if isinstance(date_field, dict) and 'date' in date_field:
                    date_field = date_field['date']
                if isinstance(date_field, str):
                    try:
                        return datetime.strptime(date_field, '%Y%m%d').strftime('%Y-%m-%d')
                    except ValueError:
                        try:
                            return datetime.strptime(date_field, '%Y-%m-%d').strftime('%Y-%m-%d')
                        except ValueError:
                            return None
                return None

            for date_field in [delivery_date, rescheduled_delivery_date, estimated_delivery_date]:
                formatted_date = extract_date(date_field)
                if formatted_date:
                    return formatted_date
            
            return f"Unparseable: {delivery_date or estimated_delivery_date or rescheduled_delivery_date}"
        
        except KeyError as e:
            return f"KeyError: {str(e)}"
        except Exception as e:
            return f"Error: {str(e)}"
    return "No data available"

def fetch_ups_delivery_date(tracking_number, access_token):
    url = f"https://onlinetools.ups.com/api/track/v1/details/{tracking_number}"
    headers = {
        "transId": "string",
        "transactionSrc": "testing",
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        tracking_data = response.json()
        try:
            package_info = tracking_data['trackResponse']['shipment'][0]['package'][0]
            delivery_date = package_info.get('deliveryDate')
            estimated_delivery_date = package_info.get('estimatedDeliveryDate')
            rescheduled_delivery_date = package_info.get('rescheduledDeliveryDate')

            def extract_date(date_field):
                if isinstance(date_field, list) and len(date_field) > 0:
                    date_field = date_field[0]
                if isinstance(date_field, dict) and 'date' in date_field:
                    date_field = date_field['date']
                if isinstance(date_field, str):
                    try:
                        return datetime.strptime(date_field, '%Y%m%d').strftime('%Y-%m-%d')
                    except ValueError:
                        return None
                return None

            for date_field in [delivery_date, rescheduled_delivery_date, estimated_delivery_date]:
                formatted_date = extract_date(date_field)
                if formatted_date:
                    return formatted_date
            return "Unknown"
        except KeyError as e:
            return f"KeyError: {str(e)}"
    else:
        print(f"Failed to fetch UPS tracking details: {response.status_code}")
        return "Error"

def get_fedex_access_token(client_id, client_secret):
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
        return token_info['access_token']
    else:
        print(f"Failed to get FedEx access token: {response.status_code}")
        return None

def fetch_fedex_delivery_date(tracking_number, access_token):
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
            if estimated_delivery:
                return estimated_delivery
            return "Unknown"
        except KeyError:
            return "KeyError"
    else:
        print(f"Failed to fetch FedEx tracking details: {response.status_code}")
        return "Error"
