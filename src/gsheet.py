import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pandas as pd

# Find our keys directory
from pathlib import Path
keys_dir = Path(__file__).parent / '../keys'

def gsheet_api_check(SCOPES):
    creds = None
    if os.path.exists(keys_dir / 'google-token.pickle'):
        with open(keys_dir / 'google-token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                keys_dir / 'google-credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            
        with open(keys_dir / 'google-token.pickle', 'wb') as token:
            pickle.dump(creds, token)
            
    return creds

def get_sheets_service():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = gsheet_api_check(SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service.spreadsheets()

service = get_sheets_service()
SPREADSHEET_ID = '1gnfLTGxN6Qin2BRBg9bYv9kEiYN96yj5FBewnv0n9i0'

def query(tab, cell_range, header_row=False):
    res = service.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!{cell_range}",
        valueRenderOption='UNFORMATTED_VALUE',
        dateTimeRenderOption='FORMATTED_STRING',
        majorDimension='ROWS').execute()
    
    if 'values' not in res:
        return pd.DataFrame()
    
    df = pd.DataFrame(res['values'], dtype=str)
    if res['majorDimension'] != 'ROWS':
        df = df.T
    
    if header_row:
        df.columns = df.iloc[0]
        df = df.iloc[1:]
    return df

def update(tab, cell_range, df):
    body = {
        'range': f"'{tab}'!{cell_range}",
        'majorDimension': 'ROWS',
        'values': df.where(~pd.isna(df), None).values.tolist()
    }
    
    response = service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab}'!{cell_range}",
        valueInputOption='USER_ENTERED',
        body=body).execute()
    
    print(f"Updated {response['updatedRows']} rows over {response['updatedColumns']} columns for {response['updatedCells']} total cells.")
    
def get_sheet_dict():
    sheet_list = service.get(spreadsheetId=SPREADSHEET_ID).execute()['sheets']    
    return { sheet['properties']['title'] : sheet['properties']['sheetId']  for sheet in sheet_list }