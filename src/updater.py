#!/usr/bin/env python

import gsheet
import kucoin_query as kcq
import pandas as pd
import numpy as np

def insertRows(numRows):
    sheetIds = gsheet.get_sheet_dict()

    body = {
        'requests': [
            {
                'insertDimension': {
                    'range': {
                        'sheetId': sheetIds['Spot Trades'],
                        'dimension': 'ROWS',
                        'startIndex': 2,
                        'endIndex': 2 + numRows
                    },
                    'inheritFromBefore': False
                }
            },
            {
                'copyPaste': {
                    'source': {
                        'sheetId': sheetIds['Spot Trades'],
                        'startRowIndex': 1,
                        'endRowIndex': 2,
                        'startColumnIndex': 0,
                        'endColumnIndex': 9
                    },
                    'destination': {
                        'sheetId': sheetIds['Spot Trades'],
                        'startRowIndex': 2,
                        'endRowIndex': 2 + numRows,
                        'startColumnIndex': 0,
                        'endColumnIndex': 9
                    },
                    'pasteType': 'PASTE_NO_BORDERS',
                    'pasteOrientation': 'NORMAL'
                }
            }
        ]
    }
    gsheet.service.batchUpdate(spreadsheetId=gsheet.SPREADSHEET_ID, body=body).execute()
    
def writeTable(df, numClobber):
    numRows = len(df)
    insertRows(numRows - numClobber)
    gsheet.update('Spot Trades', f'A3:H{3+numRows-1}', df)
    
def read_gsheet():
    gsheet_df = gsheet.query('Spot Trades', 'A:H', header_row=True)
    trades_df = gsheet_df.iloc[1:].copy()

    # Convert to numeric (removing dollar sign)
    for col in ['Investment', 'PNL']:
        trades_df[col] = pd.to_numeric(trades_df[col].str.replace('$','',regex=False), errors='coerce')

    # Convert to date/time
    for col in ['Open Time', 'Close Time']:
        trades_df[col] = pd.to_datetime(trades_df[col], errors='coerce', utc=True)
        
    return trades_df

def read_kucoin(start_time, latest_close_time, template_df):
    kucoin_trades_df = kcq.get_trades(start_time)
    kucoin_trades_df.reset_index(level=0, inplace=True)
    new_kucoin_trades_df = kucoin_trades_df[~(kucoin_trades_df['Close Time'] <= latest_close_time)]

    new_trades_df = pd.DataFrame().reindex_like(template_df)
    combined_trades_df = pd.concat([new_trades_df, new_kucoin_trades_df[::-1]])
    combined_trades_df.dropna(how='all', inplace=True)
    combined_trades_df['Coin'] = combined_trades_df['targetCurrency'] 
    new_trades_df = combined_trades_df.drop(columns=set(combined_trades_df.columns)-set(template_df.columns)).reset_index(drop=True)

    str_trades_df = new_trades_df.copy()
    str_trades_df['Open Time'] = new_trades_df['Open Time'].fillna(pd.to_datetime('today',utc=True)).dt.strftime('%m/%d/%Y %H:%M:%S.%f')
    str_trades_df['Close Time'] = new_trades_df['Close Time'].fillna(pd.to_datetime('today',utc=True)).dt.strftime('%m/%d/%Y %H:%M:%S.%f')
    str_trades_df = str_trades_df.astype(str).mask(new_trades_df.isnull(), np.NaN)
    str_trades_df['Close Time'].fillna('', inplace=True)
    
    return str_trades_df

def update():
    all_trades_df = read_gsheet()
    
    open_trades_df = all_trades_df[pd.isna(all_trades_df['Close Time'])]

    first_open_time = open_trades_df['Open Time'].min()
    latest_close_time = all_trades_df['Close Time'].max()

    if pd.isna(first_open_time):
        start_time = latest_close_time
    elif pd.isna(latest_close_time):
        start_time = first_open_time
    else:
        start_time = min(first_open_time, latest_close_time)
        
    write_trades_df = read_kucoin(start_time, latest_close_time, all_trades_df)

    writeTable(write_trades_df, numClobber=len(open_trades_df))
    
if __name__ == '__main__':
    update()