# kucoin-watcher
Regularly check personal KuCoin account for complete trades to track into Google Sheets.

# Configuration

## KuCoin Credentials
- Create a read-only API on KuCoin and note down its API Key, API Secret and API Passphrase. 
- Input those into the template file `keys/kucoin-keys-template.json`
- Rename `kucoin-keys-template.json` to `kucoin-keys.json`.

## Google Credentials
Refer to the [Google documentation](https://developers.google.com/sheets/api/quickstart/python) to create a Google API Project and download a credentials file.
- Save this file as `keys/google-credentials.json`.

## Google Spreadsheet

*I have the spreadsheet ID hardcoded for now, but it's easy to change:*

- In `src/gsheet.py`: Change `SPREADSHEET_ID = '...'` to your own spreadsheet ID

You can find this ID in the URL in Google Sheets. For example: ![Spreadsheet ID in URL](https://miro.medium.com/max/1400/1*xTDG-icHB7rnZ2fB0-43zQ.png)

This spreadsheet also needs to have a tab with the name "Spot Trades" with a header of any number of columns.
The columns must include at least the following:
- **Coin**: The traded currency
- **Investment**: Time-averaged investment amount in USD
- **PNL**: The gain/loss in USD (unrealized if the trade is not yet closed)
- **Open Time**: The trade open time
- **Close Time**: The trade close time (defined as when the coin held value goes below $1 USD)

The rest of the columns can be calculations on these given columns, defined in the second row.
The second row will be left alone as a template row, or can be used as a totals row for example.

# Usage

Run `src/updater.py` to execute a single update operation.

Run `src/watcher.py [interval]` to start a loop to periodically execute the update operation.
- The interval can be in any format accepted by [pytimeparse](https://github.com/wroberts/pytimeparse). For example:
  - `src/watcher.py 15min` will update every 15 minutes.
