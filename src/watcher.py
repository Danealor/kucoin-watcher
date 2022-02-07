#!/usr/bin/env python

import argparse
from pytimeparse.timeparse import timeparse
from datetime import datetime, timedelta
from time import sleep

def mainLoop(interval_sec):
    import updater

    while True:
        print(datetime.now().strftime('[%c]'))
        updater.update()
        print(f"Sleeping for {interval_sec} seconds...") 
        sleep(interval_sec)

        
def parseArgs():
    parser = argparse.ArgumentParser(description='Regularly check KuCoin for new trades to track into Google Sheets.')
    parser.add_argument('interval', type=timeparse,
                        help='interval between queries')

    args = parser.parse_args()
    return args
        
        
if __name__ == '__main__':
    args = parseArgs()
    interval_sec = args.interval
    interval_td = timedelta(seconds=interval_sec)
    
    print(f"Starting up the main loop with interval {interval_td}")
    try:
        mainLoop(interval_sec)
    except KeyboardInterrupt:
        print("Shutting down.")
