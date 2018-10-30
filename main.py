import pySmartGadget
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
from bluepy import btle

gadgets = {
    'Bad': {
        'addr': 'DC:82:71:D2:60:AF',
        'logInterval': 240000,
    },
    'Schlafzimmer': {
        'addr': 'C3:77:1E:95:8E:E3',
        'logInterval': 240000,
    },
    'Wohnzimmer': {
        'addr': 'DE:45:C3:F0:92:1A',
        'logInterval': 240000,
    },
    'Küche': {
        'addr': 'EF:CA:B0:9A:BC:B4',
        'logInterval': 240000,
    },
    'Arbeitszimmer': {
        'addr': 'F9:A2:EB:77:F1:91',
        'logInterval': 240000,
    },
}

logFileName = 'data.csv'


def storeData(gadgetName, gadgetData):
    data = {gadgetName + '_' + outerKey : innerDict for outerKey, innerDict in gadgetData.items()}
    data = pd.DataFrame(data)
    data.index.name = 'Timestamp'
    data.sort_index(inplace=True)
    data = data.round(2)
# Append the new data to the old data stored in the csv
    try:
        old_data = pd.read_csv(logFileName, index_col=0)
        for column in data.columns:
            new = data[[column]]
            if column in old_data.columns:
                ''' Column is available in old data set
                get rid of all NA values; needed if another column has newer data, in this case the current column is filled with NA at the end but with a newer timestamp '''
                old = old_data[[column]].dropna()
                # Concat/append all values with a newer timestamp to the old data
                old_data = pd.concat([old_data, new.loc[new.index.get_values() > old.index.get_values().max()]], sort=True)
            else:
                ''' Column is not available; concat all the new data to the dataframe '''
                old_data = pd.concat([old_data, new], sort=True)
            ''' "Merge" doubled timestamps into one row; due to removing the NA values timestamps can be duplicated '''
            old_data = old_data.groupby(level=0).first()        

        data = old_data.round(2)
        data.sort_index(inplace=True)
    except Exception as ex:
        print('Could not read existing logfile.')
    data.to_csv(logFileName)

def read():
    readGadgets = [] # list of all devices which have been read
      
    # Read all smartGadgets
    while len(readGadgets) < len(gadgets):
        for name, value in gadgets.items():
            if name not in readGadgets:
                print('Connecting to ', name, ' (', value['addr'],')', ' ...', sep='')
                try :
                    gadget = pySmartGadget.SHT31(value['addr'])
                    print('Successfully connected to', name)
                    gadget.setDeviceName(name)
                    print('Reading data of', name)
                    gadget.readLoggedDataInterval()
                    
                    while 1:
                        if False is gadget.waitForNotifications(5) or False is gadget.isLogReadoutInProgress():
                            print('Finished reading data of', name)
                            readGadgets.append(name)
                            storeData(name, gadget.loggedData)
                            logInterval = gadget.readLoggerIntervalMs() 
                            if logInterval != value['logInterval']:
                                print('Current logger interval (', logInterval, ') does not match the specified value is. Now set to: ', value['logInterval'], sep='')
                                gadget.setLoggerIntervalMs(value['logInterval'])
                            gadget.disconnect()
                            break
                    
                    break
                except btle.BTLEException as ex:
                    print('Could not connect to ', name, ', retrying later.', sep='')

    

def show():
    try:
        data = pd.read_csv(logFileName, index_col=0)
        data.index = [dt.datetime.fromtimestamp(int(x/1000)) for x in data.index.values]
    
        data = data.ffill()
        data = data.bfill()
        
        data.plot(aa=False, drawstyle='steps')
        
        plt.show()
    except Exception as ex:
        print('Could not read existing logfile.')
        
 
def main():
    while True:
        string = input('Enter your input [read/show/anything else -> quit]:')
        if string in ['read', 'r']:
            read()
        elif string in ['show', 's']:
            show()
        else:
            break
    
if __name__ == "__main__":
    main()
    print('Exit')
