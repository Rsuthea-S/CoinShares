import pandas as pd
import sys
import datetime as dt
from datetime import datetime
import os
# Implement the code needed to answer the following questions and if a value is requested print it into a text file :
# 1. Parse the 2 CSV files in the data folder
#
# 2. What is the minimum low value for BTC-USD ?
#
# 3. What is the maximum high value for BTC-USD ?
#
# 4. What is the average close value for ETH-USD ?
#
# 5. What is the close value on 2021-06-03 for BTC-USD ?
#
# 6. What is the 30-days Simple Moving Average value of the close at the close of 2021-04-25 for BTC-USD ?
#
# 7. What is the complexity of your Simple Moving Average algorithm ? Can you make it faster ?
#
# 8. The Simple Moving Average value seems a little bit off. How to make it more coherent ? Implement it.
#
# 9. Resample each CSV data set as weekly candles (opening Monday and closing Sunday). If a candle has to start
# in the middle of the week, you can use the first day as the opening day. And vice versa for closing.

#used for 5. not needed for single question, but use to stop duplicate code later
def add_datetime(ticker_data):
    """
    Used for question 5, creates datetime column based on given ticker data
    :param ticker_data: [Dataframe] ticker data for - timestamp column must be present
    :return:[Dataframe column] datetime column for use in Dataframe
    """
    date_time = pd.to_datetime(ticker_data['timestamp'], unit='s')
    return date_time


def create_weekly(ticker_data):
    """
    creates the weekly Dataframe, given daily data
    :param ticker_data: [Dataframe] either BTC or ETH ticker data -- starting 2021-03-22 (would be change for real implementation)
    :return:[Dataframe] concatenation of daily data to weekly.
    """
    # does groupby work in the situation? resample?
    # used a for loop as it appears simpler to implement - although slower

    columns = ['open', 'high', 'low', 'close', 'volume', 'datetime']
    weekly_btc = pd.DataFrame(columns=columns)

    # the data starts on 23/03/2021, a tuesday, so we can implement from 2021-03-22
    start_date = '2021-03-22'
    start_date = datetime.strptime(start_date,'%Y-%m-%d')

    #gathering all necessary data from the dataframe slice and appending it to weekly DF
    while start_date<btc.iloc[-1]['datetime']:
        end_date = start_date + dt.timedelta(days=7)
        weekly_data = btc[(btc['datetime'] > start_date) & (btc['datetime']<=(end_date))]
        open = (weekly_data.iloc[0]['open'])
        close = weekly_data.iloc[-1]['close']
        high = weekly_data['high'].max()
        low = weekly_data['low'].min()
        volume = weekly_data['volume'].sum()
        combined = [open,high,low,close,volume,end_date]
        weekly_btc.loc[len(weekly_btc)] = (combined)
        start_date=end_date
    return weekly_btc


if __name__ == "__main__":
    #moving up one directory
    path_parent = os.path.dirname(os.getcwd())
    os.chdir(path_parent)

    #1. parsing csv - can also be completed with csv package, however dataframes are used in other questions
    btc = pd.read_csv('data/BTC-USD_candles.csv', header=0)
    eth = pd.read_csv('data/ETH-USD_candles.csv', header=0)

    #2. minimum low value for btc. returns value only, not whole row
    print('BTC minimum low value : {}'.format(min(btc['low'])))

    #3. maximum high value for btc. similar to 2.
    print('BTC maximum high value: {}'.format(max(btc['high'])))

    #4. avrage eth close value. similar as above
    print('Average ETH close value: {}'.format((eth['close'].mean())))

    #5. close value on 2021-06-03 for BTC
    # adding datetime field. could take long if a lot of data - as only 78 rows of data, this is fine for reusability
    # would be simpler to find the unix timestamp for date, but then there may be consistency issues if
    # timestamped early/late
    btc['datetime']=add_datetime(btc)
    #added for future questions (not needed but useful)
    eth['datetime']=add_datetime(eth)
    close_value = btc[btc['datetime'] == ('2021-06-03')]['close'].to_string(index=False)
    print('Closing value of BTC on 2021-06-03:{}'.format(close_value))


    #6. 30 day SMA for BTC close on 2021-04-25
    btc['rolling_average']=btc.rolling(window = 30)['close'].mean()
    sma = btc[btc['datetime']==('2021-04-25')]['rolling_average'].to_string(index = False)
    print('30 day SMA for BTC on 2021-04-25 :{}'.format(sma))


    #7. time complexity of #6.
    """
    time complexity = O(n)
    space complexity = O(windowsize)
    pandas is smart, running through rolling is probably
    a FIFO algorithm with a window size array. 
    Mean is calculated for each iteration 
    """

    #8. coherence
    # Looking at the csv in Excel, the datetime was not ordered,
    # which could lead to SMA from the previous days not being accurate
    # question is do we use SMA from previous 30 days which are recorded or previous real days
    btc = btc.sort_values(by=['timestamp'])
    btc['rolling_average'] = btc.rolling(window=30)['close'].mean()
    sma = btc[btc['datetime'] == ('2021-04-25')]['rolling_average'].to_string(index=False)
    print('30 day SMA for BTC on 2021-04-25 :{}'.format(sma))

    # 2021-03-31 close value is stated as -1e+15 (-9999999)
    ## Multiple ways of changing the value, i.e.
    # by grabbing the close value online [best choice - but won't be used]
    # predicting average value (mean value of row, entire column[bad], pevious/next day)
    # removing the row [bad?]
    replace = (btc.loc[btc['datetime']=='2021-03-31'][['open','high','low']]).mean(axis =1)

    btc.loc[btc['datetime']=='2021-03-31','close']=replace

    #replaced whole column, as it is quicker than implementing spot changes
    btc['rolling_average'] = btc.rolling(window=30)['close'].mean()
    sma = btc[btc['datetime'] == ('2021-04-25')]['rolling_average'].to_string(index=False)
    print('30 day SMA for BTC on 2021-04-25 :{}'.format(sma))


    #9. weekly candles
    #so we can use either date or dateimte columns for a reference point
    #this has been sorted via question 8 so head so show the top 5 dates
    #could be put in a loop to stop duplicate code
    create_weekly(btc).to_csv('data/BTC-USD_weekly_candles.csv')
    #not much work done on ETH, assumption that all information is correct (unlike BTC)
    create_weekly(eth).to_csv('data/ETH-USD_weekly_candles.csv')
