from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['stocks-mini']

def fetch_stock_data(instrument_token):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['stocks-mini']
    collection = db[instrument_token]
    stock_data = list(collection.find())
    client.close()
    
    stock_data = pd.DataFrame(stock_data)
    stock_data.drop('_id',axis=1,inplace=True)
    stock_data['date'] = pd.to_datetime(stock_data['date'])
    # Set 'date' as the DataFrame index
    stock_data.set_index('date', inplace=True)

    # Sort by 'date' to ensure chronological order
    stock_data.sort_index(inplace=True)
    return stock_data


def calculate_indicators(stock_data,indicators):
    indicator_results = {}
    
    # Convert columns to correct types
    close = stock_data['close'].astype(float)
    high = stock_data['high'].astype(float) 
    low = stock_data['low'].astype(float)
    volume = stock_data['volume'].astype(float)
    

    # Define indicator calculation functions
    indicator_functions = {
        'RSI':                lambda: ta.rsi(close, length=14, scalar=None, talib=None, drift=None, offset=None),
        'MACD':               lambda: ta.macd(close, fast=12, slow=26, signal=9, talib=None, offset=None),
        'Bollinger_Bands':    lambda: ta.bbands(close, timeperiod=20, std=None),
        'ATR':                lambda: ta.atr(high, low, close, length=14, lensig=None, scalar=None, mamode=None, drift=None, offset=None),
        'ADX':                lambda: ta.adx(high, low, close, length=14),
        'Stochastic':         lambda: ta.stoch(high, low, close, fastk_period=14, slowk_period=3, slowd_period=3, mamode=None, offset=None),
        'VWAP':               lambda: ta.vwap(high,low,close,volume,anchor=None, offset=None),
        'Supertrend':         lambda: ta.supertrend(high,low,close,length=None, multiplier=None, offset=None),
        'Aroon':              lambda: ta.aroon(high, low, length=14, scalar=None, talib=None, offset=None),
        'Keltner':            lambda: ta.kc(high, low, close),
        'Chaikin':            lambda: ta.ad(high, low, close, volume),
        'Awesome_Oscillator': lambda: ta.ao(high, low, fast=None, slow=None,offset=None),
        'CCI':                lambda: ta.cci(high, low, close, length=None, c=None, offset=None, talib=None),
        'Parabolic_SAR':      lambda: ta.psar(high, low, close=None, af0=0.02, af=0.02, max_af=0.02, offset=None),
    }


    for indicator in indicators:
        if indicator in indicator_functions:
            result = indicator_functions[indicator]()
            # Store the complete DataFrame or Series
            indicator_results[indicator] = result

    return indicator_results


# TESTING CALCS
# our_Data = fetch_stock_data('BPCL')
# print(our_Data)
# #liste = ['RSI','VWAP','MACD','Bollinger_Bands-5','ATR','ADX-3','Stochastic-2','Supertrend-4','Aroon-3','Keltner-3','Chaikin','Awesome_Oscillator','CCI','Parabolic_SAR-4']
# liste = ['RSI','MACD']
# iresults = calculate_indicators(our_Data,liste)
# available_indicators = iresults.keys()
# avail_list = list(available_indicators)
# print(avail_list)
# print(type(iresults))
# print('The reults are :')
# print(iresults)
# print(iresults['MACD'])
# print(iresults['MACD_0'])
# print(iresults['MACD_1'])


def generate_signals(stock_data, indicators):
    # Calculate indicators
    indicator_results = calculate_indicators(stock_data, indicators)
    
    # Create a DataFrame to store signals and indicators
    signals = pd.DataFrame(index=stock_data.index)

    # Extract calculated indicators to the signals DataFrame
    for key, value in indicator_results.items():
        #if isinstance(value, pd.DataFrame):
        if True:    
            if key == 'MACD':
                signals[f'{key}_line'] = value['MACD_12_26_9']  # MACD line
                signals[f'{key}_signal'] = value['MACDs_12_26_9']  # Signal line
                signals[f'{key}_histogram'] = value['MACDh_12_26_9']  # MACD histogram

            elif key == 'Bollinger_Bands':
                signals[f'{key}_low']  = value['BBL_5_2.0']  # Lower Band
                signals[f'{key}_mid']  = value['BBM_5_2.0']  # Upper Band
                signals[f'{key}_up']   = value['BBU_5_2.0']
            
            elif key == 'Stochastic':
                signals[f'{key}_k'] = value['STOCHk_14_3_3']  # %K line
                signals[f'{key}_d'] = value['STOCHd_14_3_3']  # %D line
            
            elif key == 'ADX':
                signals[key]          = value['ADX_14']  # ADX line
                signals[f'{key}_dmp'] = value['DMP_14']  # +DI
                signals[f'{key}_dmn'] = value['DMN_14']  # -DI
            
            elif key == 'Supertrend':
                signals[f'{key}_value'] = value['SUPERT_7_3.0']  # Supertrend line
                signals[f'{key}_dir']   = value['SUPERTd_7_3.0']
                signals[f'{key}_line']  = value['SUPERTl_7_3.0']
                signals[f'{key}_s']     = value['SUPERTs_7_3.0']

            elif key == 'Aroon':
                signals[f'{key}_down'] = value['AROOND_14']  # Aroon line
                signals[f'{key}_up']   = value['AROONU_14']
                signals[f'{key}_OSC']  = value['AROONOSC_14']

            elif key == 'Keltner':
                signals[f'{key}_low']   = value['KCLe_20_2']  # Keltner line
                signals[f'{key}_Basis'] = value['KCBe_20_2']
                signals[f'{key}_up']    = value['KCUe_20_2']

            elif key == 'Chaikin':
                signals[key] = value  # Chaikin line

            elif key == 'Awesome_Oscillator':
                signals[key] = value  # AO line

            elif key == 'CCI':
                signals[key] = value  # CCI line

                    # --------------------------------------#

            elif key == 'Parabolic_SAR':
                signals[f'{key}_value']  = value['PSARl_0.02_0.02']  # PSAR line
                signals[f'{key}_s']    = value['PSARs_0.02_0.02']
                signals[f'{key}_af']   = value['PSARaf_0.02_0.02']
                signals[f'{key}_rev']  = value['PSARr_0.02_0.02']

            elif key == 'ATR':
                signals[key] = value  # ATR line
            
            elif key == 'VWAP':
                signals[key] = value  # VWAP line
            
            elif key == 'RSI':
                signals[key] = value  # RSI line
            
            else:
                raise NameError(f'{key} does not have a pandas object')

    # Initialize individual buy and sell signals
    for indicator in indicators:
        signals[f'{indicator}_buy'] = 0
        signals[f'{indicator}_sell'] = 0

    # Initialize OVERALL buy and sell signals
    signals['buy'] = 0
    signals['sell'] = 0

    # Initialize holding states for each indicator separately
    holding_states = {indicator: False for indicator in indicators}

    conditions = {
    'RSI': {
        'buy': lambda x: x < 30,  # Buy signal when RSI is below 30 (oversold)
        'sell': lambda x: x > 70,  # Sell signal when RSI is above 70 (overbought)
    },
    'MACD': {
        'buy': lambda macd_line, signal_line: macd_line > signal_line,  # Buy when MACD line crosses above the signal line
        'sell': lambda macd_line, signal_line: macd_line < signal_line,  # Sell when MACD line crosses below the signal line
    },
    'Bollinger_Bands': {
        'buy': lambda price, lower: price < lower,  # Buy when the price is below the lower Bollinger Band
        'sell': lambda price, upper: price > upper,  # Sell when the price is above the upper Bollinger Band
    },
    'ADX': {
        'buy': lambda adx, dmp, dmn: adx > 20 and dmp > dmn,  # Buy when ADX is above 20 and +DI is above -DI
        'sell': lambda adx, dmp, dmn: adx > 20 and dmp < dmn,  # Sell when ADX is above 20 and +DI is below -DI
    },
    'Stochastic': {
        'buy': lambda stoch_k: stoch_k < 20,  # Buy when %K line is below 20 (oversold)
        'sell': lambda stoch_k: stoch_k > 80,  # Sell when %K line is above 80 (overbought)
    },
    'VWAP': {
        'buy': lambda price, vwap: price > vwap,  # Buy when the price is above VWAP
        'sell': lambda price, vwap: price < vwap,  # Sell when the price is below VWAP
    },
    'ATR': {
        'buy': lambda atr: atr < atr.mean(),  # Buy when ATR is below its mean (lower volatility)
        'sell': lambda atr: atr > atr.mean(),  # Sell when ATR is above its mean (higher volatility)
    },
    'Supertrend': {
        'buy': lambda price, supertrend: price > supertrend,  # Buy when the price is above the Supertrend line
        'sell': lambda price, supertrend: price < supertrend,  # Sell when the price is below the Supertrend line
    },
    'Aroon': {
        'buy': lambda aroon_up, aroon_down: aroon_up > 70,  # Buy when Aroon Up is above 70 (strong uptrend)
        'sell': lambda aroon_up, aroon_down: aroon_down > 70,  # Sell when Aroon Down is above 70 (strong downtrend)
    },
    'Keltner': {
        'buy': lambda price, upper: price < upper,  # Buy when the price is below the Keltner Upper Band
        'sell': lambda price, lower: price > lower,  # Sell when the price is above the Keltner Lower Band
    },
    'Chaikin': {
        'buy': lambda chaikin: chaikin > 0,  # Buy when Chaikin Oscillator is above 0
        'sell': lambda chaikin: chaikin < 0,  # Sell when Chaikin Oscillator is below 0
    },
    'Awesome_Oscillator': {
        'buy': lambda ao: ao > 0,  # Buy when Awesome Oscillator is above 0
        'sell': lambda ao: ao < 0,  # Sell when Awesome Oscillator is below 0
    },
    'CCI': {
        'buy': lambda cci: cci < -100,  # Buy when CCI is below -100 (oversold)
        'sell': lambda cci: cci > 100,  # Sell when CCI is above 100 (overbought)
    },
    'Parabolic_SAR': {
        'buy': lambda price, sar: price > sar,  # Buy when the price is above the Parabolic SAR
        'sell': lambda price, sar: price < sar,  # Sell when the price is below the Parabolic SAR
    }
    }


    # Iterate over rows to generate buy and sell signals
    for i in range(1, len(signals)):
        for indicator in indicators:
            

            if indicator == 'MACD':#name correct
                macd_line = signals[f'{indicator}_line'].iloc[i]
                signal_line = signals[f'{indicator}_signal'].iloc[i]
                if conditions[indicator]['buy'](macd_line, signal_line) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](macd_line, signal_line) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False


            elif indicator == 'Bollinger_Bands':#name correct
                current_price = stock_data['close'].iloc[i]  # Assuming you have a Close column
                if conditions[indicator]['buy'](current_price, signals[f'{indicator}_low'].iloc[i]) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](current_price, signals[f'{indicator}_up'].iloc[i]) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'ADX':#name correct
                adx = signals[indicator].iloc[i]
                dmp = signals[f'{indicator}_dmp'].iloc[i]
                dmn = signals[f'{indicator}_dmn'].iloc[i]
                if conditions[indicator]['buy'](adx, dmp, dmn) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](adx, dmp, dmn) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Stochastic':#name correct
                stoch_k = signals[f'{indicator}_k'].iloc[i]
                stoch_d = signals[f'{indicator}_d'].iloc[i]
                if conditions[indicator]['buy'](stoch_k) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](stoch_k) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'VWAP':#name correct
                vwap = signals[indicator].iloc[i]  # Assuming VWAP is calculated and available
                current_price = stock_data['close'].iloc[i]
                if conditions[indicator]['buy'](current_price, vwap) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](current_price, vwap) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'ATR':#name correct
                atr = signals[indicator].iloc[i]
                if conditions[indicator]['buy'](atr) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](atr) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Supertrend':#name correct
                if conditions[indicator]['buy'](stock_data['close'].iloc[i], signals[f'{indicator}_value'].iloc[i]) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](stock_data['close'].iloc[i], signals[f'{indicator}_value'].iloc[i]) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Aroon':#name correct
                aroon_up = signals[f'{indicator}_up'].iloc[i]
                aroon_down = signals[f'{indicator}_down'].iloc[i]
                if conditions[indicator]['buy'](aroon_up, aroon_down) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](aroon_up, aroon_down) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Keltner':#name correct
                current_price = stock_data['close'].iloc[i]
                upper_band = signals[f'{indicator}_up'].iloc[i]
                lower_band = signals[f'{indicator}_low'].iloc[i]
                if conditions[indicator]['buy'](current_price, upper_band) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](current_price, lower_band) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Chaikin':
                chaikin = signals[indicator].iloc[i]#name correct
                if conditions[indicator]['buy'](chaikin) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](chaikin) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Awesome_Oscillator':
                ao = signals[indicator].iloc[i]#name correct
                if conditions[indicator]['buy'](ao) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](ao) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'CCI':
                cci = signals[indicator].iloc[i]#name correct
                if conditions[indicator]['buy'](cci) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](cci) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'Parabolic_SAR':
                sar = signals[f'{indicator}_value'].iloc[i]#name correct
                current_price = stock_data['close'].iloc[i]
                if conditions[indicator]['buy'](current_price, sar) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](current_price, sar) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False

            elif indicator == 'RSI':
                rsi = signals[indicator].iloc[i]
                if conditions[indicator]['buy'](rsi) and not holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_buy'] = 1
                    holding_states[indicator] = True
                elif conditions[indicator]['sell'](rsi) and holding_states[indicator]:
                    signals.at[signals.index[i], f'{indicator}_sell'] = 1
                    holding_states[indicator] = False
    
    #print('the signals are in :')
    #print(signals)

    return signals


# TEST ON SIGNALS
# t_data = fetch_stock_data('BPCL')
# siglist = ['MACD']
# signal_ans = generate_signals(t_data,siglist)
# # print(type(signal_ans)) -> list
# # Create a DataFrame from the signal list
# signal_df = pd.DataFrame(signal_ans, columns=['Time', 'Signal'])


# # Count the number of trades per day
# signal_df['Date'] = signal_df['Time'].dt.date  # Extract date from timestamp
# trades_per_day = signal_df.groupby('Date').size().reset_index(name='Number of Trades')

# # Write trades per day to a CSV file
# trades_output_csv_file = 'daily_trades_count.csv'  # Specify the desired output CSV file name
# trades_per_day.to_csv(trades_output_csv_file, index=False)

# print(f'Trade counts written to {trades_output_csv_file}')


# Function to compute final signals with a margin of 1 candle
def compute_final_signals(signals, indicators):
    # Initialize final buy/sell columns
    signals['buy'] = 0
    signals['sell'] = 0
    
    for i in range(len(signals)):
        # Check for final buy condition within margin
        for j in range(max(0, i - 1), min(len(signals), i + 2)):
            for indicator in indicators:
                if signals[f'{indicator}_buy'].iloc[j] == 1:
                    signals.at[signals.index[i], 'buy'] = 1
                    break

        # Check for final sell condition within margin
        for j in range(max(0, i - 1), min(len(signals), i + 2)):
            for indicator in indicators:
                if signals[f'{indicator}_sell'].iloc[j] == 1:
                    signals.at[signals.index[i], 'sell'] = 1
                    break

    
    return signals[['buy','sell']]

def compute_final_signals_intersection(signals, indicators):
    # Initialize final buy/sell columns
    signals['buy'] = 0
    signals['sell'] = 0

    for i in range(len(signals)):
        # Check for final buy condition within margin
        buy_intersection = all(signals[f'{indicator}_buy'].iloc[i] == 1 for indicator in indicators)
        if buy_intersection:
            signals.at[signals.index[i], 'buy'] = 1

        # Check for final sell condition within margin
        sell_intersection = all(signals[f'{indicator}_sell'].iloc[i] == 1 for indicator in indicators)
        if sell_intersection:
            signals.at[signals.index[i], 'sell'] = 1

    return signals[['buy', 'sell']]




def calculate_profit_loss(signals, prices):
    buy_times = []
    sell_times = []
    profits = []
    total_profit = 0

    holding = False
    buy_price = 0

    # Iterate through each row in the signals dataframe
    for index, row in signals.iterrows():
        if row['buy'] and not holding:
            # Record the buy time and price
            buy_times.append(index)
            buy_price = prices.loc[index]
            holding = True
        
        elif row['sell'] and holding:
            # Record the sell time and price
            sell_times.append(index)
            sell_price = prices.loc[index]
            profit = sell_price - buy_price
            profits.append(profit)
            total_profit = sum(profits)
            holding = False

    # If there is an active buy without a corresponding sell, we do not consider it
    return profits,total_profit,buy_times,sell_times
     
    






















# TRIALS
# stocks_data = fetch_stock_data('BPCL')
# inds = ['Bollinger_Bands']
# signal_list = generate_signals(stocks_data,inds)
# signals_df = pd.DataFrame(signal_list, columns=['Time', 'Signal'])
# signals_df.to_csv('signals.csv', index=False)

# profits, total_profit = calculate_profit_loss(signal_list, stocks_data)

# # Print results
# print(f'Profits: {profits}')
# print(f'Total Profit : {total_profit}')

if __name__ == '__main__':
    print("imported")
































