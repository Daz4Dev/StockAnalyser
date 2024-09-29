from flask import Flask, request, jsonify
from indicators_calculation import fetch_stock_data,generate_signals,compute_final_signals,calculate_profit_loss
import matplotlib.pyplot as plt
from datetime import datetime,timedelta
import io
import base64

app = Flask(__name__)

@app.route('/compute-signals', methods=['POST'])
def compute_signals():
    data = request.get_json()
    stock = data.get('stock')
    indicators = data.get('indicators')
 

    stock_data = fetch_stock_data(stock)
    all_signals_list = generate_signals(stock_data,indicators)
    final_signal_list = compute_final_signals(all_signals_list,indicators)
    profits, total_profit, buy_times, sell_times = calculate_profit_loss(final_signal_list,stock_data['close'])

    num_buys = final_signal_list['buy'].sum()
    num_sells = final_signal_list['sell'].sum()


    result = {'profits':profits,
              'total_profit':int(total_profit),
              'num_buys':int(num_buys),
              'num_sells':int(num_sells),
              'num_trades':int(min(num_buys,num_sells)),
              }
    



    # Filter for the last month of data
    end_date = stock_data.index[-1]  # Latest timestamp in the dataset
    start_date = end_date - timedelta(days=30)  # One month ago

    # Filter closing prices and timestamps for the last month
    recent_data = stock_data[(stock_data.index >= start_date) & (stock_data.index <= end_date)]
    recent_prices = recent_data['close'].values  # Closing prices
    recent_dates = recent_data.index  # Date index

    # Create a plot
    plt.figure(figsize=(10, 5))
    plt.plot(recent_dates, recent_prices, label='Closing Prices', color='blue')

    # Find buy and sell indices in the recent timeframe
    recent_buy_indices = [i for i, time in enumerate(buy_times) if start_date <= time <= end_date]
    recent_sell_indices = [i for i, time in enumerate(sell_times) if start_date <= time <= end_date]

    # Plot buy points
    if recent_buy_indices:
        plt.scatter(
            [buy_times[i] for i in recent_buy_indices],
            [recent_prices[recent_data.index.get_loc(buy_times[i])] for i in recent_buy_indices],
            marker='^', color='green', label='Buy Points', s=100
        )

    # Plot sell points
    if recent_sell_indices:
        plt.scatter(
            [sell_times[i] for i in recent_sell_indices],
            [recent_prices[recent_data.index.get_loc(sell_times[i])] for i in recent_sell_indices],
            marker='v', color='red', label='Sell Points', s=100
        )

    plt.title(f'Stock Closing Prices and Buy/Sell Points for {stock} (Last Month)')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()

    # Increase margins
    plt.margins(y=0.1)  # Adjust y-margin (10% of the data range)
    # Save plot to a PNG image
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)


    # Encode image to base64
    plot_data = base64.b64encode(img.getvalue()).decode('utf-8')

    
    result['plot'] = plot_data
    
    result = jsonify(result)
    
    return result

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)