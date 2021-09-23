## An Adaptive Order Execution Strategy for VWAP tracking
This folder contains related materials of an adaptive execution strategy for high
frequency trading in SET (Stock Exchange of Thailand). The objective of the strategy is to buy or sell stocks for a specific amount in SET to match the actual daily market
VWAP (Volume-Weighted Average Price) as much as possible in a specified time interval for daytrading. By sending required amounts of stocks to acquire or liquidate in a specific time interval, our adaptive execution algorithm will calculate a volume profile and a ratio between Limit Order
(LO) and Market Order (MO) and send the order at each time step until the end of the time interval. The algorithm can be tested in
an order simulation system by using a historical data set from Stock Exchange of Thailand (SET).

### Software and version
* Python 3.8.5
    * Flask 1.1.2
    * flask_cors 3.0.10
    * Numpy 1.19.2
    * Pandas 1.1.3
    * Imblearn 0.8.0
    * Sklearn 0.24.2
* React 17.02
* Node.js 10.19.0

### Installation
* Install all necessary python libraries
* Install Node.js
* Use "npm install" at ./Frontend

### How to run
* Use "npm start" at ./Frontend. The frontend part will run on port 3000
* At ./Backend use "python backend_server.py". The backend part will run on port 1111
* go to port 3000 in your browser to use website

### Contirbutors
#### Project manger
* Asst.Prof. Yodyium Tipsuwan, Ph.D. CQF.
#### Chief Engineer
* Patara Chavalit, CQF
#### Secretary
* Isareeya Warapakornkit, CTP
#### Engineer
* Aphisit    Leksomboon
* Apinya     Leangaramkul
* Nattapon Prasatthong
* Nattawut Tanomwong
* Olan    Sanguanprasit
* Rossukon Kasikijvorakul
* Wanawat    Thawatwong
