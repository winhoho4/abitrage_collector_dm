# Crypto Arbitrage Data Collector

This project collects order book data from multiple cryptocurrency exchanges using `ccxt.pro` and publishes the data using `zmq`. The data includes bid and ask prices from different exchanges and calculates the difference between them. If there is a significant time difference (greater than 10 seconds) between the data from two exchanges, a warning message is logged.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Output Format](#output-format)
- [Contributing](#contributing)
- [License](#license)

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/winhoho4/abitrage_collector_ov.git
    cd https://github.com/winhoho4/abitrage_collector_ov
    ```

2. **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. **Install the required packages:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up environment variables:**
    Create a `.env` file in the root directory of the project and add the following variables:
    ```env
    OV_COINS=BTC,ETH,BCH,ETC,SOL
    OV_EXCHANGES=binance,bybit,okx,bitget,hyperliquid
    ```

## Usage

To start collecting and publishing data, run the following command:
```bash
python abitrage_collector_ov.py

