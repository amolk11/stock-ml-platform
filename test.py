import yfinance as yf

data = yf.Ticker("AAPL")
df = data.history(period="1mo")

print(df.head())