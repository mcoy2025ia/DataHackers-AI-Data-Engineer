SECTORS = {
    'Semiconductors': ['NVDA', 'AMD', 'AVGO', 'INTC'],
    'Big Tech & Cloud': ['MSFT', 'AAPL', 'GOOGL', 'META', 'AMZN'],
    'Software & AI Services': ['PLTR', 'CRM', 'NOW', 'NFLX'],
    'FinTech & Banking': ['JPM', 'COIN'],
    'Cybersecurity': ['CRWD'],
    'High-Beta / Speculative': ['TSLA', 'RKLB']
}

def get_peers(ticker):
    \"\"\"Devuelve la lista de competidores del mismo sector para un ticker dado.\"\"\"
    for sector, tickers in SECTORS.items():
        if ticker in tickers:
            return [t for t in tickers if t != ticker]
    return []
```"