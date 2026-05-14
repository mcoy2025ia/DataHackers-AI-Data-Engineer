SECTORS: dict[str, list[str]] = {
    "Semiconductors":          ["NVDA", "AMD", "AVGO", "INTC"],
    "Big Tech & Cloud":        ["MSFT", "AAPL", "GOOGL", "META", "AMZN"],
    "Software & AI Services":  ["PLTR", "CRM", "NOW", "NFLX"],
    "FinTech & Banking":       ["JPM", "COIN"],
    "Cybersecurity":           ["CRWD"],
    "High-Beta / Speculative": ["TSLA", "RKLB"],
}


def get_peers(ticker: str) -> list[str]:
    """Return sector peers for ticker, excluding the ticker itself."""
    for tickers in SECTORS.values():
        if ticker in tickers:
            return [t for t in tickers if t != ticker]
    return []


def get_sector(ticker: str) -> str | None:
    """Return the sector name for ticker, or None if unknown."""
    for sector, tickers in SECTORS.items():
        if ticker in tickers:
            return sector
    return None
