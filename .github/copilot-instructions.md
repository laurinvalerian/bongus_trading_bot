# Bongus Trading Bot – AI Instructions (2026)

## Projekt-Überblick
Du arbeitest am **Bongus Delta-Neutral Funding Arbitrage Bot** für Binance (Spot + Perpetual).  
Ziel: Institutionelle Zuverlässigkeit, niedrigste Latenz (Tokyo-Server), konsistente Profitabilität durch Funding-Rate-Arbitrage.

## Kern-Philosophie (NIE brechen)
- Safety first: Der Bot darf das Konto **niemals** gefährden
- Jede Strategie-Änderung **muss** mit `walk_forward.py` validiert werden
- Production-ready Code only (keine Quick-Hacks)
- Low-Latency-First: Event-driven, minimale Allokationen im Hot-Path
- API-Keys **ausschließlich** über Environment-Variablen (nie hart-codiert)

## Hard Rules aus config.py & risk_engine.py
- MAX_DRAWDOWN_PCT = 0.10 (10%)
- SOFT_DRAWDOWN_PCT = 0.05 (5%)
- MAX_SYMBOL_CONCENTRATION = 0.50
- ENTRY_ANN_FUNDING_THRESHOLD = 0.10 (10% annualisiert)
- Immer Taker/Maker-Fee, Slippage und Funding-Schedule berücksichtigen
- Max 1-2% Risiko pro Trade (auch wenn nicht explizit in Config)

## Technischer Stack & Stil
- Primär: **Polars** (für Geschwindigkeit) + pandas wo nötig
- Rust Execution Engine (`execution_alpha.py`) für Live-Trading
- FastAPI + Uvicorn für Web-Dashboard
- Telegram-Alerts via `telegram_alerter.py`
- Walk-Forward-Backtesting ist Pflicht
- Typisierung streng (pyrightconfig.json)

## Bevorzugter Workflow bei jeder Aufgabe
1. Planung & Risiko-Check
2. Config-Parameter anpassen (wenn nötig)
3. Walk-Forward-Backtest durchführen
4. Code in sauberen Modulen implementieren
5. Logging + Error-Handling + Circuit-Breaker hinzufügen
6. Deployment-Hinweise aus TRAINING_AND_DEPLOYMENT.md beachten (Tokyo EC2 bevorzugt)

Sei extrem vorsichtig bei Live-Trading-Code. Cleverness < Sicherheit.