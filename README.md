# 🛒 Flipkart.com Price Error Detector

Monitors **1,00,000+ unique Flipkart.com products** using a multithreaded direct-HTTP search sweep engine (`GET https://www.flipkart.com/search?...`, parsing 40 structured JSON product listings per request from `window.__INITIAL_STATE__`), tracks compact historical average prices in `price-data.json`, detects pricing errors (`>= 30%` drop vs historical average), and sends instant tiered Telegram alerts (`>= 30%` → `-5460461500`, `>= 70%` → `-5533342235`).

## 🚀 Usage

```bash
# Harvest 1,00,000 unique Flipkart products across 50 categories
npm run harvest

# Run one monitoring sweep
npm run monitor

# Run continuously every 5 minutes
npm run monitor:loop

# Add custom Flipkart PIDs
node price-monitor.js --add MOBHQDMKCCDK2VT7

# List tracked PIDs & stats
node price-monitor.js --list

# Send a test Telegram alert
npm test
```

## ⚙️ Environment Variables

- `TELEGRAM_BOT_TOKEN` — Telegram bot token
- `TELEGRAM_CHAT_ID` — Primary Telegram group chat ID (`>= 50%` discounts)
- `MID_DISCOUNT_TELEGRAM_CHAT_ID` — Optional group chat ID for `>= 70%` discounts
- `HIGH_DISCOUNT_TELEGRAM_CHAT_ID` — Optional group chat ID for `> 90%` discounts
- `WORKERS` / `CONCURRENCY_PER_WORKER` — Worker thread pool tuning
