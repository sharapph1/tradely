import json
import math
import os
import time
import logging
from collections import defaultdict, deque
import requests
from kafka import KafkaConsumer

# ---------------------------------------------------------
# 1. CONFIGURATION & AUTHENTICATION
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("RedAlphasOptimal")

GATEWAY = "http://192.168.65.157:3002"
BROKER = "192.168.65.157:9094"
USERNAME = "mn2668"

TOKEN = os.environ.get(
    "BOT_ACCESS_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZWFtX2lkIjoidGVhbV9wMmozYTViIiwidXNlcm5hbWUiOiJtbjI2NjgiLCJ0ZWFtX25hbWUiOiJSRUQgQUxQSEFTIiwiaXNfdmVyaWZpZWQiOnRydWUsImlhdCI6MTc5MzI2MTg0NSwiZXhwIjoxNzkzNDM0NjQ1fQ.03wwoQ5rvUHXurxuIKGp_CrztWK9UOzeyyPLm1ophcg"
)

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

session = requests.Session()
session.headers.update(headers)


def clip(val, low, high):
    return max(low, min(high, val))


def mean(values):
    return sum(values) / len(values) if values else 0.0


def std_dev(values):
    if len(values) < 2:
        return 0.0
    m = mean(values)
    variance = sum((x - m) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


class OptimalTradingBot:
    def __init__(self):
        self.tradable_tickers = set()
        self.cash = 1000000.0
        self.positions = {}    # ticker -> {"quantity": int, "avg_buy_price": float, "is_short": bool}
        self.candles = defaultdict(lambda: deque(maxlen=150))
        self.sentiment = defaultdict(float)
        self.last_trade_date = {}

    def initialize_universe_and_history(self):
        try:
            resp = session.get(f"{GATEWAY}/api/v1/stocks", timeout=5)
            resp.raise_for_status()
            stocks_list = resp.json().get("stocks", [])
            new_tickers = {s["ticker"] for s in stocks_list if "ticker" in s}

            if new_tickers and new_tickers != self.tradable_tickers:
                self.tradable_tickers = new_tickers
                logger.info(f"Tradable Universe Loaded ({len(self.tradable_tickers)}): {sorted(self.tradable_tickers)}")

                for ticker in sorted(self.tradable_tickers):
                    self.preload_stock_history(ticker, days=120)

            self.sync_portfolio()
        except Exception as e:
            logger.error(f"Universe initialization error: {e}")

    def preload_stock_history(self, ticker, days=120):
        try:
            resp = session.get(
                f"{GATEWAY}/api/v1/stocks/history",
                params={"ticker": ticker, "days": days},
                timeout=4
            )
            if resp.status_code == 200:
                payload = resp.json()
                bars = (
                    payload if isinstance(payload, list)
                    else payload.get("history") or payload.get("bars") or payload.get("data") or []
                )
                self.candles[ticker].clear()
                for b in bars:
                    c_val = b.get("close") or b.get("c")
                    if c_val is not None:
                        self.candles[ticker].append({
                            "open": float(b.get("open", b.get("o", c_val))),
                            "high": float(b.get("high", b.get("h", c_val))),
                            "low": float(b.get("low", b.get("l", c_val))),
                            "close": float(c_val),
                            "volume": float(b.get("volume", b.get("v", 1000)))
                        })
                logger.info(f"Loaded {len(self.candles[ticker])} historical bars for {ticker}")
        except Exception as e:
            logger.warning(f"History preload failed for {ticker}: {e}")

    def sync_portfolio(self):
        try:
            resp = session.get(f"{GATEWAY}/api/v1/portfolio", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                for cash_key in ("cash", "available_cash", "cash_balance", "balance"):
                    if cash_key in data and data[cash_key] is not None:
                        self.cash = float(data[cash_key])
                        break

                raw_positions = data.get("positions") or data.get("holdings") or data.get("open_positions") or []
                self.positions.clear()
                for pos in raw_positions:
                    tkr = pos.get("ticker") or pos.get("symbol")
                    qty = int(pos.get("quantity", 0))
                    if tkr and qty != 0:
                        self.positions[tkr] = {
                            "quantity": qty,
                            "avg_buy_price": float(pos.get("avg_buy_price", pos.get("average_price", 0.0))),
                            "is_short": bool(pos.get("is_short", qty < 0))
                        }
        except Exception as e:
            logger.warning(f"Portfolio sync warning: {e}")

    def compute_advanced_indicators(self, ticker):
        bars = list(self.candles[ticker])
        closes = [b["close"] for b in bars]
        volumes = [b["volume"] for b in bars]
        highs = [b["high"] for b in bars]
        lows = [b["low"] for b in bars]
        price = closes[-1]

        # 1. RSI (14)
        recent_15 = closes[-15:]
        gains, losses = [], []
        for i in range(1, len(recent_15)):
            delta = recent_15[i] - recent_15[i - 1]
            if delta > 0:
                gains.append(delta)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-delta)
        avg_gain, avg_loss = mean(gains), mean(losses)
        rsi = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

        # 2. Bollinger Bands (20, 2.0)
        recent_20 = closes[-20:]
        sma20 = mean(recent_20)
        std20 = std_dev(recent_20)
        upper_bb = sma20 + 2.0 * std20
        lower_bb = sma20 - 2.0 * std20
        z_score = (price - sma20) / std20 if std20 > 1e-6 else 0.0

        # 3. Volume Spike Detection
        avg_vol_20 = mean(volumes[-20:])
        volume_spike = volumes[-1] > (1.25 * avg_vol_20) if avg_vol_20 > 0 else True

        # 4. Average True Range (ATR 14) for dynamic volatility stops
        true_ranges = []
        for i in range(1, min(15, len(bars))):
            h, l, c_prev = highs[-i], lows[-i], closes[-i-1]
            tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
            true_ranges.append(tr)
        atr = mean(true_ranges) if true_ranges else price * 0.02
        atr_pct = atr / price if price > 0 else 0.02

        return {
            "price": price,
            "rsi": rsi,
            "upper_bb": upper_bb,
            "lower_bb": lower_bb,
            "z_score": z_score,
            "volume_spike": volume_spike,
            "atr_pct": atr_pct
        }

    def score_news_event(self, event):
        data = event.get("data") if isinstance(event.get("data"), dict) else event
        ticker = data.get("ticker") or data.get("symbol") or event.get("ticker")

        for key in ("sentiment_score", "sentiment", "score", "impact_score"):
            val = data.get(key)
            if isinstance(val, (int, float)):
                return ticker, clip(float(val), -1.0, 1.0)

        text = " ".join(
            str(data.get(k, "")) for k in ("headline", "title", "summary", "body", "content", "message")
        ).lower()

        bullish = ["beat", "surge", "record", "profit", "upgrade", "buyback", "approval", "contract", "growth", "dividend", "outperform", "strong", "launch"]
        bearish = ["miss", "plunge", "loss", "downgrade", "lawsuit", "investigation", "recall", "fraud", "default", "cut", "delay", "weak", "bankruptcy", "penalty"]

        score = 0.0
        for w in bullish:
            if w in text: score += 0.35
        for w in bearish:
            if w in text: score -= 0.35

        if not ticker and self.tradable_tickers:
            for t in self.tradable_tickers:
                if t.lower() in text:
                    ticker = t
                    break

        return ticker, clip(score, -1.0, 1.0)

    def evaluate_tick(self, ticker, simulated_date):
        if len(self.candles[ticker]) < 25:
            return

        if simulated_date and self.last_trade_date.get(ticker) == simulated_date:
            return

        ind = self.compute_advanced_indicators(ticker)
        price = ind["price"]
        rsi = ind["rsi"]
        z_score = ind["z_score"]
        vol_spike = ind["volume_spike"]
        atr_pct = ind["atr_pct"]
        news_score = self.sentiment[ticker]

        pos = self.positions.get(ticker)
        qty_held = pos["quantity"] if pos else 0
        avg_price = pos["avg_buy_price"] if pos else 0.0

        # --- A. RISK MANAGEMENT FOR OPEN POSITIONS ---
        if qty_held > 0:
            pnl_pct = (price - avg_price) / avg_price if avg_price > 0 else 0.0
            # Dynamic stop-loss based on volatility (1.5x ATR) or take-profit (3.0x ATR / +4.5%)
            stop_loss_threshold = -max(0.02, atr_pct * 1.5)
            take_profit_threshold = max(0.045, atr_pct * 3.0)

            if (
                pnl_pct >= take_profit_threshold
                or pnl_pct <= stop_loss_threshold
                or (rsi > 72 and pnl_pct > 0.015)
                or news_score <= -0.4
            ):
                self.submit_order(ticker, "SELL", qty_held, simulated_date, reason=f"Managed Long Exit (PnL={pnl_pct*100:.2f}%, RSI={rsi:.1f})")
            return

        if qty_held < 0:
            short_qty = abs(qty_held)
            pnl_pct = (avg_price - price) / avg_price if avg_price > 0 else 0.0
            stop_loss_threshold = -max(0.02, atr_pct * 1.5)
            take_profit_threshold = max(0.045, atr_pct * 3.0)

            if (
                pnl_pct >= take_profit_threshold
                or pnl_pct <= stop_loss_threshold
                or (rsi < 28 and pnl_pct > 0.015)
                or news_score >= 0.4
            ):
                self.submit_order(ticker, "COVER", short_qty, simulated_date, reason=f"Managed Short Cover (PnL={pnl_pct*100:.2f}%, RSI={rsi:.1f})")
            return

        # --- B. HIGH-ACCURACY ENTRY FILTERS (WHEN FLAT) ---
        if len(self.positions) >= 6 or self.cash < 200000:
            return  # Keep robust cash buffer

        trade_budget = min(self.cash * 0.10, 100000.0)
        shares = int(trade_budget // price)
        if shares <= 0:
            return

        # Adjust RSI boundaries if high-conviction news supports it
        rsi_buy_limit = 35 if news_score >= 0.3 else 30
        rsi_sell_limit = 65 if news_score <= -0.3 else 70

        # HIGH-CONVICTION BUY: Volume spike + Oversold/Bollinger + Non-negative news
        if (rsi < rsi_buy_limit and z_score <= -1.5 and vol_spike and news_score >= -0.2):
            self.submit_order(ticker, "BUY", shares, simulated_date, reason=f"High-Accuracy Buy (RSI={rsi:.1f}, Z={z_score:.2f}, VolSpike)")
            return

        # HIGH-CONVICTION SHORT: Volume spike + Overbought/Bollinger + Non-positive news
        if (rsi > rsi_sell_limit and z_score >= 1.5 and vol_spike and news_score <= 0.2):
            self.submit_order(ticker, "SHORT", shares, simulated_date, reason=f"High-Accuracy Short (RSI={rsi:.1f}, Z={z_score:.2f}, VolSpike)")
            return

        self.sentiment[ticker] *= 0.7

    def submit_order(self, ticker, action, quantity, simulated_date, reason=""):
        payload = {
            "ticker": ticker,
            "action": action,
            "quantity": int(quantity)
        }
        if simulated_date:
            payload["date"] = str(simulated_date)

        try:
            resp = session.post(f"{GATEWAY}/api/v1/orders", json=payload, timeout=2.5)
            if resp.status_code in (200, 201):
                self.last_trade_date[ticker] = simulated_date
                logger.info(f"OPTIMAL ORDER [{simulated_date}] {action} {quantity}x {ticker} | {reason} | Resp: {resp.text}")
                
                last_px = self.candles[ticker][-1]["close"]
                if action == "BUY":
                    self.positions[ticker] = {"quantity": quantity, "avg_buy_price": last_px, "is_short": False}
                    self.cash -= quantity * last_px
                elif action == "SHORT":
                    self.positions[ticker] = {"quantity": -quantity, "avg_buy_price": last_px, "is_short": True}
                elif action in ("SELL", "COVER"):
                    self.positions.pop(ticker, None)

                self.sync_portfolio()
            else:
                logger.warning(f"ORDER REJECTED [{simulated_date}] {action} {quantity}x {ticker} -> {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Order error for {ticker}: {e}")


def main():
    bot = OptimalTradingBot()
    bot.initialize_universe_and_history()

    consumer = KafkaConsumer(
        "market.ticks",
        "market.news",
        bootstrap_servers=[BROKER],
        group_id=f"group-{USERNAME}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda msg: json.loads(msg.decode("utf-8")),
    )
    logger.info(f"Optimal Trading Bot active on Kafka broker {BROKER}...")

    for message in consumer:
        try:
            event = message.value
            if not isinstance(event, dict):
                continue

            event_type = event.get("event_type") or event.get("event")

            if event_type == "preflight_tick" or "seq" in event:
                if not bot.tradable_tickers:
                    bot.initialize_universe_and_history()
                seq_num = event.get("seq")
                if seq_num is not None:
                    session.post(f"{GATEWAY}/api/v1/bot/heartbeat", json={"seq": seq_num}, timeout=2)
                continue

            if message.topic == "market.news" or event_type in ("news", "market_news"):
                ticker, score = bot.score_news_event(event)
                if ticker and (not bot.tradable_tickers or ticker in bot.tradable_tickers):
                    bot.sentiment[ticker] = score
                    logger.info(f"NEWS CATALYST [{event.get('simulated_date')}] {ticker} -> Score: {score:+.2f}")
                continue

            if message.topic == "market.ticks":
                tick_data = event.get("data") if isinstance(event.get("data"), dict) else event
                ticker = tick_data.get("ticker") or tick_data.get("symbol")
                simulated_date = event.get("simulated_date") or tick_data.get("date")

                if not ticker:
                    continue
                if not bot.tradable_tickers:
                    bot.initialize_universe_and_history()
                if bot.tradable_tickers and ticker not in bot.tradable_tickers:
                    continue

                close_px = tick_data.get("close") or tick_data.get("ltp") or tick_data.get("price")
                if close_px is None:
                    continue
                close_px = float(close_px)

                bot.candles[ticker].append({
                    "open": float(tick_data.get("open", close_px)),
                    "high": float(tick_data.get("high", close_px)),
                    "low": float(tick_data.get("low", close_px)),
                    "close": close_px,
                    "volume": float(tick_data.get("volume", 1000))
                })

                bot.evaluate_tick(ticker, simulated_date)

        except Exception as e:
            logger.error(f"Stream processing error: {e}")


if __name__ == "__main__":
    main()