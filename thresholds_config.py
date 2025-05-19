# thresholds_config.py

# --- Bitcoin Fear & Greed Index (0-100) ---
# Sources suggest values above 75-80 indicate extreme greed.
# Historically, market tops have often coincided with F&G values > 80, sometimes > 90.
FG_EXTREME_GREED = 80  # Red: High Risk (Extreme Greed)
FG_GREED = 65          # Yellow: Medium Risk (Greed)
# Green: < 65 (Neutral to Fear - Lower risk for top signal)

# --- Pi Cycle Top ---
# This is primarily a crossover event: 111DMA crossing above 350DMA*2.
# The risk is highest when this cross occurs and persists.
# We'll define states rather than numerical thresholds for the value itself.
# Red: 111DMA is currently AT or ABOVE 350DMA*2.
# Yellow: 111DMA is approaching 350DMA*2 (e.g., 111DMA is > 0.95 * (350DMA*2) AND 111DMA < 350DMA*2).
# Green: Otherwise.
PI_CYCLE_APPROACH_FACTOR = 0.95 # For Yellow state

# --- Bitcoin Price vs. 200 Week Moving Average (200WMA) ---
# Risk assessed by how far price (P) is above the 200WMA. P/200WMA ratio.
# Historical tops often show significant extension. The Mayer Multiple (P/200DMA) uses >2.4 as overbought.
# For the 200WMA, multiples can be higher at cycle peaks.
WMA200_PRICE_RATIO_HIGH = 3.0    # Red: High Risk (Price is 3x or more above 200WMA)
WMA200_PRICE_RATIO_MEDIUM = 2.0  # Yellow: Medium Risk (Price is 2x to 3x above 200WMA)
# Green: < 2.0 (Lower risk based on this multiple)

# --- Google Trends for "Bitcoin" (Scale 0-100) ---
# High values indicate increased retail FOMO, often correlated with market tops.
# Peak historical interest has hit 100. Sustained high levels are a warning.
GTRENDS_HIGH_RISK = 85       # Red: High Risk (Approaching peak historical search interest)
GTRENDS_MEDIUM_RISK = 65     # Yellow: Medium Risk (Significantly elevated search interest)
# Green: < 65 (Lower retail search frenzy)
# Note: Velocity/rate of change can also be an indicator.

# --- Bitcoin Dominance (BTC.D %) ---
# Lower BTC.D can indicate altcoin season froth / late-stage bull market characteristics.
# Historically, major altseasons saw BTC.D drop below 50% and even into the 30s at extreme peaks.
DOMINANCE_FROTH_HIGH = 40.0  # Red: High Risk (BTC.D < 40%, often signals peak altcoin frenzy)
DOMINANCE_FROTH_MEDIUM = 48.0 # Yellow: Medium Risk (BTC.D < 48%, altcoins gaining significant traction)
# Green: >= 48.0 (Bitcoin relatively stronger, less overall market froth indicated by this metric)
# Note: The interpretation of BTC.D evolves with market structure (e.g., stablecoin growth, ETH's role).

# --- Stock-to-Flow (S2F) Model Price Deviation ---
# Risk when actual Bitcoin price significantly deviates above the S2F model's predicted price.
# This is often measured as a ratio: Actual Price / S2F Model Price.
S2F_PRICE_DEVIATION_HIGH = 2.5   # Red: High Risk (BTC Price is >= 2.5x S2F Model Price)
S2F_PRICE_DEVIATION_MEDIUM = 1.7 # Yellow: Medium Risk (BTC Price is 1.7x to 2.5x S2F Model Price)
# Green: < 1.7 (Price is closer to or below the model price)
# Note: Different S2F model versions exist; consistency in calculation is key.

# --- Puell Multiple (Calculated) ---
# (Daily USD Value of Issued BTC) / (365-day MA of Daily USD Value of Issued BTC)
# High values suggest miners are very profitable and may increase selling pressure.
# Historically, values above 3-4 have signaled tops, but recent cycles might see lower peaks.
PUELL_HIGH_RISK = 3.0       # Red: High Risk (Miners exceptionally profitable vs. yearly average)
PUELL_MEDIUM_RISK = 1.8     # Yellow: Medium Risk (Miners significantly profitable)
# Green: < 1.8 (Miner profitability not indicating extreme market heat)

# --- General Overall Risk Assessment ---
# Thresholds for how many individual Red/Yellow signals trigger an overall market warning.
# These are subjective and depend on how many indicators you actively use.
# Example if you have ~7 indicators:
OVERALL_HIGH_RISK_COUNT_RED = 3    # If 3 or more indicators are RED
OVERALL_MEDIUM_RISK_COUNT_RED = 2  # If 2 indicators are RED
OVERALL_MEDIUM_RISK_SUM_YELLOW_RED = 4 # Or if (RED count + YELLOW count) is 4 or more