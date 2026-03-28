import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

# load local csv
df = pd.read_csv("combined_data_clean.csv")

# drop nulls
df = df.dropna()

# features
features = [
    "Open", "High", "Low", "Close", "Volume",
    "daily_return_pct",
    "daily_exposure_count", "daily_avg_tone",
    "day_of_week"
]

results = {}

for ticker in df["ticker"].unique():
    print(f"\nTraining for {ticker}")

    sub_df = df[df["ticker"] == ticker].sort_values("event_date")

    X = sub_df[features]
    y = sub_df["next_day_close"]

    split = int(len(sub_df) * 0.8)
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    model = RandomForestRegressor()
    model.fit(X_train, y_train)

    # model prediction
    y_pred = model.predict(X_test)

    # baseline
    baseline_pred = X_test["Close"]

    # evaluate
    mae = mean_absolute_error(y_test, y_pred)
    baseline_mae = mean_absolute_error(y_test, baseline_pred)

    print(f"{ticker} Model MAE:", mae)
    print(f"{ticker} Baseline MAE:", baseline_mae)

    results[ticker] = {
    "model_mae": mae,
    "baseline_mae": baseline_mae
}
print("\nFinal Results:", results)