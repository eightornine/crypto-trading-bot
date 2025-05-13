import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

# Simulated dataset based on logs (augmented with sentiment)
data = {
    "initial_buy_sol": [2.0, 3.0, 1.3, 2.5, 1.46627566, 2.0, 2.7, 1.8, 1.1, 0.02, 0.1, 0.0, 0.0001, 0.0, 0.3],
    "liquidity_sol": [31.99, 32.99, 31.29, 32.49, 31.46, 31.99, 32.69, 31.79, 31.09, 30.01, 30.09, 30.0, 30.0, 30.0, 30.29],
    "market_cap_sol": [31.81, 33.83, 30.43, 32.81, 30.75, 31.81, 33.21, 31.41, 30.04, 27.99, 28.14, 27.95, 27.95, 27.95, 28.52],
    "price_change_5min": [0.1, 0.2, 0.15, 0.3, 0.05, 0.25, 0.4, 0.1, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "sentiment_score": [0.8, 0.9, 0.6, 0.7, 0.5, 0.85, 0.95, 0.65, 0.75, 0.1, 0.2, 0.0, 0.0, 0.0, 0.3],  # Simulated sentiment (0 to 1)
    "success_label": [1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0]  # Simulated (1 if token increased 100% in 24h)
}

# Create DataFrame
df = pd.DataFrame(data)

# Features and target
X = df[["initial_buy_sol", "liquidity_sol", "market_cap_sol", "price_change_5min", "sentiment_score"]]
y = df["success_label"]

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train XGBoost model
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42,
    eval_metric="logloss"
)
model.fit(X_train, y_train)

# Evaluate model
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"Model Accuracy: {accuracy:.2f}")

# Save the model
joblib.dump(model, "ml_model.pkl")
print("Model saved as ml_model.pkl")
