from fastapi import FastAPI
import pandas as pd
import os

app = FastAPI()

DATA_PATH = "/app/data"

# -----------------------------
# Health
# -----------------------------
@app.get("/")
def health():
    return {"status": "API running 🚀"}


# -----------------------------
# Metrics
# -----------------------------
@app.get("/metrics")
def get_metrics():
    path = f"{DATA_PATH}/observability"

    if not os.path.exists(path):
        return []

    df = pd.read_parquet(path)

    return df.tail(5).to_dict(orient="records")


# -----------------------------
# Vehicle Activity
# -----------------------------
@app.get("/vehicle_activity")
def get_activity():
    path = f"{DATA_PATH}/vehicle_activity"

    if not os.path.exists(path):
        return []

    df = pd.read_parquet(path)

    return df.tail(10).to_dict(orient="records")