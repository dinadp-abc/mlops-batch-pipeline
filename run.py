print("DEBUG: Script started")
import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys

def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def write_error(output, version, message):
    error_output = {
        "version": version if version else "unknown",
        "status": "error",
        "error_message": message
    }
    with open(output, "w") as f:
        json.dump(error_output, f, indent=2)
    print(json.dumps(error_output, indent=2))
    sys.exit(1)

def main(args):
    start_time = time.time()

    setup_logger(args.log_file)
    logging.info("Job started")

    version = None

    try:
        # 1️⃣ Load config
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        required_keys = ["seed", "window", "version"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing config key: {key}")

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # 2️⃣ Load dataset
        df = pd.read_csv(args.input)

        if df.empty:
            raise ValueError("CSV file is empty")

        if "close" not in df.columns:
            raise ValueError("Missing 'close' column")

        logging.info(f"Rows loaded: {len(df)}")

        # 3️⃣ Rolling mean
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        # 4️⃣ Signal (ignore NaN rows)
        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
        df = df.dropna()

        # 5️⃣ Metrics
        rows_processed = len(df)
        signal_rate = df["signal"].mean()
        latency_ms = int((time.time() - start_time) * 1000)

        result = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(float(signal_rate), 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)

        logging.info(f"Metrics: {result}")
        logging.info("Job completed successfully")

        print(json.dumps(result, indent=2))
        sys.exit(0)

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        write_error(args.output, version, str(e))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()
    main(args)
    print("DEBUG: Script finished")