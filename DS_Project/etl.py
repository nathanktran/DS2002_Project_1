import pandas as pd
import requests
import sqlite3
import os
import argparse
import matplotlib.pyplot as plt

# Set up directories and file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REDFIN_CSV = os.path.join(BASE_DIR, "data", "data.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
SQLITE_DB = os.path.join(OUTPUT_DIR, "final_data.db")
YEARS = [2022, 2023]

# Get violent crime data from FBI Crime Data API
def fetch_crime_data(state_name, state_abbr, api_key):
    try:
        url = f"https://api.usa.gov/crime/fbi/cde/summarized/state/{state_abbr}/V?from=01-{YEARS[0]}&to=12-{YEARS[1]}&API_KEY={api_key}"
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        # Get crime rate and population data
        offenses = data.get("offenses")
        rates = offenses.get("rates") if offenses else None
        state_rates = rates.get(state_name) if rates else None
        populations = data.get("populations", {}).get("population", {}).get(state_name)

        # Handle missing data
        if not state_rates or not populations:
            print(f"Missing data for {state_name}. Response: {data}")
            return {"Population": 0, "TotalViolentCrimes": 0, "CrimeRate": 0.0}

        # Estimate average population and total crimes
        avg_population = sum(populations.values()) / len(populations)
        total_crimes_est = sum(rate for rate in state_rates.values() if isinstance(rate, (int, float))) * avg_population / 100_000
        crime_rate = (total_crimes_est / avg_population) * 100_000

        return {
            "Population": int(round(avg_population)),
            "TotalViolentCrimes": int(round(total_crimes_est)),
            "CrimeRate": round(crime_rate, 2)
        }

    except Exception as e:
        print(f"Error fetching data for {state_name}: {e}")
        return {"Population": 0, "TotalViolentCrimes": 0, "CrimeRate": 0.0}

# Print summary statistics
def summarize(df, label):
    print(f"--- {label} ---")
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print(df.columns.tolist())

# Export DataFrame to output
def export_data(df, name, output_format):
    path = os.path.join(OUTPUT_DIR, f"{name}.{output_format}")
    if output_format == "csv":
        df.to_csv(path, index=False)
    elif output_format == "json":
        df.to_json(path, orient="records", indent=2)
    print(f"Exported {name} to {path}")

# Main ETL pipeline
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apikey", required=True, help="Your FBI Crime Data API key")
    parser.add_argument("--format", choices=["csv", "json", "sqlite"], default="sqlite", help="Output format for transformed data")
    args = parser.parse_args()

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load housing data from Redfin CSV
    df_redfin = pd.read_csv(REDFIN_CSV, encoding="utf-16", delimiter="\t")

    # Clean and filter for 2022–2023
    df_redfin = df_redfin[["Region", "Month of Period End", "Median Sale Price"]].copy()
    df_redfin = df_redfin[df_redfin["Month of Period End"].str.contains("2022|2023")]
    df_redfin["Median Sale Price"] = df_redfin["Median Sale Price"].replace("[$K]", "", regex=True)
    df_redfin["Median Sale Price"] = pd.to_numeric(df_redfin["Median Sale Price"], errors='coerce') * 1000
    df_redfin.dropna(inplace=True)

    # Calculate average median price per state
    housing_summary = df_redfin.groupby("Region")["Median Sale Price"].mean().reset_index()
    housing_summary.columns = ["State", "AvgMedianPrice"]
    summarize(housing_summary, "Housing Data")

    # Map state names to abbreviations
    state_abbr_map = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
        "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
        "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
        "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
        "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
        "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
        "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
        "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
        "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"
    }

    # Find crime data for all states
    crime_data = []
    for state, abbr in state_abbr_map.items():
        print(f"Getting data for {state}")
        crime_stats = fetch_crime_data(state, abbr, args.apikey)
        crime_data.append({"State": state, **crime_stats})

    df_crime = pd.DataFrame(crime_data)
    summarize(df_crime, "Crime Data")

    # Merge housing and crime data
    merged = pd.merge(housing_summary, df_crime, on="State", how="inner")

    # Add ranking and comparison statistics
    merged["CrimeRank"] = merged["CrimeRate"].rank(ascending=False).astype(int)
    merged["PriceRank"] = merged["AvgMedianPrice"].rank(ascending=False).astype(int)
    merged["CrimesPerDollar"] = (merged["CrimeRate"] / merged["AvgMedianPrice"]).round(6)
    summarize(merged, "Merged Data")

    # Compute correlation
    correlation = merged["CrimeRate"].corr(merged["AvgMedianPrice"])
    print(f"\nCorrelation between Crime Rate and Median Housing Price: {correlation:.3f}")

    # Create scatter plot
    plt.figure(figsize=(10, 6))
    plt.scatter(merged["CrimeRate"], merged["AvgMedianPrice"], color="blue", edgecolors="black")
    plt.title("Crime Rate vs. Median Housing Price by State")
    plt.xlabel("Violent Crime Rate per 100,000 people")
    plt.ylabel("Average Median Housing Price")
    plt.grid(True)

    # Label top 3 in each ranking
    for _, row in merged.iterrows():
        if row["CrimeRank"] <= 3 or row["PriceRank"] <= 3:
            plt.annotate(row["State"], (row["CrimeRate"], row["AvgMedianPrice"]), fontsize=8)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "crime_vs_price_scatter.png"))
    plt.show()

    # Save results
    if args.format == "sqlite":
        conn = sqlite3.connect(SQLITE_DB)
        housing_summary.to_sql("housing_data", conn, if_exists="replace", index=False)
        df_crime.to_sql("crime_data", conn, if_exists="replace", index=False)
        merged.to_sql("merged_data", conn, if_exists="replace", index=False)
        conn.close()
        print("\nData stored in SQLite database:", SQLITE_DB)
    else:
        export_data(housing_summary, "housing_data", args.format)
        export_data(df_crime, "crime_data", args.format)
        export_data(merged, "merged_data", args.format)

if __name__ == "__main__":
    main()
