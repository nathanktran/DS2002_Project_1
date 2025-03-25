DS2002 Data Project 1 — Housing Prices vs. Violent Crime in U.S. States

Contributors: Nathan Tran (ddz2sb)

This project displays an ETL pipeline that shows the relationship between state-level violent crime rates and median housing prices from all of 2022 to 2023.

Project Overview

Goal: Explore whether there’s a relationship between crime and housing values across all U.S. states.
Data Sources:
  - Redfin Housing Data(https://www.redfin.com/news/data-center/) (CSV file provided in DS_Project/data/data.csv)
  - FBI Crime Data API(https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi)(base URL: api.usa.gov/crime/fbi/cde/)

Features:
- Extracts housing data from a local CSV file
- Gathers violent crime statistics from the FBI Crime Data API
- Transforms and merges datasets
- Calculates:
  - Average median housing price over the timespan (2022-2023 in this case)
  - Total violent crimes
  - Crime rate per 100,000 people
  - State population estimate
  - Crime & housing price state rankings
  - Ratio of crime and price
  - Correlation coefficient of Crime Rate and Median Housing Price
- Outputs results to:
  - SQLite database (output/final_data.db)
  - CSV or JSON files (optional, output folder includes these files already generated as well)
  - Scatter plot image and correlation number (output/crime_vs_price_scatter.png)

Running the ETL Script

Prerequisites:

- Python 3.7+
- Install required packages:

```bash
pip install pandas requests matplotlib
```

Runing the Script

```bash
python etl.py --apikey YOUR_API_KEY --format sqlite
```

Arguments

- `--apikey`: FBI Crime Data API key (get one at https://api.data.gov/signup/)
- `--format`: Output format: `sqlite` (default), `csv`, or `json`
