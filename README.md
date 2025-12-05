# 🚚 Last‑Mile Delivery Analytics
This project analyzes Amazon last‑mile delivery operations using a Kaggle dataset of 43,739 records. It demonstrates end‑to‑end ETL pipeline design, data cleaning, validation, and visualization of operational KPIs.

🔧 Tech Stack
- Database: PostgreSQL (deliveries schema with 20 columns)
- ETL: Python (etl_pipeline.py with pandas + psycopg2)
- Validation: pandas checks + pytest tests
- Visualization: Tableau Public dashboards

📊 Business Questions Answered
- How many deliveries were processed and what is the average delivery time?
- Which categories show the fastest vs slowest delivery times?
- How do SLA flags (delivered, late, on‑time) vary across categories?
- What is the impact of weather conditions on delivery performance?
- What is the overall data quality after cleaning and validation?

📁 Project Structure
delivery-analytics/
├── etl_pipeline.py
├── sql_queries/
│   └── analysis_queries.sql
├── data/
│   ├── deliveries_cleaned.csv
│   └── validation_report.txt
├── dashboards/
│   └── screenshots/
└── README.md



📸 Screenshots (Proof of Queries)

### Row Count Verification
(dashboards/screenshots/Row Count Verification.png)

### Category Performance (Delivery Time) 
(dashboards/screenshots/Delivery Time.png)

### - Day‑of‑Week Trends 
(dashboards/screenshots/Day‑of‑Week Trends.png)

### - Weather Impact
(dashboards/screenshots/Weather Impact.png)

### KPI Summary
(dashboards/screenshots/KPI Summary.png)



📈 Tableau Dashboard
Interactive dashboard showing category throughput, SLA performance, and weather impact:
https://public.tableau.com/views/Delivery_Analytics/DeliveryOperationsAnalytics

✅ Key Insights (from actual query outputs)
- Dataset size: 43,739 rows, 20 columns after cleaning
- Data quality: <1% missing after imputation, 0 duplicates, 100% PostgreSQL load success
- Category performance: Grocery avg delivery = 26.54 min vs Electronics = 130.84 min, Sports = 132.25 min (~5× gap)
- Weather impact: Fog and Cloudy conditions increase avg delivery time to ~136–138 min vs Sunny ~103 min
- KPI summary: Avg delivery time = 124.91 min, Avg agent rating = 4.63, Areas covered = 4

🚀 How to Run
- Clone the repo
- Set up PostgreSQL and create delivery_analytics database
- Configure .env with DB credentials
- Run etl_pipeline.py to clean and load data
- View outputs in /data and dashboards in Tableau

📜 License
MIT License

