# Weather Data ETL Pipeline

A hands-on project for practicing real-world data engineering.

This project pulls weather data from an API, stores it on AWS S3, processes it with PySpark, and visualizes it using a simple dashboard.

---

## About the Project

I built this project to improve my skills with:
- Working with external APIs (OpenWeatherMap)
- Saving and organizing data in the cloud (AWS S3)
- Using PySpark for processing and transformations
- Building a basic dashboard to view the results

---

## Getting Started

### 1. Clone the repository
```
git clone https://github.com/MaayanOscar/weather_etl_pipeline
cd weather-etl-pipeline
```

### 2. Install requirements
```
pip install -r requirements.txt
```

### 3. Add your credentials
- Copy `.env.example` to `.env`
- Fill in your OpenWeatherMap API key and AWS keys

### 4. Configure cities
Open the file `utils/config.yaml` and change the list of cities to whatever you'd like.

### 5. Run the pipeline
```
main.py
```

This will download weather data, process it, and open a dashboard in your browser.

---

## What It Does

```
[API] → JSON → S3 (raw)
                ↓
            PySpark → S3 (processed)
                         ↓
                      Dashboard (Streamlit + HTML)
```

---

## Output

- Weather data in JSON format per city
- Processed version uploaded to AWS S3
- A dashboard exported as a static HTML file (opened automatically in your browser)

---

## Tech Stack

- Python
- PySpark
- Streamlit
- AWS S3 (boto3)
- OpenWeatherMap API
- YAML & dotenv

---

## Environment Variables

Stored in a `.env` file (not committed):

```
OWM_API_KEY=your_openweathermap_key
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
```

Use the `.env.example` file as a template.

---

## Author

Created by Maayan Oscar - Data Engineer
[LinkedIn](https://www.linkedin.com/in/maayan-oscar)
