import os
import boto3
import webbrowser
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime
from upload_to_aws import dashboard_to_s3


def show_dashboard(final_df, summary_text, config):
    '''
    Generates and displays a weather dashboard both in Streamlit and as an HTML file,
    then uploads the HTML dashboard to S3 and automatically opens it in the default web browser.

    :param final_df: A dictionary where keys are city names and values are Spark DataFrames.
                      containing the processed weather data per city.
    :param summary_text: weather summary text to be displayed in the dashboard.
    :param config: Configuration dictionary loaded from YAML.
    :return: None
    '''

    todays_date = datetime.today().strftime('%d-%m-%Y')

    # Streamlit UI rendering
    st.set_page_config(page_title="Weather Dashboard", layout="wide")
    st.title("Daily Weather Dashboard")
    st.markdown(f"#### Date: {todays_date}")
    st.markdown(f"### Summary:\n{summary_text}")

    for df_name, df in final_df.items():
        pdf = df.toPandas()
        st.subheader(f"{df_name.capitalize()}")
        st.dataframe(pdf)

    # create local save path
    output_folder_name = config['local_save']['output_folder_name']
    output_dir = os.path.join(os.path.dirname(__file__), output_folder_name)
    os.makedirs(output_dir, exist_ok=True)
    html_path = os.path.join(output_dir, "weather_dashboard.html")

    # Generate city dropdown options for HTML
    unique_cities = list(final_df.values())[0].toPandas()['city'].unique()
    city_options_html = "<option value='all'>All cities</option>" + ''.join([
        f"<option value='{city}'>{city.capitalize()}</option>" for city in unique_cities])

    # Build the HTML content for the dashboard
    html_content = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Weather Dashboard</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Rubik:wght@400;500&display=swap');
            body {{
                font-family: 'Rubik', sans-serif;
                background: linear-gradient(to bottom, #87ceeb, #ffffff);
                color: #333;
                padding: 40px;
                text-align: center;
            }}
            h1 {{ font-size: 36px; color: #004466; margin-bottom: 10px; }}
            h2 {{ color: #006699; margin-bottom: 20px; }}
            pre {{
                font-family: 'Rubik', sans-serif;
                font-size: 18px;
                background-color: rgba(255,255,255,0.85);
                padding: 20px;
                border-radius: 12px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                white-space: pre-wrap;
                text-align: left;
            }}
            hr {{ margin-bottom: 5px; }}
            .city-table {{
                margin-top: 20px;
                background-color: rgba(255,255,255,0.95);
                padding: 20px;
                border-radius: 12px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                padding: 8px 12px;
                border: 1px solid #ccc;
                text-align: center;
            }}
            th {{ background-color: #f0f8ff; }}
            td.city-name {{
                font-weight: bold;
                font-size: 18px;
                color: #004466;
            }}
            #city-dropdown {{
                display: block;
                margin: 5px auto;
                padding: 10px;
                font-size: 16px;
                width: 300px;
                background-color: rgba(255,255,255,0.85);
                border: 1px solid #ccc;
                border-radius: 8px;
                color: #333;
            }}
            *:focus {{
                outline: none !important;
                box-shadow: none !important;
            }}
        </style>
        <script>
            // Filter displayed rows based on selected city
            function filterCityRows() {{
                const selected = document.getElementById("city-dropdown").value.toLowerCase();
                const rows = document.querySelectorAll(".city-table table tbody tr");
                rows.forEach(row => {{
                    const cityCell = row.querySelector("td");
                    if (!cityCell) return;
                    const city = cityCell.textContent.toLowerCase();
                    row.style.display = selected === 'all' || city === selected ? "" : "none";
                }});
            }}
        </script>
    </head>
    <body>
        <h1>☀️ Daily Weather Dashboard</h1>
        <h2>📅 Date: {todays_date}</h2>
        <pre>{summary_text}</pre>
        <hr>
        <select id="city-dropdown" onchange="filterCityRows()">
            {city_options_html}
        </select>
    """

    # Add all cities table to the HTML
    for df_name, df in final_df.items():
        pdf = df.toPandas()
        html_content += f"<div class='city-table'><h3>{df_name.capitalize()}</h3>"
        html_content += pdf.rename(columns=lambda x: x.replace('_', ' ')).to_html(index=False, border=0)
        html_content += "</div><br>"

    html_content += "</body></html>"

    # Save the dashboard as HTML file
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Upload the HTML file to S3
    dashboard_to_s3(
        html_path,
        config['s3']['bucket_name'],
        f"{config['s3']['summaries_prefix']}/{todays_date}/{os.path.basename(html_path)}")

    # Open the dashboard locally in the browser
    webbrowser.open(f"file://{os.path.abspath(html_path)}")
