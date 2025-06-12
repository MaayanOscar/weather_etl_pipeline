import os
import webbrowser
import streamlit as st
from datetime import datetime
from upload_to_aws import dashboard_to_s3


def show_dashboard(final_dfs, summary_text, config):
    todays_date = datetime.today().strftime('%d-%m-%Y')

    # Streamlit UI
    st.set_page_config(page_title="Weather Dashboard", layout="wide")
    st.title("Daily Weather Dashboard")
    st.markdown(f"#### Date: {todays_date}")
    st.markdown(f"### Summary:\n{summary_text}")

    for city, df in final_dfs.items():
        pdf = df.toPandas()
        st.subheader(f"{city.capitalize()}")
        st.dataframe(pdf)

    html_path = os.path.join(os.path.dirname(__file__), os.path.join(config['local_save']['folder_name'], "weather_dashboard.html"))

    unique_cities = list(final_dfs.values())[0].toPandas()['city'].unique()
    city_options_html = "<option value='all'>All cities</option>" + ''.join([f"<option value='{city}'>{city.capitalize()}</option>" for city in unique_cities])

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
            hr {{
                margin-bottom: 5px;
            }}
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
            th {{
                background-color: #f0f8ff;
            }}
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
                outline: none;
                box-shadow: none;
            }}
            /* הסרת כל קווי מתאר וצללים בזמן פוקוס */
            *:focus {{
                outline: none !important;
                box-shadow: none !important;
            }}
        </style>
        <script>
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

    for city, df in final_dfs.items():
        pdf = df.toPandas()
        html_content += f"<div class='city-table'><h3>{city.capitalize()}</h3>"
        html_content += pdf.rename(columns=lambda x: x.replace('_', ' ')).to_html(index=False, border=0)
        html_content += "</div><br>"

    html_content += "</body></html>"

    # Save HTML
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # Upload to S3
    dashboard_to_s3(html_path, config['s3']['bucket_name'], f"{config['s3']['summaries_prefix']}/{todays_date}/{os.path.basename(html_path)}")

    # Open locally
    webbrowser.open(f"file://{os.path.abspath(html_path)}")
