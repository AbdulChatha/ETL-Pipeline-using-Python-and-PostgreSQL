from flask import Flask, render_template_string
import folium
import pandas as pd

app = Flask(__name__)
# Fetch data from the database
import pandas as pd
from sqlalchemy import create_engine, text

DB_NAME = "Addresses"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DB_PORT = "5432"

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
query = text("""
    SELECT house_no, city, state_code, zip_code, lat, lng
    FROM addresslist;
""")

with engine.connect() as conn:
    result = conn.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=['house_no', 'city', 'state_code', 'zip_code', 'lat', 'lng'])

# Create a map centered around the average coordinates
m = folium.Map(location=[df['lat'].mean(), df['lng'].mean()], zoom_start=12)

# Add markers for each address
for _, row in df.iterrows():
    folium.Marker(
        location=[row['lat'], row['lng']],
        popup=f"{row['house_no']}, {row['city']}, {row['state_code']} {row['zip_code']}",
        icon=folium.Icon(color='blue')
    ).add_to(m)

# Save map to HTML file
map_html = 'map.html'
m.save(map_html)

@app.route('/')
def index():
    # Serve the HTML file
    with open(map_html, 'r') as file:
        map_html_content = file.read()

    return render_template_string(map_html_content)

if __name__ == '__main__':
    app.run(debug=True)