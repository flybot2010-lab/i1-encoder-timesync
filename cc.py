import sqlite3
import requests
import json

OUTPUT_FILE = "temp/cc.py"
DB_FILE = "LFRecord.db"
api_key = "e1f10a1e78da46f5b10a1e78da96f525"

wind_direction_codes = {
    'Calm': 0,
    'N': 1,
    'NNE': 2,
    'NE': 3,
    'ENE': 4,
    'E': 5,
    'ESE': 6,
    'SE': 7,
    'SSE': 8,
    'S': 9,
    'SSW': 10,
    'SW': 11,
    'WSW': 12,
    'W': 13,
    'WNW': 14,
    'NW': 15,
    'NNW': 16,
    'Var': 17
}

def load_config():
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading config.json: {e}")
        return None

def fetch_tecci_coordinates(tecci_locations):
    coords = []
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        for tecci_id in tecci_locations:
            cursor.execute("SELECT primTecci, lat, long, cntyId FROM LFRecord WHERE primTecci = ?", (tecci_id,))
            record = cursor.fetchone()
            if record:
                coords.append(record)
            else:
                print(f"Tecci ID {tecci_id} not found in LFRecord DB.")
        conn.close()
        return coords
    except Exception as e:
        print(f"Error querying LFRecord DB: {e}")
        return []

def fetch_twc_api(latitude, longitude, api_key):
    try:
        url = f"https://api.weather.com/v1/geocode/{latitude}/{longitude}/observations/current.json?language=en-US&units=e&apiKey={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching TWC API for {latitude}, {longitude}: {e}")
        return None

def write_conditions_to_file(tecci_locations, api_key):
    with open(OUTPUT_FILE, "w") as f:
        for tecci_id, latitude, longitude, county in tecci_locations:
            twc_data = fetch_twc_api(latitude, longitude, api_key)
            if not twc_data or "observation" not in twc_data:
                continue

            obs = twc_data["observation"]

            sky_condition = obs.get("icon_extd", 3200)
            temp = obs.get("imperial", {}).get("temp", 67)
            humidity = obs.get("humidity", 67)
            dewpoint = obs.get("imperial", {}).get("dewpt", 51)
            altimeter = obs.get("imperial", {}).get("altimeter", 29.67)
            visibility = obs.get("imperial", {}).get("vis", 10.0)
            wdir_cardinal = obs.get("wdir_cardinal", "Calm")
            wind_direction = wind_direction_codes.get(wdir_cardinal, 0)
            wind_speed = obs.get("imperial", {}).get("wspd", 0)
            wind_chill = obs.get("imperial", {}).get("wc", temp)
            pressure_tendency = obs.get("ptend", 2)

            f.write("import twccommon\n\n")
            f.write(f"areaList = wxdata.getUGCInterestList('{county}', 'county')\n\n")
            f.write("twccommon.Log.info(\"i1DT - Thanks for using the 45 Degrees I1 Encoder.\")\n\n")
            f.write("if (not areaList):\n")
            f.write("    abortMsg()\n\n")
            f.write("for area in areaList:\n")
            f.write("    b = twc.Data()\n")
            f.write(f"    b.skyCondition = {sky_condition}\n")
            f.write(f"    b.temp = {temp}\n")
            f.write(f"    b.humidity = {humidity}\n")
            f.write(f"    b.dewpoint = {dewpoint}\n")
            f.write(f"    b.altimeter = {altimeter}\n")
            f.write(f"    b.visibility = {visibility}\n")
            f.write(f"    b.windDirection = {wind_direction}\n")
            f.write(f"    b.windSpeed = {wind_speed}\n")
            f.write(f"    b.windChill = {wind_chill}\n")
            f.write(f"    b.pressureTendency = {pressure_tendency}\n\n")
            f.write(f"    wxdata.setData('{tecci_id}', 'obs', b, 1749400257)\n")
            f.write(f"    twccommon.Log.info(\"i1DG - Current Conditions data set for \" + area)\n\n")

    print(f"Weather conditions written to {OUTPUT_FILE}")

def main():
    config = load_config()
    if not config:
        return

    tecci_ids = config.get("tecci", {}).get("locations", [])
    if not tecci_ids:
        print("No tecci locations found in config.json.")
        return

    tecci_locations = fetch_tecci_coordinates(tecci_ids)
    if not tecci_locations:
        print("No matching tecci locations found in LFRecord DB.")
        return

    write_conditions_to_file(tecci_locations, api_key)

if __name__ == "__main__":
    main()
