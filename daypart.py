import sqlite3
import requests
import json
import time

OUTPUT_FILE = "temp/daypart.py"
DB_FILE = "LFRecord.db"
api_key = "e1f10a1e78da46f5b10a1e78da96f525"

def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

def fetch_tecci_coordinates(tecci_locations):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    coords = []
    for tecci_id in tecci_locations:
        cursor.execute("SELECT coopId, lat, long, cntyId FROM LFRecord WHERE coopId = ?", (tecci_id,))
        record = cursor.fetchone()
        if record:
            coords.append(record)
        else:
            print(f"TECCI ID {tecci_id} not found in DB.")
    conn.close()
    return coords

def fetch_twc_daily_api(lat, lon, api_key):
    url = f"https://api.weather.com/v3/wx/forecast/daily/10day?geocode={lat},{lon}&format=json&units=e&language=en-US&apiKey={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def write_daypart_forecast_file(tecci_locations, api_key):
    with open(OUTPUT_FILE, "w") as f:
        f.write("import twccommon\n")
        f.write("import time\n\n")

        f.write("Y, M, D, h, m, s, wd, jd, dst = time.localtime(time.time())\n")
        f.write("if h < 16:\n    dOffset = 0\nelse:\n    dOffset = 1\n")
        f.write("keyTime = time.mktime((Y, M, D + dOffset, 5, 0, 0, 0, 0, -1))\n\n")

        num_dayparts = 14

        for tecci_id, lat, lon, county in tecci_locations:
            data = fetch_twc_daily_api(lat, lon, api_key)
            dayparts = data.get("daypart", {})[0].get("daypartName", [])
            phrases = data.get("daypart", {})[0].get("narrative", [])
            icons = data.get("daypart", {})[0].get("iconCodeExtend", [])
            temps = data.get("daypart", {})[0].get("temperature", [])
            
            f.write(f"areaList = wxdata.getUGCInterestList('{county}', 'county')\n\n")
            f.write("twccommon.Log.info(\"i1DT - Thanks for using the 45 Degrees I1 Encoder.\")\n")
            f.write("if not areaList:\n    abortMsg()\n\n")

            for dp_index in range(len(phrases)):
                if phrases[dp_index] is None:
                    continue

                forecast_time = f"keyTime + ({dp_index} * 6 * 3600)"
                f.write("for area in areaList:\n")
                f.write(f"    validTime = int({forecast_time})\n")
                f.write(f"    b = twc.Data()\n")
                f.write(f"    b.phrase = '{phrases[dp_index]}'\n")
                icon = icons[dp_index] if dp_index < len(icons) and icons[dp_index] is not None else 3200
                temp = temps[dp_index] if dp_index < len(temps) and temps[dp_index] is not None else 70
                f.write(f"    b.skyCondition = {icon}\n")
                f.write(f"    b.temp = {temp}\n")
                f.write(f"    wxdata.setDaypartData(\n")
                f.write(f"        loc='{tecci_id}',\n")
                f.write(f"        type='textFcst',\n")
                f.write(f"        data=b,\n")
                f.write(f"        validTime=validTime,\n")
                f.write(f"        numDayparts={num_dayparts},\n")
                f.write(f"        expiration=validTime + 86400\n")
                f.write(f"    )\n")
                f.write(f"    twccommon.Log.info(\"i1DG - Daypart forecast set for \" + area + \" at \" + str(validTime))\n\n")

    print(f"Daypart forecast written to {OUTPUT_FILE}")

def main():
    config = load_config()
    tecci_ids = config.get("coop", {}).get("locations", [])
    tecci_locations = fetch_tecci_coordinates(tecci_ids)
    write_daypart_forecast_file(tecci_locations, api_key)

if __name__ == "__main__":
    main()
