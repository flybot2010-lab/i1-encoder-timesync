import paramiko
import os
import re
import json
import requests
import time
import threading

with open("config.json", "r") as f:
    ssh_config = json.load(f).get("ssh", {})

ssh_connected = False
ssh_client = None
shell = None


def ensure_temp_dir():
    if not os.path.exists("temp"):
        os.makedirs("temp")


def connect_ssh():
    global ssh_client, shell, ssh_connected
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_client.connect(
        ssh_config["hostname"],
        port=ssh_config["port"],
        username=ssh_config["username"],
        password=ssh_config["password"],
        look_for_keys=False,
        allow_agent=False
    )
    ssh_connected = True
    shell = ssh_client.invoke_shell()
    print("i1DT - SSH connection established.")

    def handle_output():
        while True:
            output = shell.recv(1024).decode()
            if "Password:" in output:
                shell.send("i1\n")

    threading.Thread(target=handle_output, daemon=True).start()
    shell.send("su -l dgadmin\n")


def send_command(command):
    if not ssh_connected:
        connect_ssh()
    print(f"i1DT - Sent command: {command}")
    shell.send(command + "\n")


def get_config():
    ensure_temp_dir()
    transport = paramiko.Transport((ssh_config["hostname"], ssh_config["port"]))
    transport.connect(username=ssh_config["username"], password=ssh_config["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    local_path = os.path.join("config.py")
    remote_path = "/usr/home/dgadmin/config/current/config.py"
    sftp.get(remote_path, local_path)
    print("i1DT - Config downloaded from i1.")
    sftp.close()
    transport.close()

    with open(local_path, "r") as f:
        config_content = f.read()

    pattern = r"wxdata\.setInterestList\('coopId','1',\[(.*?)\]\)"
    matches = re.findall(pattern, config_content)
    locations = []

    for match in matches:
        ids = [id.strip().replace("'", "").replace('"', '') for id in match.split(",") if id.strip()]
        ids = [id for id in ids if not id.startswith("K") ]
        ids = [id for id in ids if not id.startswith("W") ]
        locations.extend(ids)

    pattern_tecci = r"wxdata\.setInterestList\('obsStation','1',\[(.*?)\]\)"
    matches_tecci = re.findall(pattern_tecci, config_content)
    locations_tecci = []

    for match in matches_tecci:
        ids = [id.strip().replace("'", "").replace('"', '') for id in match.split(",") if id.strip()]
        ids = [id for id in ids if not id.startswith("K") ]
        ids = [id for id in ids if not id.startswith("W") ]
        locations_tecci.extend(ids)

    unique_locations = list(set(locations))
    unique_locations_tecci = list(set(locations_tecci))
    config_data = {"ssh": ssh_config, "coop": {"locations": unique_locations},"tecci": {"locations": unique_locations_tecci}}

    with open("config.json", "w") as f:
        json.dump(config_data, f, indent=2)

    print("i1DT - Config parsed and config.json written.")
    return config_data


def get_data(record, locations):
    ensure_temp_dir()
    try:
        url = f"https://wist.minnwx.com/api/i1/{record}/{','.join(locations)}?apiKey=5da46bbb68c49e48e05dc3362ea65adf"
        response = requests.get(url)
        local_file = os.path.join("temp", f"{record}.py")
        with open(local_file, "w") as f:
            f.write(response.text)
        print(f"i1DT - {record} data downloaded.")

        transport = paramiko.Transport((ssh_config["hostname"], ssh_config["port"]))
        transport.connect(username=ssh_config["username"], password=ssh_config["password"])
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_path = f"/home/dgadmin/{record}.py"
        sftp.put(local_file, remote_path)
        print(f"i1DT - {record}.py uploaded.")
        sftp.close()
        transport.close()

        if not ssh_connected:
            connect_ssh()

        time.sleep(1)
        send_command("su -l dgadmin")
        time.sleep(0.5)
        send_command("source ~/.bash_profile")
        time.sleep(0.5)
        send_command(f"runomni /twc/util/loadSCMTconfig.pyc /home/dgadmin/{record}.py")

    except Exception as e:
        print(f"i1DT - Error processing {record}: {e}")


def start_schedules():
    config = get_config()
    if not config or "coop" not in config or "locations" not in config["coop"]:
        print("i1DT - Failed to load i1 config.")
        return

    locations = config["coop"]["locations"]
    locationsCC = config["tecci"]["locations"]

    def run_cc():
        while True:
            get_data("cc", locationsCC)
            time.sleep(600)

    def run_hourly_daily():
        while True:
            get_data("hourly", locations)
            get_data("daily", locations)
            get_data("daypart", locations)
            time.sleep(1800)

    threading.Thread(target=run_cc, daemon=True).start()
    threading.Thread(target=run_hourly_daily, daemon=True).start()

    print("i1DT - Data fetch schedule started.")


if __name__ == "__main__":
    start_schedules()
    while True:
        time.sleep(1)
