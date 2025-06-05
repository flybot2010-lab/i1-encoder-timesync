const fs = require("fs");
const path = require("path");
const { Client } = require("ssh2");
const SFTPClient = require("ssh2-sftp-client");

const configuration = JSON.parse(fs.readFileSync(path.join(__dirname, "config.json"), "utf8"));
if (!configuration.ssh || !configuration.ssh.hostname || !configuration.ssh.port || !configuration.ssh.username || !configuration.ssh.password) {
    console.error("i1DT - SSH configuration is missing in config.json");
    process.exit(1);
}
const { hostname, port, username, password } = configuration.ssh;
    
const sshConfig = {
    host: hostname,
    port: port,
    username: username,
    password: password,
    algorithms: {
        kex: ["diffie-hellman-group-exchange-sha1", "diffie-hellman-group1-sha1"],
        serverHostKey: ["ssh-rsa", "ssh-dss"],
        cipher: ["aes256-cbc", "aes192-cbc", "aes128-cbc", "3des-cbc"],
    },
};

const conn = new Client();
let shellStream = null;
let sshConnected = false;

if (!fs.existsSync(path.join(__dirname, "temp"))) {
    fs.mkdirSync(path.join(__dirname, "temp"));
}

function connectSSH() {
    conn.on("ready", () => {
        console.log("i1DT - SSH connection established.");
        conn.shell((err, stream) => {
            if (err) throw err;
            shellStream = stream;
            sshConnected = true;
            stream.on("close", () => {
                console.log("i1DT - SSH stream closed");
                sshConnected = false;
                conn.end();
            })
            .on("data", (data) => {
                if (data.toString().includes("Password:")) {
              stream.write("i1\n");
                }
            });
            stream.write("su -l dgadmin\n");
        });
    })
    .connect(sshConfig);
}

function sendCommand(command) {
    if (!sshConnected) connectSSH();
    if (!shellStream) {
        console.log("i1DT - SSH session not ready yet.");
        return;
    }
    console.log(`i1DT - Sent command: ${command}`);
    shellStream.write(command + "\n");
}

async function getConfig() {
    const sftp = new SFTPClient();
    try {
        await sftp.connect(sshConfig);
        await sftp.get("/usr/home/dgadmin/config/current/config.py", path.join(__dirname, "config.py"));
        console.log("i1DT - Config downloaded.");
        sftp.end();

        const config = fs.readFileSync(path.join(__dirname, "config.py"), "utf8");
        const locationRegex = /wxdata\.setInterestList\('coopId','1',\[(.*?)\]\)/g;
        const locations = [];
        let match;
        while ((match = locationRegex.exec(config)) !== null) {
            const locationIds = match[1].split(",").map((id) => id.trim().replace(/['"]/g, ""));
            const filteredIds = locationIds.filter((id) => id !== "");
            locations.push(...filteredIds);
        }

        const locationRegexTecci = /wxdata\.setInterestList\('obsStation','1',\[(.*?)\]\)/g;
        const locationsTecci = [];
        let matchTecci;
        while ((matchTecci = locationRegexTecci.exec(config)) !== null) {
            const locationIds = matchTecci[1].split(",").map((id) => id.trim().replace(/['"]/g, ""));
            const filteredIds = locationIds.filter((id) => id !== "");
            const filteredIdsFinal = filteredIds.filter((id) => !(id.length === 4 && (id.startsWith("K") || id.startsWith("W"))));
            locationsTecci.push(...filteredIdsFinal);
        }
        const uniqueLocations = [...new Set(locations)];
        const uniqueLocationsTecci = [...new Set(locationsTecci)];
        const configData = { ssh: configuration.ssh, coop: { locations: uniqueLocations }, tecci: { locations: uniqueLocationsTecci } };
        fs.writeFileSync(path.join(__dirname, "config.json"), JSON.stringify(configData, null, 2));
        console.log("i1DT - Config parsed and config.json written.");
        return configData;
    } catch (err) {
        console.error("i1DT - Error fetching config:", err);
    }
}

async function getData(record, locations) {
    try {
        const res = await fetch(`https://wist.minnwx.com/api/i1/${record}/${locations.join(",")}?apiKey=5da46bbb68c49e48e05dc3362ea65adf`);
        const text = await res.text();
        fs.writeFileSync(path.join(__dirname, "temp", `${record}.py`), text);

        const sftp = new SFTPClient();
        await sftp.connect(sshConfig);
        await sftp.put(path.join(__dirname, "temp", `${record}.py`), `/home/dgadmin/${record}.py`);
        console.log(`i1DT - ${record}.py uploaded.`);

        if (!sshConnected) connectSSH();
        setTimeout(() => sendCommand("su -l dgadmin"), 1000);
        setTimeout(() => sendCommand("source ~/.bash_profile"), 1500);
        setTimeout(() => sendCommand(`runomni /twc/util/loadSCMTconfig.pyc /home/dgadmin/${record}.py`), 2000);
        sftp.end();
    } catch (err) {
        console.error(`i1DT - Error processing ${record}:`, err);
    }
}

async function startSchedules() {
    connectSSH();
    const config = await getConfig();
    if (!config || !config.coop || !config.coop.locations) {
        console.error("i1DT - Failed to load config.");
        return;
    }

    getData("cc", config.tecci.locations);
    getData("hourly", config.coop.locations);
    getData("daily", config.coop.locations);
    getData("daypart", config.coop.locations)

    setInterval(() => getData("cc", config.tecci.locations), 10 * 60 * 1000);

    setInterval(() => {
        getData("hourly", config.coop.locations);
        getData("daily", config.coop.locations);
        getData("daypart", config.coop.locations)
    }, 30 * 60 * 1000);

    console.log("i1DT - Data fetch schedule started.");
}

// Start process loop
startSchedules();
