import os

def detect_files(folder_path: str):
    files = {
        "energy_manager": None,
        "charger_app": None,
        "meter": None,
        "pcap": []
    }

    for root, _, filenames in os.walk(folder_path):
        for f in filenames:
            path = os.path.join(root, f)

            if "EnergyManager" in f:
                files["energy_manager"] = path
            elif "ChargerApp" in f:
                files["charger_app"] = path
            elif "meter" in f:
                files["meter"] = path
            elif f.endswith(".pcap"):
                files["pcap"].append(path)

    return files