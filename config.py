import json
import os

print("⚡ VELOCITY SYNC CONFIG WIZARD ⚡\n")

print("How to find your IP address:")
print("👉 Android / Linux: run `ifconfig` or `ip a`")
print("👉 Windows: run `ipconfig`\n")

sync_root = input("Enter Sync Folder Path (example: /storage/emulated/0/V-S): ").strip()
my_ip = input("Enter YOUR device IP (example: 192.168.1.10): ").strip()
peer_ip = input("Enter PEER device IP (example: 192.168.1.20): ").strip()
port = input("Enter PORT (default 7777): ").strip()

if not port:
    port = "7777"

config = {
    "SYNC_ROOT": sync_root,
    "MY_IP": my_ip,
    "PEER": f"http://{peer_ip}:{port}",
    "PORT": int(port),
    "CHUNK_SIZE": 1024 * 512,
    "TIMEOUT": 10,
    "PEER_TIMEOUT": 10
}

with open("config.json", "w") as f:
    json.dump(config, f, indent=4)

print("\n✅ Config saved to config.json")
print("Now run: python main.py")