**Bank CSV Normalizer**

A modular, extensible, and fully automated system for normalizing, validating, and processing bank CSV files.  
Originally designed for TrueNAS, but compatible with any Linux environment.

---

## 🚀 Overview

This project ingests incoming CSV files from various banks, validates them, converts them into a unified model, and processes them through a Python engine.

A Bash script orchestrates the workflow:

- Detects incoming CSV files  
- Manages temporary lockfiles  
- Creates timestamped log files  
- Invokes the Python engine  
- Moves failed files to `failed/`  

---

## 📁 Project Structure

bank-csv-normalizer/  
├── engine/  
│   └── process_csv.py  
├── core/  
│   └── csv_model.py  
├── data/  
│   ├── incoming/  
│   ├── failed/  
│   └── logs/  
├── bank-csv-normalizer.bash  
└── .vscode/  
    └── sftp.json  

---

## ⚙️ How It Works

1. The Bash script runs periodically (cron, systemd timer, or manually).  
2. It scans `data/incoming/` for new CSV files.  
3. For each file:  
   - A timestamped log file is created  
   - A temporary lockfile prevents double processing  
   - The Python engine processes the file  
4. On errors:  
   - The file is moved to `data/failed/`  
   - The error is logged  

---

## 🧠 Python Engine

The engine:

- Reads raw CSV files  
- Validates required columns  
- Normalizes values  
- Converts everything into a unified data model (`csv_model.py`)  
- Prepares the result for further processing or export  

---

## 🔧 Installation

### Requirements

- Python 3.10+  
- Bash  
- TrueNAS or any Linux environment  
- SFTP access (for VS Code auto‑sync)

### Setup

git clone git@github.com:<your-account>/bank-csv-normalizer.git  
cd bank-csv-normalizer  

(Optional) install Python dependencies:

pip install -r requirements.txt  

---

## ▶️ Running the Script

Manual:

./bank-csv-normalizer.bash  

Cron example (every 5 minutes):

*/5 * * * * /path/to/bank-csv-normalizer.bash  

---

## 🔄 VS Code SFTP Sync

This project includes `.vscode/sftp.json` for automatic upload on save.

Example:

{
  "host": "truenas-master",
  "username": "masta",
  "privateKeyPath": "C:/Users/m4st4/.ssh/id_ed25519",
  "remotePath": "/mnt/ssdmaster-pool/encrypted-ds/app-ds/bank-csv-normalizer",
  "uploadOnSave": true
}

---

## 📜 License

Private project — no public license.
