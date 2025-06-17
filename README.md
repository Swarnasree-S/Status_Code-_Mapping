# 🚀 Async InfluxDB CSV Ingestor with RxPY

This project provides an efficient way to parse large amounts of time-series data from CSV files and write them into **InfluxDB** using **RxPY for batching** and **asyncio for asynchronous writing**.

---

## 📌 Features

- ✅ Reads CSVs from a folder and skips bad or empty files
- ✅ Extracts fields: `dlid`, `did`, `f`, `ts`, `value`
- ✅ Converts specific fields into bitwise InfluxDB points using mapping (`abbreviated_datapoints.json`)
- ✅ Uses **RxPY** to buffer points in **batches of 5000**
- ✅ Writes asynchronously to InfluxDB using the `InfluxDBClientAsync`
- ✅ Automatically moves unreadable or corrupt files to a `bad-csvs` folder

---

## 🛠 Folder Structure

├── csv-source/ # Source folder for raw CSVs
│ └── bad-csvs/ # Automatically moves empty or corrupt CSVs here
├── abbreviated_datapoints.json # Bitwise mapping configuration
├── main.py # Main script to run
└── README.md # This file

---

## 🧠 How It Works

1. Scans `csv-source/` for `.csv` files.
2. For each file:
   - Validates format and contents.
   - Parses each data row and converts it to an InfluxDB `Point`.
   - If field is configured for binary decoding, it yields one point per bit.
3. Uses **RxPY** to:
   - Create an observable stream from the CSV generator.
   - Buffer it into batches of 5000 points.
   - Map each batch into an `asyncio` coroutine.
4. Batches are written concurrently to InfluxDB.

---

## 🔧 Configuration

Make sure to fill in the InfluxDB config in `main()`:

```python
async with InfluxDBClientAsync(url='', token='', org='') as client:
    ...
    await write_api.write(bucket='', record=batch)
Also configure abbreviated_datapoints.json like:

{
  "fields": [
    {
      "field_name": "DL01_INV01_status",
      "binary_splitup": {
        "0": "RUNNING",
        "1": "FAULT",
        ...
        "15": ""
      }
    }
  ]
}
🧪 Requirements
Python 3.8+

reactivex

influxdb-client[async]
pip install reactivex influxdb-client[async]
📤 Running the Script
Just place your CSVs into csv-source/, and run:
python main.py

📁 CSV Format (Expected)
ts,INV01_status,INV01_power,...
1718618000,21760,3200,...



ts: Unix timestamp in seconds (adjusted in script)

Each field: <device_id>_<field_name>

🧹 Output Behavior
✅ Valid data is written to InfluxDB.

❌ Empty or invalid files are moved to csv-source/bad-csvs/.

🤝 Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

🛡 License
This project is open-source and available under the MIT License.
