import asyncio
import aiohttp
import pandas as pd
import numpy as np
import argparse
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
import sys
import time

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kaltim_flood"
COLLECTION_NAME = "weather_data"

# ============================================================
# 🗺️ MODIFIKASI UTAMA: KOORDINAT KALIMANTAN TIMUR
# Sebelumnya : 59 kelurahan Samarinda (koordinat simulasi random)
# Sekarang   : 10 Kabupaten/Kota Kalimantan Timur (koordinat nyata)
# ============================================================
kota_list = [
    {"id": 1,  "nama": "Samarinda",      "lat": -0.5022, "lon": 117.1536, "tipe": "Kota"},
    {"id": 2,  "nama": "Balikpapan",     "lat": -1.2379, "lon": 116.8529, "tipe": "Kota"},
    {"id": 3,  "nama": "Bontang",        "lat":  0.1333, "lon": 117.5000, "tipe": "Kota"},
    {"id": 4,  "nama": "Tenggarong",     "lat": -0.4167, "lon": 117.0000, "tipe": "Kab. Kutai Kartanegara"},
    {"id": 5,  "nama": "Sangatta",       "lat":  0.5167, "lon": 117.5833, "tipe": "Kab. Kutai Timur"},
    {"id": 6,  "nama": "Sendawar",       "lat":  0.1167, "lon": 115.5500, "tipe": "Kab. Kutai Barat"},
    {"id": 7,  "nama": "Tanjung Redeb",  "lat":  2.1500, "lon": 117.4833, "tipe": "Kab. Berau"},
    {"id": 8,  "nama": "Tanah Grogot",   "lat": -1.9167, "lon": 116.2000, "tipe": "Kab. Paser"},
    {"id": 9,  "nama": "Penajam",        "lat": -1.1667, "lon": 116.6000, "tipe": "Kab. Penajam Paser Utara"},
    {"id": 10, "nama": "Ujoh Bilang",    "lat": -0.6500, "lon": 114.8833, "tipe": "Kab. Mahakam Ulu"},
]

async def fetch_weather(session, url, kota_id, kota_nama):
    try:
        async with session.get(url) as response:
            if response.status == 429:
                wait_time = 10
                print(f"[Warning] Rate Limit (429) hit for {kota_nama}. Waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                return await fetch_weather(session, url, kota_id, kota_nama)
            
            if response.status == 400:
                return None
                
            response.raise_for_status()
            data = await response.json()
            return {"kota_id": kota_id, "kota_nama": kota_nama, "data": data}
    except Exception as e:
        print(f"Error fetching data for {kota_nama}: {e}")
        return None

def get_date_chunks(start_date, end_date, chunk_days=90):
    """Membagi rentang tanggal menjadi potongan kecil untuk menghindari load besar."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    chunks = []
    curr = start
    while curr < end:
        chunk_end = min(curr + timedelta(days=chunk_days), end)
        chunks.append((curr.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        curr = chunk_end + timedelta(days=1)
    return chunks

async def process_chunk(start_date, end_date, throttle_sec=10):
    results = []
    print(f"Processing chunk: {start_date} to {end_date}...")
    
    async with aiohttp.ClientSession() as session:
        batch_size = 5  # Lebih kecil dari asli (10) karena lokasi lebih sedikit
        for i in range(0, len(kota_list), batch_size):
            batch = kota_list[i : i + batch_size]
            tasks = []
            for kota in batch:
                # ============================================================
                # 🗺️ MODIFIKASI: lat & lon dari kota_list (bukan kelurahan_list)
                # ============================================================
                url = (
                    f"https://archive-api.open-meteo.com/v1/archive"
                    f"?latitude={kota['lat']}"
                    f"&longitude={kota['lon']}"
                    f"&start_date={start_date}"
                    f"&end_date={end_date}"
                    f"&hourly=rain,soil_moisture_0_to_7cm"
                    f"&timezone=Asia%2FSingapore"
                )
                tasks.append(fetch_weather(session, url, kota["id"], kota["nama"]))
            
            responses = await asyncio.gather(*tasks)
            
            for res in responses:
                if res and "data" in res:
                    data = res["data"]
                    elevation = data.get("elevation", 0)
                    hourly = data.get("hourly", {})
                    times = hourly.get("time", [])
                    rain = hourly.get("rain", [])
                    soil_m = hourly.get("soil_moisture_0_to_7cm", [])
                    
                    # Ambil info kota yang sesuai
                    kota_info = next(k for k in kota_list if k["id"] == res["kota_id"])
                    
                    for idx in range(len(times)):
                        results.append({
                            # ============================================================
                            # 🗺️ MODIFIKASI: field diganti dari kelurahan → kota
                            # ============================================================
                            "kota_id":          res["kota_id"],
                            "kota_nama":        res["kota_nama"],
                            "kota_tipe":        kota_info["tipe"],
                            "latitude":         kota_info["lat"],
                            "longitude":        kota_info["lon"],
                            "elevation_meters": elevation,
                            "timestamp":        times[idx],
                            "rainfall_mm":      rain[idx],
                            "soil_moisture":    soil_m[idx]
                        })
            
            if i + batch_size < len(kota_list):
                print(f"  Throttling for {throttle_sec}s between batches...")
                await asyncio.sleep(throttle_sec)
                
    return results

def save_to_mongodb(results):

    if not results:
        return

    try:
        client = MongoClient(MONGO_URI)

        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        collection.insert_many(results)

        print(f"{len(results)} data berhasil masuk MongoDB")

    except Exception as e:
        print("MongoDB Error:", e)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", help="Format YYYY-MM-DD")
    parser.add_argument("--end-date", help="Format YYYY-MM-DD")
    parser.add_argument("--initial", action="store_true", help="Fetch 5 years of history")
    parser.add_argument("--throttle", type=int, default=15, help="Throttle seconds between batches")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date

    if args.initial:
        end_dt = datetime.now() - timedelta(days=2)
        start_dt = end_dt - timedelta(days=5*365)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
        print(f"MODE INITIAL: Menarik sejarah 5 tahun ({start_date} s/d {end_date})")
        print(f"Lokasi: {len(kota_list)} Kab/Kota Kalimantan Timur")

    if not start_date or not end_date:
        print("Error: --start-date and --end-date are required if not using --initial")
        sys.exit(1)

    chunks = get_date_chunks(start_date, end_date, chunk_days=180)
    
    for c_start, c_end in chunks:
        results = await process_chunk(c_start, c_end, throttle_sec=args.throttle)
        save_to_mongodb(results)
        if len(chunks) > 1:
            print(f"Waiting 30s between time chunks...")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
