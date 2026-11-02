# Streaming Pipeline - Real-time Event Processing

Simulasi sistem streaming data untuk memproses event e-commerce secara real-time. Project ini mendemonstrasikan bagaimana producer-consumer pattern bekerja dengan threading untuk handle data streaming.

## Apa yang Dilakukan Project Ini?

Project ini mensimulasikan:
1. **Toko online** yang generate transaksi terus-menerus (order placed, payment confirmed, shipped, delivered)
2. **System analytics** yang aggregate data secara real-time
3. **Anomaly detection** untuk deteksi kota dengan revenue tinggi
4. **Dashboard sederhana** untuk lihat hasil analisis

## Cara Pakai

### 1. Pastikan Python Terinstall

Cek versi Python (minimal 3.8):
```bash
python --version
```

### 2. Clone atau Download Project

```bash
git clone <repo-url>
cd stream_pipeline
```

### 3. (Optional) Install Library untuk Grafik Lebih Bagus

```bash
pip install plotext
```

Kalau skip step ini juga gpp, visualisasi tetap jalan tapi text-based aja.

### 4. Jalankan Script

```bash
python stream_pipeline.py
```

Tunggu 30 detik, script akan:
- Generate 20 events per detik
- Aggregate data setiap 3 detik
- Tampilkan summary di akhir

## Cara Ubah Konfigurasi

Buka file `stream_pipeline.py`, cari class `Config` di bagian atas:

```python
class Config:
    DEFAULT_DURATION = 30              # berapa lama program jalan (detik)
    DEFAULT_EVENTS_PER_SEC = 20        # berapa event per detik di-generate
    WINDOW_SECONDS = 3                 # setiap berapa detik aggregate data
    ANOMALY_THRESHOLD = 300000         # batas revenue dianggap anomali (Rp300rb)
```

Ubah angka sesuai kebutuhan, save, terus jalankan ulang.

## Output yang Dihasilkan

Setelah jalan, kamu akan lihat:

### 1. Log Real-time
```
2026-11-02 12:00:01 - INFO - Producer started: 20 events/sec for 30s
2026-11-02 12:00:04 - INFO - Window: 60 events (20.0 events/sec) | Total revenue: Rp15.234.567
```

### 2. Summary Metrics
- Total event yang diproses
- Total revenue (format Rupiah)
- Rata-rata events per detik
- Kota dengan revenue tertinggi
- Event type paling sering muncul

### 3. Event Distribution
Breakdown berapa kali tiap event terjadi (order_placed, payment_confirmed, dll) dengan bar chart sederhana

### 4. Revenue by City
Detail revenue per kota (Jakarta, Bandung, Surabaya, Medan, Bali) plus rata-rata nilai order

### 5. Anomaly Detection
Kota mana aja yang punya total revenue di atas threshold

## Cara Kerja Technical

```
Producer Thread                Consumer Thread
     │                              │
     ├─ Generate event ────────────>│
     │  (order info)                │
     │                              ├─ Aggregate
     │                              │  (hitung total, avg, dll)
     │                              │
     └─ [Queue 10k size] ──────────>└─ Simpan ke state
                                          │
                                          └─ Analytics & Report
```

**Producer** = thread yang generate event terus-menerus
**Queue** = antrian untuk simpan event sementara (thread-safe)
**Consumer** = thread yang ambil event dari queue dan proses
**State** = tempat simpan hasil agregasi

## Data yang Di-generate

**Event Types:**
- `order_placed` - order baru masuk
- `payment_confirmed` - pembayaran berhasil
- `order_shipped` - barang dikirim
- `order_delivered` - barang sampai

**Cities:** Jakarta, Bandung, Surabaya, Medan, Bali

**Order Value:** Random antara Rp10.000 - Rp500.000

## Troubleshooting

**Program error atau crash?**
- Pastikan Python 3.8+
- Coba jalankan ulang

**Mau stop program di tengah jalan?**
- Tekan `Ctrl+C`

**Mau data lebih banyak/sedikit?**
- Ubah `DEFAULT_DURATION` dan `DEFAULT_EVENTS_PER_SEC` di Config

## Use Case

Project ini cocok buat:
- Belajar streaming data processing
- Demo producer-consumer pattern
- Contoh multi-threading di Python
- Base code untuk real streaming system (Kafka, Spark Streaming, dll)
