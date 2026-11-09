# Hybrid Spark & Hadoop Dashboard

Dashboard analitik data medis berbasis **FastAPI**, **Apache Spark** dan
**Hadoop HDFS**. Aplikasi ini menyediakan antarmuka web sederhana untuk:

- memfilter data pasien kanker menggunakan Spark SQL/DataFrame;
- mencari kata kunci pada laporan patologi menggunakan Spark RDD;
- melihat preview data terstruktur dan tidak terstruktur;
- mengukur waktu eksekusi operasi analitik dari sisi API.

> **Status implementasi:** repository ini merupakan proyek demonstrasi/laboratorium.
> Mode aplikasi ditentukan langsung di `main.py` melalui `EXEC_MODE`. Agar seluruh
> endpoint dashboard berjalan, gunakan mode `spark`. Mode `hadoop` saat ini
> menginisialisasi koneksi HDFS, tetapi router analitik masih membutuhkan state
> Spark (`spark` dan `text_rdd`).

## Daftar Isi

- [Pembagian Peran ETS & Panduan Bagian Big Data Developer](#pembagian-peran-ets--panduan-bagian-big-data-developer)
- [Arsitektur](#arsitektur)
- [Teknologi](#teknologi)
- [Struktur Proyek](#struktur-proyek)
- [Dataset](#dataset)
- [Prasyarat](#prasyarat)
- [Instalasi](#instalasi)
- [Menjalankan Aplikasi](#menjalankan-aplikasi)
- [Mode Eksekusi](#mode-eksekusi)
- [Halaman Web](#halaman-web)
- [API Reference](#api-reference)
- [Contoh Penggunaan API](#contoh-penggunaan-api)
- [Docker Hadoop HDFS](#docker-hadoop-hdfs)
- [Troubleshooting](#troubleshooting)
- [Catatan Pengembangan](#catatan-pengembangan)
- [Lisensi](#lisensi)

## Pembagian Peran ETS & Panduan Bagian Big Data Developer

Berdasarkan pembagian tugas **ETS IFB-451 Big Data (Tabel 7.1)**:
- **Data Engineer (Segmen 3 — Modul M3–M4, Fokus: *Variety & Veracity*):** **SUDAH AKTIF**. Mencakup penyimpanan & pembacaan dari Hadoop HDFS/Spark, pembacaan multi-format (`patient_encounters.csv` & 5 batch *Nested JSON* `lab_results_batch_*.json`), *flattening* skema JSON, validasi & pembersihan kualitas data (*deduplication*, filter *missing/negative values*, standarisasi satuan `mmol/L` & `umol/L` ke `mg/dL`), serta integrasi relasional (`INNER JOIN` pada `encounter_id`) yang ditampilkan pada halaman `/tables`.
- **Big Data Developer (Segmen 4 — Modul M5–M6, Fokus: *Volume & Velocity*):** **DIKOMENTARI / DITANDAI DI KODE**. Bagian eksekusi operasi pemrosesan Spark SQL/RDD, optimasi komputasi, simulasi pembacaan bertahap (*batch ingestion latency*), dan pengukuran rata-rata waktu eksekusi (`Spark vs Hadoop`) ditandai dengan komentar `# === [BAGIAN BIG DATA DEVELOPER] ===` agar dikerjakan/diaktifkan oleh anggota yang bertugas sebagai **Big Data Developer**.

### Daftar File & Baris Kode Bagian **Big Data Developer**

Untuk anggota tim yang memegang peran **Big Data Developer**, berikut adalah daftar file dan bagian kode yang perlu dibuka komentarnya (*uncomment*) atau dilengkapi:

| No | File | Baris / Blok Penanda | Tugas & Fungsi (Modul M5–M6: Volume & Velocity) |
|---:|---|---|---|
| 1 | `main.py` | `# === [BAGIAN BIG DATA DEVELOPER: Modul Pengukuran Performa Komputasi] ===` (Baris 10–16 & 245) | Mengimpor `analytics_api`, menjalankan `analytics_api.initialise_metrics(app)`, dan mendaftarkan `app.include_router(analytics_api.router)`. |
| 2 | `main.py` | `# === [BAGIAN BIG DATA DEVELOPER: Pengukuran Latensi Ingestion (Velocity)] ===` (Baris 46–56) | Mengukur waktu baca (*ingestion latency*) per *batch* (`t0 = time.time()`) saat memuat 5 file JSON dari **Hadoop HDFS**. |
| 3 | `main.py` | `# === [BAGIAN BIG DATA DEVELOPER: Optimasi In-Memory Caching RDD & Pengukuran Velocity] ===` (Baris 151–165) | Mengaktifkan `.cache()` pada `text_rdd`, membaca 5 *batch* JSON secara bertahap dengan `spark.read.json(bf)` + `.unionByName()`, serta mencetak *ingestion latency* per *batch* di terminal untuk bukti **Velocity**. |
| 4 | `routers/dataframe_api.py` | `# [BAGIAN BIG DATA DEVELOPER (M5-M6)]` — Fungsi `filter_patients` (`GET /api/patients/filter`, Baris 10–77) | Menjalankan query pemrosesan relasional **Spark SQL** (`WHERE department = ... AND result_value >= ...`) di atas view `patients`, menghitung waktu eksekusi (`exec_time`), dan mencatatnya dengan `record_execution(request, "filter_data", exec_time)`. |
| 5 | `routers/rdd_api.py` | `# [BAGIAN BIG DATA DEVELOPER (M5-M6)]` — Fungsi `search_keyword` (`GET /api/pathology/search`, Baris 18–65) | Menjalankan operasi pencarian kata kunci pada 100.000 rekaman JSON menggunakan **Spark RDD** (`text_rdd.collect()`), menghitung waktu eksekusi, dan mencatatnya dengan `record_execution(request, "cari_teks", exec_time)`. |
| 6 | `routers/analytics_api.py` | `# [BAGIAN BIG DATA DEVELOPER (M5-M6)]` (Seluruh isi file, Baris 1–49) | Menyimpan riwayat waktu eksekusi di memori dan menyediakan endpoint `GET /api/analytics/execution-summary` untuk menghitung rata-rata waktu eksekusi (*average*) dan waktu terakhir (*latest*) antara **Spark vs Hadoop**. |
| 7 | `static/index.html` | `<!-- [BAGIAN BIG DATA DEVELOPER (M5-M6)] -->` (Baris 168–244 & Baris 382–385) | Menampilkan UI **Dashboard Analitik Waktu Eksekusi (Spark vs Hadoop)**, kartu **Filter Utilisasi & Hasil Lab (DataFrame SQL Join)**, kartu **Pencarian Log Instrumen & Batch JSON Lab (RDD)**, serta mengaktifkan kembali pemanggilan `loadExecutionAnalytics()`, `fetchPatients()`, dan `searchKeyword()` pada `window.onload`. |

### Checklist Demonstrasi Video untuk **Big Data Developer** (Durasi 1:15 — Segmen 4)
1. **Bukti Velocity (Terminal):** Tunjukkan log terminal saat server dinyalakan yang menampilkan waktu muat (*ingestion latency*) per *batch* (`Batch 1` s/d `Batch 5`, masing-masing 20.000 baris JSON).
2. **Bukti Kompleksitas Komputasi & Volume (Kode):** Tunjukkan perbedaan pemrosesan sederhana vs pemrosesan relasional (`JOIN` antara CSV & JSON yang dioptimasi dengan `spark.sql.shuffle.partitions = 4` dan `.cache()`).
3. **Bukti Performa Pemrosesan (Dashboard Web `/`):** Jalankan fitur **Filter Data (Spark SQL)** dan **Cari Teks (Spark RDD)** di browser, lalu tunjukkan perbandingan rata-rata waktu eksekusi (`execution_time_sec`) antara mode **Spark** dan **Hadoop**.

## Arsitektur

### Diagram komponen

```mermaid
flowchart LR
    U["Pengguna"] --> UI["Dashboard HTML CSS JavaScript"]
    UI --> API["FastAPI"]

    API --> R["Router RDD<br/>/api/pathology"]
    API --> DF["Router DataFrame<br/>/api/patients"]

    R --> RDD["Spark RDD<br/>mtsamples.csv"]
    DF --> SQL["Spark SQL DataFrame<br/>patients"]

    RDD --> P["data/mtsamples.csv"]
    SQL --> C["data/breast_cancer_data.csv"]

    API -. mode hadoop .-> HDFS["Hadoop HDFS<br/>NameNode :9870"]
    HDFS -. membaca .-> P
```

### Alur pemrosesan

```mermaid
sequenceDiagram
    participant B as Browser
    participant F as FastAPI
    participant S as Spark
    participant D as Dataset

    B->>F: GET /api/patients/filter
    F->>S: Spark SQL pada temporary view patients
    S->>D: Membaca breast_cancer_data.csv
    D-->>S: DataFrame hasil filter
    S-->>F: DataFrame
    F-->>B: JSON records + metrik waktu

    B->>F: GET /api/pathology/search
    F->>S: collect() dan pencarian keyword
    S->>D: Membaca mtsamples.csv sebagai RDD
    D-->>S: Baris teks yang cocok
    S-->>F: Maksimal 50 baris
    F-->>B: JSON hasil pencarian
```

## Analisis Arsitektur Proyek

### 1. Arsitektur Umum dan Alur Data

Struktur utama proyek adalah:

```text
.
├── data/
│   ├── breast_cancer_data.csv   # Data kanker tabular
│   └── mtsamples.csv            # Laporan patologi berbasis teks
├── routers/
│   ├── dataframe_api.py         # Endpoint DataFrame/Spark SQL
│   ├── rdd_api.py               # Endpoint RDD/pencarian teks
│   └── analytics_api.py         # Metrik waktu eksekusi
├── static/
│   ├── index.html               # Dashboard utama
│   └── tables.html              # Preview data mentah
├── main.py                      # Entry point FastAPI dan pemilih mode
├── docker-compose.yml           # Infrastruktur Hadoop HDFS
└── README.md
```

Komponen aplikasi dan aliran datanya:

```mermaid
flowchart LR
    U["Browser / Dashboard<br/>static/*.html"] -->|HTTP REST| API["FastAPI / Uvicorn<br/>main.py"]
    API --> DF["Router DataFrame<br/>/api/patients/*"]
    API --> RDD["Router RDD<br/>/api/pathology/*"]
    API --> ANA["Router Analytics<br/>/api/analytics/*"]

    DF --> CSV1["data/breast_cancer_data.csv"]
    CSV1 --> DFPROC["DataFrame / Spark SQL<br/>atau pandas"]
    RDD --> CSV2["data/mtsamples.csv"]
    CSV2 --> RDDPROC["RDD / list teks"]

    DFPROC --> OUT["JSON response"]
    RDDPROC --> OUT
    ANA --> METRICS["Execution metrics<br/>in-memory"]
    METRICS --> OUT
    OUT --> U
```

- `breast_cancer_data.csv` berisi data terstruktur dengan kolom seperti
  `id`, `diagnosis`, `radius_mean`, dan fitur tumor lainnya.
- `mtsamples.csv` berisi laporan patologi, transkripsi, spesialisasi medis,
  dan keywords.
- `main.py` membuat aplikasi FastAPI, membaca `EXEC_MODE`, lalu menginisialisasi
  Spark atau koneksi HDFS.
- `dataframe_api.py` menangani filter diagnosis/radius dan preview pasien.
- `rdd_api.py` menangani pencarian keyword dan preview baris laporan patologi.
- `analytics_api.py` menyimpan jumlah, rata-rata, dan waktu eksekusi terakhir
  secara in-memory selama proses aplikasi berjalan.

Pada mode Spark, aliran input lokalnya adalah:

```mermaid
flowchart TD
    C["data/breast_cancer_data.csv"] --> READCSV["spark.read.csv<br/>header=True, inferSchema=True"]
    READCSV --> DF["Spark DataFrame"]
    DF --> VIEW["Temporary view: patients"]
    VIEW --> SQL["Spark SQL filter / LIMIT"]
    SQL --> PANDAS["Convert to Pandas"]
    PANDAS --> JSON1["JSON pasien"]

    T["data/mtsamples.csv"] --> TEXT["sparkContext.textFile"]
    TEXT --> RDD["RDD String"]
    RDD --> ACTION{"Action"}
    ACTION -->|take| PREVIEW["Preview baris"]
    ACTION -->|collect| SEARCH["Pencarian keyword di Python driver"]
    SEARCH --> JSON2["JSON hasil teks"]
    PREVIEW --> JSON2
```

Pada mode Hadoop, aplikasi membaca dataset dari HDFS. Jika HDFS tidak dapat
diakses, `main.py` menggunakan file lokal di `data/` sebagai fallback:

```mermaid
flowchart LR
    HDFS["HDFS /data/*.csv"] --> CLIENT["hdfs.InsecureClient"]
    LOCAL["data/*.csv lokal"] -. fallback jika HDFS gagal .-> CLIENT
    CLIENT --> TEXT["list teks"]
    CLIENT --> PDF["pandas DataFrame"]
    TEXT --> JSON["JSON response"]
    PDF --> JSON
```

### 2. Arsitektur Pemrosesan Mode Spark

Mode Spark diaktifkan dengan `EXEC_MODE=spark`. `main.py` membuat
`SparkSession` menggunakan `local[1]`, sehingga Spark berjalan pada satu
mesin dan satu thread komputasi:

```mermaid
flowchart TB
    subgraph APP["Proses FastAPI / Uvicorn"]
        DRIVER["Spark Driver<br/>SparkSession<br/>SparkContext"]
        EXEC["Local Executor<br/>local mode one thread"]
        DRIVER -->|local scheduler| EXEC
    end

    NOTE["Local mode"] -.-> DRIVER
    NO_MASTER["Tidak ada Spark Master eksternal"] -.-> DRIVER
    NO_WORKER["Tidak ada Spark Worker cluster"] -.-> EXEC
```

Tidak ada Spark Master eksternal, Spark Worker, atau executor terpisah pada
repository ini. Dengan kata lain, `local[1]` adalah local mode, bukan Spark
cluster deployment.

#### Alur DataFrame dan Spark SQL

1. `spark.read.csv(..., header=True, inferSchema=True)` membaca
   `breast_cancer_data.csv`.
2. Data dibentuk menjadi Spark DataFrame.
3. DataFrame didaftarkan sebagai temporary view bernama `patients`.
4. Endpoint `/api/patients/filter` menjalankan query SQL untuk memfilter
   `diagnosis` dan `radius_mean`.
5. Endpoint memanggil `toPandas()`, mengubah hasil ke dictionary, lalu
   mengembalikannya sebagai JSON.

```mermaid
flowchart LR
    CSV["breast_cancer_data.csv"] --> READER["Spark CSV Reader"]
    READER --> DF["Spark DataFrame"]
    DF --> VIEW["SQL view: patients"]
    VIEW --> FILTER["SELECT id, diagnosis, radius_mean<br/>WHERE diagnosis and radius_mean"]
    FILTER --> PANDAS["Convert to Pandas"]
    PANDAS --> RECORDS["Dictionary records"]
    RECORDS --> JSON["JSON response"]
```

#### Alur RDD

1. `spark.sparkContext.textFile("data/mtsamples.csv")` membuat
   `RDD[String]`.
2. `/api/pathology/all` menggunakan `take(limit)`.
3. `/api/pathology/search` menggunakan `collect()`, kemudian pencarian
   keyword dilakukan oleh Python driver.
4. Hasil pencarian dibatasi maksimal 50 baris.

```mermaid
flowchart LR
    CSV["mtsamples.csv"] --> TEXT["Spark text file reader"]
    TEXT --> RDD["RDD String"]
    RDD --> COLLECT["Collect all rows"]
    COLLECT --> DRIVER["Python driver<br/>keyword in line.lower()"]
    DRIVER --> LIMIT["Ambil maksimal 50 baris"]
    LIMIT --> JSON["JSON response"]
```

Implementasi pencarian saat ini belum menggunakan transformasi
`text_rdd.filter(...)`; seluruh baris dikumpulkan ke driver terlebih dahulu.
Karena itu pendekatan ini cocok untuk demonstrasi, tetapi kurang ideal untuk
dataset teks yang sangat besar.

### 3. Infrastruktur Hadoop melalui Docker Compose

`docker-compose.yml` hanya mendefinisikan dua service HDFS:

| Service | Peran | Port host |
|---|---|---|
| `namenode` | Metadata HDFS, RPC, dan Web UI/WebHDFS | `9870`, `9000` |
| `datanode` | Penyimpanan blok data HDFS | `9864` |

Tidak ada YARN ResourceManager, NodeManager, Hadoop History Server, Spark
Master, atau Spark Worker dalam Compose ini. Jadi konfigurasi tersebut adalah
HDFS minimal, bukan cluster Hadoop lengkap berbasis YARN.

```mermaid
flowchart TB
    APP["Host: FastAPI / Uvicorn<br/>hdfs.InsecureClient"]

    subgraph DOCKER["Docker Compose network"]
        NN["NameNode<br/>Web UI/WebHDFS: 9870<br/>RPC: 9000"]
        DN["DataNode<br/>Web UI: 9864"]
        VN["hadoop_namenode<br/>named volume"]
        VD["hadoop_datanode<br/>named volume"]
        NN -->|metadata dan koordinasi blok| DN
        NN --- VN
        DN --- VD
    end

    APP -->|HTTP localhost:9870| NN
    DN -->|hostname namenode<br/>hdfs://namenode:9000| NN
```

Detail komunikasi:

- Kedua container berada pada network default Docker Compose.
- `datanode` menemukan NameNode melalui hostname `namenode`.
- `CORE_CONF_fs_defaultFS=hdfs://namenode:9000` menetapkan NameNode sebagai
  filesystem default.
- `SERVICE_PRECONDITION=namenode:9870` membuat DataNode menunggu NameNode
  siap.
- Named volume `hadoop_namenode` menyimpan metadata NameNode.
- Named volume `hadoop_datanode` menyimpan blok data DataNode.
- Folder host `data/` tidak otomatis dimount ke container dan tidak otomatis
  masuk HDFS.

Dataset harus dimasukkan ke HDFS secara manual, misalnya:

```mermaid
flowchart TD
    HOST["data/*.csv di host"] -->|docker cp| TMP["File sementara<br/>di container namenode"]
    TMP -->|hdfs dfs -put|     HDFS["HDFS /data"]
    HDFS --> MTS["/data/mtsamples.csv"]
    HDFS --> BC["/data/breast_cancer_data.csv"]
```

Contoh perintah:

```powershell
docker cp data\mtsamples.csv namenode:/tmp/mtsamples.csv
docker cp data\breast_cancer_data.csv namenode:/tmp/breast_cancer_data.csv
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f /tmp/mtsamples.csv /data/
docker exec namenode hdfs dfs -put -f /tmp/breast_cancer_data.csv /data/
```

Dalam mode Hadoop, `InsecureClient("http://localhost:9870", user="root")`
membaca file melalui NameNode/WebHDFS. NameNode mengelola metadata dan lokasi
blok, sedangkan isi blok disimpan oleh DataNode. Setelah dibaca, pemrosesan
berlangsung di memori proses FastAPI menggunakan list Python dan pandas; tidak
ada job MapReduce atau Spark-on-YARN.

## Teknologi

| Komponen | Peran |
|---|---|
| Python | Bahasa pemrograman utama |
| FastAPI | REST API dan routing |
| Uvicorn | Application server ASGI |
| PySpark | Pemrosesan DataFrame, Spark SQL, dan RDD |
| Hadoop HDFS | Penyimpanan/akses data terdistribusi opsional |
| `hdfs` | Client Python untuk HDFS |
| HTML, CSS, JavaScript | Dashboard frontend |
| Docker Compose | Menjalankan NameNode dan DataNode |

## Struktur Proyek

```text
.
├── data/
│   ├── breast_cancer_data.csv    # Data kanker berbentuk tabular
│   └── mtsamples.csv             # Laporan patologi berbentuk teks
├── routers/
│   ├── dataframe_api.py          # Endpoint Spark SQL/DataFrame
│   └── rdd_api.py                # Endpoint Spark RDD
├── static/
│   ├── index.html                # Dashboard analitik
│   └── tables.html               # Preview data mentah
├── docker-compose.yml            # Konfigurasi Hadoop HDFS
├── main.py                       # Inisialisasi FastAPI dan Spark/HDFS
└── README.md
```

## Dataset

### `data/breast_cancer_data.csv`

Dataset tabular dengan kolom utama:

- `id`: identitas sampel;
- `diagnosis`: `M` untuk malignant dan `B` untuk benign;
- `radius_mean`: nilai radius rata-rata;
- kolom fitur tumor lainnya seperti `texture_mean`, `area_mean`,
  `smoothness_mean`, dan fitur `*_worst`.

File ini dibaca oleh Spark dengan `header=True` dan `inferSchema=True`, lalu
daftarkan sebagai temporary view bernama `patients`.

### `data/mtsamples.csv`

Dataset laporan medis tidak terstruktur. Kolom yang tersedia antara lain
`description`, `medical_specialty`, `sample_name`, `transcription`, dan
`keywords`. File ini dimuat sebagai RDD berbasis baris teks untuk pencarian
keyword.

## Prasyarat

- Python 3.10+;
- Java yang kompatibel dengan versi PySpark yang digunakan;
- Apache Spark/PySpark;
- Docker Desktop dan Docker Compose, hanya jika ingin menjalankan HDFS;
- Windows: konfigurasi Spark lokal dapat memerlukan `winutils` sesuai versi
  Hadoop yang dipakai.

Repository belum menyediakan `requirements.txt`. Instal dependensi Python
secara manual:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install fastapi uvicorn pyspark hdfs
```

Jika PowerShell menolak aktivasi environment, jalankan:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
```

## Instalasi

1. Clone repository dan masuk ke direktori proyek.
2. Buat serta aktifkan virtual environment.
3. Instal dependensi Python seperti pada bagian [Prasyarat](#prasyarat).
4. Pastikan dataset tersedia di direktori `data/`.
5. Untuk penggunaan mode Spark, ubah konfigurasi berikut di `main.py`:

   ```python
   EXEC_MODE = "spark"
   ```

## Menjalankan Aplikasi

Jalankan server dari root repository:

```powershell
python -m uvicorn main:app --reload
```

Buka halaman berikut di browser:

- Dashboard: <http://localhost:8000/>
- Tabel data: <http://localhost:8000/tables>
- Dokumentasi Swagger: <http://localhost:8000/docs>
- Dokumentasi ReDoc: <http://localhost:8000/redoc>

Spark dijalankan dalam `local[1]` agar lebih stabil pada Windows. Konfigurasi
Arrow juga dimatikan di `main.py` untuk menghindari masalah konversi Pandas.

## Mode Eksekusi

### Mode `spark`

Mode ini:

1. membuat `SparkSession` lokal;
2. memuat `data/mtsamples.csv` sebagai `text_rdd`;
3. memuat `data/breast_cancer_data.csv` sebagai DataFrame;
4. membuat temporary view SQL bernama `patients`;
5. mengaktifkan endpoint DataFrame dan RDD.

Gunakan mode ini untuk menjalankan dashboard secara lengkap.

### Mode `hadoop`

Mode ini membuat `InsecureClient` ke:

```text
http://localhost:9870
```

dan mencoba membaca `/data/mtsamples.csv` dari HDFS. Dataset harus sudah
tersedia di HDFS. Pada implementasi saat ini, mode ini belum membuat
`SparkSession` dan `text_rdd`, sehingga endpoint analitik dashboard belum dapat
digunakan sepenuhnya tanpa pengembangan lanjutan pada `main.py`.

## Halaman Web

### Dashboard utama `/`

Dashboard memiliki dua panel:

1. **Filter Rekam Medis (DataFrame)**  
   Memfilter pasien berdasarkan diagnosis dan batas minimum `radius_mean`.
2. **Pencarian Teks Patologi (RDD)**  
   Mencari keyword pada baris laporan patologi dan menampilkan maksimal 50
   hasil.

Kedua panel otomatis memuat data ketika halaman dibuka.

### Tabel data `/tables`

Menampilkan:

- maksimal 100 baris data pasien dari endpoint DataFrame;
- maksimal 100 baris data laporan patologi dari endpoint RDD.

## API Reference

Semua endpoint mengembalikan JSON. Jika terjadi kegagalan pemrosesan, API
mengembalikan HTTP `500` dengan field `detail`.

### DataFrame API

#### `GET /api/patients/filter`

Parameter query:

| Parameter | Tipe | Default | Keterangan |
|---|---|---:|---|
| `min_radius` | float | `20.0` | Nilai minimum `radius_mean` |
| `diag_type` | string | `M` | Diagnosis, umumnya `M` atau `B` |

Response berisi `execution_time_sec`, `total_found`, dan `data`.

#### `GET /api/patients/all`

Parameter query:

| Parameter | Tipe | Default | Keterangan |
|---|---|---:|---|
| `limit` | integer | `100` | Jumlah baris maksimum |

Response berisi `columns` dan `data`.

### RDD API

#### `GET /api/pathology/search`

Parameter query:

| Parameter | Tipe | Default | Keterangan |
|---|---|---:|---|
| `keyword` | string | `cancer` | Teks yang dicari tanpa membedakan kapitalisasi |

Response berisi `execution_time_sec`, `keyword`, dan maksimal 50 baris pada
`data`.

#### `GET /api/pathology/all`

Parameter query:

| Parameter | Tipe | Default | Keterangan |
|---|---|---:|---|
| `limit` | integer | `100` | Jumlah baris maksimum |

Response berisi `data` berupa baris teks.

## Contoh Penggunaan API

```powershell
# Filter pasien malignant dengan radius minimum 20
curl "http://localhost:8000/api/patients/filter?min_radius=20&diag_type=M"

# Ambil 10 baris data pasien
curl "http://localhost:8000/api/patients/all?limit=10"

# Cari keyword pada laporan patologi
curl "http://localhost:8000/api/pathology/search?keyword=tumor"

# Ambil 20 baris pertama RDD
curl "http://localhost:8000/api/pathology/all?limit=20"
```

## Docker Hadoop HDFS

Jalankan cluster HDFS:

```powershell
docker compose up -d
docker compose ps
```

Antarmuka NameNode tersedia di <http://localhost:9870>. Untuk memasukkan
dataset ke HDFS, gunakan shell container NameNode:

```powershell
docker exec -it namenode bash
mkdir -p /data
hdfs dfs -put -f /path/di/container/mtsamples.csv /data/mtsamples.csv
hdfs dfs -ls /data
exit
```

Perintah `hdfs dfs -put` harus dijalankan terhadap file yang memang tersedia
di dalam container. Jika file masih berada di host, salin terlebih dahulu
dengan `docker cp`.

Hentikan layanan HDFS dengan:

```powershell
docker compose down
```

## Troubleshooting

### `AttributeError: 'State' object has no attribute 'spark'`

Pastikan `EXEC_MODE = "spark"` di `main.py`, lalu restart Uvicorn.

### `Java gateway process exited before sending its port number`

Periksa instalasi Java dan variabel `JAVA_HOME`. Pastikan versi Java cocok
dengan PySpark yang terpasang dan jalankan ulang virtual environment.

### HDFS tidak dapat diakses di port `9870`

Pastikan container berjalan:

```powershell
docker compose ps
docker compose logs namenode
```

Pastikan juga port `9870` tidak sedang digunakan aplikasi lain.

### Hasil API terlalu besar atau proses lambat

Gunakan parameter `limit` untuk endpoint `/all`. Endpoint pencarian RDD
mengumpulkan data dengan `collect()` dan membatasi response menjadi 50 baris,
tetapi tetap dapat memakai memori besar jika dataset bertambah signifikan.

## Catatan Pengembangan

- `main.py` saat ini menyimpan pilihan mode secara hard-coded melalui
  `EXEC_MODE`; konfigurasi environment variable dapat ditambahkan untuk
  deployment yang lebih fleksibel.
- Query DataFrame dibangun dari parameter request. Untuk penggunaan produksi,
  tambahkan validasi input dan gunakan pendekatan yang menghindari interpolasi
  SQL langsung.
- `toPandas()` cocok untuk preview berukuran kecil, bukan untuk mengirim
  keseluruhan dataset berukuran besar.
- File `__pycache__` dan environment lokal sebaiknya tidak di-commit ke
  repository.
- Tambahkan `requirements.txt` atau `pyproject.toml` agar instalasi dependency
  dapat direproduksi.

## Lisensi

Repository ini belum menyertakan berkas lisensi. Tambahkan lisensi proyek
secara eksplisit sebelum digunakan atau didistribusikan di luar kebutuhan
internal/pembelajaran.
