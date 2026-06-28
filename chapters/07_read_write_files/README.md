# Chapter 07 - Read / Write Files

## Mục tiêu

Biết đọc và ghi dữ liệu:

- CSV
- JSON
- Parquet

Trong ETL thực tế, flow thường là:

```text
read source -> transform -> write output
```

## Chạy demo

```bash
python chapters/07_read_write_files/demo.py
python chapters/07_read_write_files/exercise.py
```

## Bài tập

Đọc file `data/input/province.csv`, sau đó ghi ra `data/output/chapter07/province_parquet` dạng Parquet.

## Windows: winutils.exe khi ghi Parquet

Tren Windows, doc CSV va cac action nhu `show()`, `printSchema()`, `count()` co the thanh cong nhung buoc ghi Parquet van fail neu Hadoop khong tim thay `winutils.exe`.

Project PySpark 4.0.1 hien dong kem Hadoop client 3.4.1. Hay dung `winutils.exe` tuong thich Hadoop 3.4.x tu nguon tin cay, sau do dat file tai:

```text
C:\hadoop\bin\winutils.exe
```

Project khong tu dong tai executable. Khong tao file rong mang ten `winutils.exe`; can dung binary Windows thuc da duoc PM/doi ha tang phe duyet. Nen quet antivirus va doi chieu checksum theo quy trinh cong ty truoc khi copy.

Tao thu muc va copy file:

```powershell
New-Item -ItemType Directory -Force C:\hadoop\bin
Copy-Item C:\Downloads\winutils.exe C:\hadoop\bin\winutils.exe
```

Set bien cho terminal hien tai:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
${env:hadoop.home.dir} = "C:\hadoop"
$env:Path = "C:\hadoop\bin;$env:Path"
```

Set vinh vien cho user:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[Environment]::SetEnvironmentVariable("hadoop.home.dir", "C:\hadoop", "User")

$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ";") -notcontains "C:\hadoop\bin") {
    [Environment]::SetEnvironmentVariable(
        "Path",
        ($userPath.TrimEnd(";") + ";C:\hadoop\bin"),
        "User"
    )
}
```

Mo PowerShell moi va verify:

```powershell
echo $env:HADOOP_HOME
echo ${env:hadoop.home.dir}
Get-Command winutils.exe
Test-Path C:\hadoop\bin\winutils.exe
```

Chay lai bai tap:

```powershell
cd E:\PySpark\pyspark-etl-course
.\.venv\Scripts\Activate.ps1
python chapters/07_read_write_files/exercise.py
```

`shared/spark_utils.py` se canh bao ro neu thieu bien moi truong hoac binary. Canh bao khong chu dong dung app; neu ghi Parquet can `winutils.exe`, loi thuc te van duoc Spark bao tai buoc write.

## Quick Notes

### Bài này học gì?

* Đọc dữ liệu từ CSV, JSON và Parquet.
* Ghi kết quả ETL ra file output.
* Hiểu flow cơ bản: read source, transform, write output.

### Khái niệm chính

* `spark.read`: API đọc dữ liệu từ file/source.
* `DataFrameWriter`: API ghi DataFrame ra output.
* CSV: format text phổ biến, thường cần header/schema/options.
* JSON: format semi-structured, mỗi record có thể là object.
* Parquet: format cột, phù hợp cho analytics và Spark.
* `mode()`: chọn cách xử lý khi output path đã tồn tại.

### Nhớ nhanh

* Spark write thường tạo folder output, không phải một file duy nhất.
* Parquet thường tốt hơn CSV cho dữ liệu đã xử lý.
