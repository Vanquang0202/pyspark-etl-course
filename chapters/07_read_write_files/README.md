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

Trên Windows, đọc CSV và các action như `show()`, `printSchema()`, `count()` có thể thành công nhưng bước ghi Parquet vẫn fail nếu Hadoop không tìm thấy `winutils.exe`.

Project PySpark 4.0.1 hiện đóng kèm Hadoop client 3.4.1. Hãy dùng `winutils.exe` tương thích Hadoop 3.4.x từ nguồn tin cậy, sau đó đặt file tại:

```text
C:\hadoop\bin\winutils.exe
```

Project không tự động tải executable. Không tạo file rỗng mang tên `winutils.exe`; cần dùng binary Windows thật đã được PM/đội hạ tầng phê duyệt. Nên quét antivirus và đối chiếu checksum theo quy trình công ty trước khi copy.

Tạo thư mục và copy file:

```powershell
New-Item -ItemType Directory -Force C:\hadoop\bin
Copy-Item C:\Downloads\winutils.exe C:\hadoop\bin\winutils.exe
```

Set biến cho terminal hiện tại:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
${env:hadoop.home.dir} = "C:\hadoop"
$env:Path = "C:\hadoop\bin;$env:Path"
```

Set vĩnh viễn cho user:

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

Mở PowerShell mới và verify:

```powershell
echo $env:HADOOP_HOME
echo ${env:hadoop.home.dir}
Get-Command winutils.exe
Test-Path C:\hadoop\bin\winutils.exe
```

Chạy lại bài tập:

```powershell
cd E:\PySpark\pyspark-etl-course
.\.venv\Scripts\Activate.ps1
python chapters/07_read_write_files/exercise.py
```

`shared/spark_utils.py` sẽ cảnh báo rõ nếu thiếu biến môi trường hoặc binary. Cảnh báo không chủ động dừng app; nếu ghi Parquet cần `winutils.exe`, lỗi thực tế vẫn được Spark báo tại bước write.

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
