import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WINDOWS_HADOOP_HOME = Path(r"C:\hadoop")


def _add_project_root_to_pythonpath() -> None:
    project_root = str(PROJECT_ROOT)
    pythonpath_entries = [
        entry for entry in os.environ.get("PYTHONPATH", "").split(os.pathsep)
        if entry
    ]
    if project_root not in pythonpath_entries:
        pythonpath_entries.insert(0, project_root)
    os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath_entries)


def _warn_if_windows_hadoop_is_missing() -> None:
    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    hadoop_home_dir = os.environ.get("hadoop.home.dir")
    winutils_path = DEFAULT_WINDOWS_HADOOP_HOME / "bin" / "winutils.exe"

    warnings = []
    if not hadoop_home:
        warnings.append('HADOOP_HOME is not set (expected value: C:\\hadoop).')
    if not hadoop_home_dir:
        warnings.append('hadoop.home.dir is not set (expected value: C:\\hadoop).')
    if not winutils_path.is_file():
        print(
            "[WARNING] Missing winutils.exe. "
            "Writing CSV/Parquet on Windows may fail.",
            file=sys.stderr,
        )
        warnings.append(f"Expected file: {winutils_path}")

    if warnings:
        print("[WARNING] Windows Hadoop setup is incomplete.", file=sys.stderr)
        for warning in warnings:
            print(f"[WARNING] {warning}", file=sys.stderr)
        print(
            "[WARNING] Reading data may work, but writing Parquet can fail. "
            "See README.md or chapters/07_read_write_files/README.md.",
            file=sys.stderr,
        )


def create_spark_session(app_name: str) -> "SparkSession":
    """Create a local Spark session using the current Python executable."""
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    _add_project_root_to_pythonpath()
    _warn_if_windows_hadoop_is_missing()

    # Import only after configuring the Python executable used by Spark.
    from pyspark.sql import SparkSession

    print(f"Spark Python executable: {python_executable}")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
