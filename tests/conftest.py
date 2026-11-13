"""Pytest configuration and global environment bootstrapping for PySpark."""

import os
import sys
from pathlib import Path

# 1. Tự động tìm JAVA_HOME nếu terminal hiện tại chưa load biến môi trường
if "JAVA_HOME" not in os.environ:
    candidate_jdk = Path(os.environ.get("USERPROFILE", "C:/Users/ASUS")) / "jdk-21"
    if candidate_jdk.exists():
        subdirs = [d for d in candidate_jdk.iterdir() if d.is_dir()]
        jdk_home = subdirs[0] if subdirs else candidate_jdk
        os.environ["JAVA_HOME"] = str(jdk_home)
        bin_dir = str(jdk_home / "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"

# 2. Đảm bảo PySpark worker sử dụng chính xác Python của venv
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
