# -*- coding: utf-8 -*-
import os, sys
# Fix Windows console encoding
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

"""
generate_test_datasets.py
=========================
Sinh 10 dirty CSV dataset (moi cai 1000 rows) phuc vu test pipeline Agentic Data Cleaner.

Mỗi dataset tập trung vào 1–3 loại lỗi cụ thể mà hệ thống cần xử lý:
  DS01 – Full-row duplicate hoàn toàn (toàn bộ 500 rows bị nhân đôi)
  DS02 – Duplicate theo Primary Key (PK trùng, các cột khác khác nhau)
  DS03 – Null rate cao toàn bộ (nhiều cột gần 100% null)
  DS04 – NULL + disguised missing values (N/A, unknown, -, none, 0)
  DS05 – Type mismatch: số lưu dưới dạng string, ngày sai format
  DS06 – Mixed type: cột số chứa lẫn string "N/A", int và float lẫn lộn
  DS07 – Kết hợp: Duplicate PK + NULL vừa phải
  DS08 – Kết hợp: NULL cao + Type mismatch nghiêm trọng
  DS09 – Kết hợp toàn diện: Full duplicate + PK duplicate + NULL + DMV + Typecast
  DS10 – Edge case: cột constant, cột unique ratio thấp, tên cột có khoảng trắng

Usage:
    python tests/generate_test_datasets.py
    # → Tạo thư mục tests/dirty_datasets/ với 10 file CSV
"""

import random
import string
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
rng = np.random.default_rng(SEED)
random.seed(SEED)

OUTPUT_DIR = Path(__file__).parent / "dirty_datasets"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

N = 1000  # số rows mỗi dataset


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def rand_str(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_letters, k=n))

def rand_email(name: str) -> str:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
    return f"{name.lower()}@{random.choice(domains)}"

def rand_phone() -> str:
    return f"0{rng.integers(300_000_000, 999_999_999)}"

def rand_date_str(fmt="%Y-%m-%d") -> str:
    y = rng.integers(2018, 2025)
    m = rng.integers(1, 13)
    d = rng.integers(1, 29)
    from datetime import date
    return date(y, m, d).strftime(fmt)

def inject_nulls(series: pd.Series, null_rate: float, rng_obj=rng) -> pd.Series:
    """Thay một phần giá trị bằng NaN theo null_rate."""
    mask = rng_obj.random(len(series)) < null_rate
    series = series.copy().astype(object)
    series[mask] = np.nan
    return series

def inject_dmv(series: pd.Series, dmv_rate: float, dmv_values=None, rng_obj=rng) -> pd.Series:
    """Thay một phần giá trị bằng disguised missing values."""
    if dmv_values is None:
        dmv_values = ["N/A", "unknown", "-", "none", "0", "NULL", ""]
    mask = rng_obj.random(len(series)) < dmv_rate
    series = series.copy().astype(object)
    series[mask] = [random.choice(dmv_values) for _ in range(mask.sum())]
    return series

def save(df: pd.DataFrame, name: str):
    path = OUTPUT_DIR / name
    df.to_csv(path, index=False)
    print(f"  [OK] Saved {name} -- {len(df)} rows x {len(df.columns)} cols")
    return path


# ─────────────────────────────────────────────────────────────────
# DS01: Full-row duplicate (500 hàng gốc x 2)
# Domain: bệnh viện / bệnh nhân
# Đặc tính: exact_full_row_duplicate_count = 500
# ─────────────────────────────────────────────────────────────────
def ds01_full_row_duplicate():
    """DS01 — Toàn bộ 500 hàng bị nhân đôi (1000 rows = 500 unique x 2)."""
    base_n = N // 2
    base = pd.DataFrame({
        "patient_id":    [f"P{i:05d}" for i in range(1, base_n + 1)],
        "full_name":     [rand_str(10) for _ in range(base_n)],
        "age":           rng.integers(18, 85, size=base_n).tolist(),
        "gender":        random.choices(["Male", "Female", "Other"], k=base_n),
        "blood_type":    random.choices(["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"], k=base_n),
        "diagnosis":     random.choices(["Diabetes", "Hypertension", "Flu", "Asthma", "COVID-19"], k=base_n),
        "admission_date": [rand_date_str() for _ in range(base_n)],
        "discharge_date": [rand_date_str() for _ in range(base_n)],
        "ward":          random.choices(["ICU", "Emergency", "General", "Surgery", "Pediatrics"], k=base_n),
        "doctor_id":     [f"DR{rng.integers(100, 999)}" for _ in range(base_n)],
    })
    df = pd.concat([base, base], ignore_index=True)  # 100% duplicate
    save(df, "DS01_full_row_duplicate.csv")

    print(f"     → duplicate_rows = {df.duplicated().sum()} (expected 500)")
    return df


# ─────────────────────────────────────────────────────────────────
# DS02: PK-based duplicate (PK trùng, fields khác nhau)
# Domain: đơn hàng e-commerce
# Đặc tính: order_id bị lặp, nhưng các trường khác khác nhau
# ─────────────────────────────────────────────────────────────────
def ds02_pk_duplicate():
    """DS02 — 300/1000 rows bị PK duplicate (order_id trùng nhưng fields khác)."""
    products = ["Laptop", "Phone", "Headphones", "Monitor", "Keyboard", "Mouse"]
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

    # 700 unique orders
    ids_unique = [f"ORD-{i:06d}" for i in range(1, 701)]
    # 300 duplicated PKs (lấy từ 300 order đầu)
    ids_dup = random.choices(ids_unique[:300], k=300)
    all_ids = ids_unique + ids_dup

    df = pd.DataFrame({
        "order_id":       all_ids,
        "customer_id":    [f"CUST-{rng.integers(1000, 9999)}" for _ in range(N)],
        "product_name":   random.choices(products, k=N),
        "quantity":       rng.integers(1, 10, size=N).tolist(),
        "unit_price":     rng.uniform(10.0, 2000.0, size=N).round(2).tolist(),
        "order_date":     [rand_date_str() for _ in range(N)],
        "shipping_addr":  [f"{rng.integers(1, 999)} Main St, City {rng.integers(1, 50)}" for _ in range(N)],
        "status":         random.choices(statuses, k=N),
        "payment_method": random.choices(["credit_card", "paypal", "bank_transfer", "cod"], k=N),
        "notes":          random.choices(["urgent", "fragile", "gift-wrap", "", None, None], k=N),
    })
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    save(df, "DS02_pk_duplicate.csv")

    dup_count = df.duplicated(subset=["order_id"]).sum()
    print(f"     → pk_duplicate_count (order_id) = {dup_count} (expected ~300)")
    return df


# ─────────────────────────────────────────────────────────────────
# DS03: Null rate cực cao (>70% null trên nhiều cột)
# Domain: khảo sát / survey data
# Đặc tính: high_null_columns nhiều, nhiều cột gần 100% null
# ─────────────────────────────────────────────────────────────────
def ds03_high_null():
    """DS03 — Nhiều cột có null_rate > 70%, 2 cột gần 100% null."""
    df = pd.DataFrame({
        "survey_id":      [f"SRV-{i:05d}" for i in range(1, N + 1)],
        "respondent_id":  [f"RESP-{rng.integers(10000, 99999)}" for _ in range(N)],
        "age_group":      random.choices(["18-25", "26-35", "36-50", "50+", None], weights=[20, 25, 25, 20, 10], k=N),
        "q1_satisfaction": rng.choice([1, 2, 3, 4, 5, None], size=N, p=[0.05, 0.05, 0.1, 0.1, 0.05, 0.65]).tolist(),
        "q2_recommend":   rng.choice(["Yes", "No", "Maybe", None], size=N, p=[0.05, 0.03, 0.02, 0.90]).tolist(),
        "q3_comment":     inject_nulls(pd.Series(["Great service" if i % 20 == 0 else "OK" for i in range(N)]), 0.92).tolist(),
        "q4_income":      inject_nulls(pd.Series(rng.integers(1000, 50000, size=N).astype(float)), 0.85).tolist(),
        "q5_education":   inject_nulls(pd.Series(random.choices(["High School", "Bachelor", "Master", "PhD"], k=N)), 0.78).tolist(),
        "q6_occupation":  inject_nulls(pd.Series(random.choices(["Student", "Employed", "Self-employed", "Retired"], k=N)), 0.80).tolist(),
        "submit_date":    [rand_date_str() for _ in range(N)],
    })
    save(df, "DS03_high_null.csv")

    null_rates = df.isnull().mean()
    high = null_rates[null_rates > 0.5]
    print(f"     → high_null_columns (>50%): {list(high.index)} | rates: {high.round(2).to_dict()}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS04: NULL + Disguised Missing Values (DMV)
# Domain: nhân sự / HR records
# Đặc tính: null thật + "N/A", "unknown", "-", "none" lẫn lộn
# ─────────────────────────────────────────────────────────────────
def ds04_null_and_dmv():
    """DS04 — Kết hợp null thật và disguised missing values (DMV)."""
    names = [rand_str(6).capitalize() + " " + rand_str(8).capitalize() for _ in range(N)]
    df = pd.DataFrame({
        "emp_id":         [f"EMP-{i:05d}" for i in range(1, N + 1)],
        "full_name":      names,
        "department":     inject_dmv(pd.Series(random.choices(["Engineering", "Sales", "HR", "Finance", "Marketing"], k=N)), 0.15),
        "job_title":      inject_dmv(pd.Series(random.choices(["Manager", "Senior", "Junior", "Lead", "Director"], k=N)), 0.20, ["N/A", "unknown", "-"]),
        "salary":         inject_dmv(
                            inject_nulls(pd.Series(rng.integers(20000, 150000, size=N).astype(float)), 0.10),
                            0.12, ["N/A", "0", "unknown"]
                          ),
        "hire_date":      inject_dmv(pd.Series([rand_date_str() for _ in range(N)]), 0.08, ["N/A", "-", "unknown"]),
        "phone":          inject_dmv(pd.Series([rand_phone() for _ in range(N)]), 0.25, ["N/A", "none", "-", ""]),
        "email":          inject_dmv(pd.Series([rand_email(n.split()[0]) for n in names]), 0.18, ["N/A", "unknown"]),
        "manager_id":     inject_nulls(pd.Series([f"EMP-{rng.integers(1, 100):05d}" for _ in range(N)]), 0.30),
        "performance_score": inject_dmv(
                            inject_nulls(pd.Series(rng.choice([1.0, 2.0, 3.0, 4.0, 5.0], size=N)), 0.10),
                            0.15, ["N/A", "unknown", "0"]
                          ),
    })
    save(df, "DS04_null_and_dmv.csv")

    # Report disguised nulls
    for col in ["department", "job_title", "salary", "phone"]:
        str_col = df[col].dropna().astype(str).str.strip().str.lower()
        dmv_count = str_col.isin(["n/a", "unknown", "-", "none", "0", "null", ""]).sum()
        print(f"     → DMV in '{col}': {dmv_count}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS05: Type mismatch (số lưu dạng string, ngày sai format)
# Domain: financial transactions
# Đặc tính: amount là string "1234.56 VND", date là "15/06/2023", bool là "yes"/"no"
# ─────────────────────────────────────────────────────────────────
def ds05_type_mismatch():
    """DS05 — Type mismatch nghiêm trọng: số lưu dạng string, ngày sai format, bool sai."""
    def bad_amount():
        """Số tiền lưu dạng string với đơn vị tiền tệ."""
        amt = rng.uniform(100, 100000)
        choices = [
            f"{amt:,.2f} VND",
            f"${amt:.2f}",
            f"{amt:.0f}",
            f"VND {amt:.2f}",
            str(int(amt)),
        ]
        return random.choice(choices)

    def bad_date():
        """Ngày lưu sai format."""
        y = rng.integers(2018, 2025)
        m = rng.integers(1, 13)
        d = rng.integers(1, 29)
        formats = [
            f"{d:02d}/{m:02d}/{y}",          # DD/MM/YYYY
            f"{m:02d}-{d:02d}-{y}",          # MM-DD-YYYY
            f"{d}/{m}/{str(y)[-2:]}",         # D/M/YY
            f"{y}/{m:02d}/{d:02d}",           # YYYY/MM/DD
        ]
        return random.choice(formats)

    df = pd.DataFrame({
        "txn_id":         [f"TXN{i:07d}" for i in range(1, N + 1)],
        "account_from":   [f"ACC-{rng.integers(100000, 999999)}" for _ in range(N)],
        "account_to":     [f"ACC-{rng.integers(100000, 999999)}" for _ in range(N)],
        "amount":         [bad_amount() for _ in range(N)],          # ← type mismatch: should be float
        "currency":       random.choices(["VND", "USD", "EUR", "GBP"], k=N),
        "txn_date":       [bad_date() for _ in range(N)],            # ← type mismatch: should be date
        "txn_type":       random.choices(["debit", "credit", "transfer", "refund"], k=N),
        "is_flagged":     random.choices(["yes", "no", "true", "false", "1", "0"], k=N),  # ← should be bool
        "fee":            [f"{rng.uniform(0, 50):.2f} VND" if rng.random() < 0.6 else str(round(float(rng.uniform(0, 50)), 2)) for _ in range(N)],  # ← mixed format
        "processed_by":   [f"USR-{rng.integers(100, 999)}" for _ in range(N)],
    })
    save(df, "DS05_type_mismatch.csv")

    print(f"     → 'amount' sample: {df['amount'].head(5).tolist()}")
    print(f"     → 'txn_date' sample: {df['txn_date'].head(5).tolist()}")
    print(f"     → 'is_flagged' sample: {df['is_flagged'].head(5).tolist()}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS06: Mixed type (cột số chứa lẫn string, float và int lẫn lộn)
# Domain: điểm thi / education
# Đặc tính: score chứa lẫn "N/A", "absent", int và float; grade_point là string
# ─────────────────────────────────────────────────────────────────
def ds06_mixed_type():
    """DS06 — Cột số chứa lẫn string DMV; mixed int/float; grade là string cần cast."""
    def messy_score():
        roll = rng.random()
        if roll < 0.70:
            return str(round(rng.uniform(0, 10), 1))  # normal score as string "7.5"
        elif roll < 0.80:
            return random.choice(["N/A", "absent", "medical_leave", "excused"])
        elif roll < 0.90:
            return str(rng.integers(0, 11))   # integer score as string "8"
        else:
            return None

    df = pd.DataFrame({
        "student_id":     [f"STU-{i:06d}" for i in range(1, N + 1)],
        "student_name":   [rand_str(8).capitalize() for _ in range(N)],
        "course_code":    random.choices(["CS101", "MATH201", "ENG301", "PHY101", "CHEM201", "BIO101"], k=N),
        "semester":       random.choices(["2022-1", "2022-2", "2023-1", "2023-2", "2024-1"], k=N),
        "midterm_score":  [messy_score() for _ in range(N)],         # ← mixed: float + string DMV
        "final_score":    [messy_score() for _ in range(N)],         # ← mixed: float + string DMV
        "lab_score":      [messy_score() for _ in range(N)],         # ← mixed
        "grade_letter":   random.choices(["A", "B+", "B", "C+", "C", "D", "F", "N/A"], k=N),
        "gpa_point":      [str(round(rng.uniform(0.0, 4.0), 2)) if rng.random() > 0.15 else random.choice(["N/A", "absent", None]) for _ in range(N)],  # ← string, should be float
        "is_pass":        random.choices(["pass", "fail", "incomplete", "1", "0", True, False], k=N),  # ← mixed bool
    })
    save(df, "DS06_mixed_type.csv")
    print(f"     → 'midterm_score' sample: {df['midterm_score'].head(8).tolist()}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS07: PK Duplicate + NULL vừa phải
# Domain: sản phẩm / inventory
# Đặc tính: product_id bị lặp (250 PKs trùng) + 20-40% null trên vài cột
# ─────────────────────────────────────────────────────────────────
def ds07_pk_dup_and_null():
    """DS07 — Kết hợp PK duplicate (250 cases) + moderate NULL (20-40%)."""
    categories = ["Electronics", "Clothing", "Food", "Furniture", "Books", "Sports"]
    # 750 unique products + 250 PK duplicates
    unique_ids = [f"PROD-{i:06d}" for i in range(1, 751)]
    dup_ids = random.choices(unique_ids[:250], k=250)
    all_ids = unique_ids + dup_ids

    df = pd.DataFrame({
        "product_id":     all_ids,
        "product_name":   [rand_str(12) for _ in range(N)],
        "category":       random.choices(categories, k=N),
        "brand":          inject_nulls(pd.Series(random.choices(["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"], k=N)), 0.25),
        "price":          inject_nulls(pd.Series(rng.uniform(5.0, 5000.0, size=N).round(2)), 0.15),
        "stock_qty":      inject_nulls(pd.Series(rng.integers(0, 10000, size=N).astype(float)), 0.20),
        "weight_kg":      inject_nulls(pd.Series(rng.uniform(0.1, 50.0, size=N).round(3)), 0.35),
        "supplier_id":    inject_nulls(pd.Series([f"SUP-{rng.integers(100, 999)}" for _ in range(N)]), 0.30),
        "warehouse":      random.choices(["WH-North", "WH-South", "WH-East", "WH-West", None], weights=[25, 25, 25, 20, 5], k=N),
        "last_updated":   inject_nulls(pd.Series([rand_date_str() for _ in range(N)]), 0.10),
    })
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    save(df, "DS07_pk_dup_and_null.csv")

    dup_count = df.duplicated(subset=["product_id"]).sum()
    null_rates = df.isnull().mean().round(2)
    print(f"     → pk_duplicate (product_id) = {dup_count}")
    print(f"     → null_rates: {null_rates.to_dict()}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS08: NULL cao + Type mismatch nghiêm trọng
# Domain: IoT sensor data
# Đặc tính: nhiều cột null >60%, các cột numeric lưu dưới dạng string
# ─────────────────────────────────────────────────────────────────
def ds08_null_and_typecast():
    """DS08 — NULL cao (50-80%) + type mismatch (sensor values là string)."""
    def sensor_val(null_rate=0.0, unit=""):
        vals = []
        for _ in range(N):
            r = rng.random()
            if r < null_rate:
                vals.append(None)
            elif r < null_rate + 0.1:
                vals.append(random.choice(["ERROR", "N/A", "TIMEOUT", "#ERR"]))
            else:
                v = round(float(rng.uniform(-10, 150)), 3)
                vals.append(f"{v} {unit}".strip() if unit else str(v))
        return vals

    df = pd.DataFrame({
        "sensor_id":        [f"SEN-{i:06d}" for i in range(1, N + 1)],
        "device_name":      inject_nulls(pd.Series(random.choices([f"Device_{c}" for c in "ABCDEFGHIJ"], k=N)), 0.55),
        "location":         inject_nulls(pd.Series(random.choices(["Room A", "Room B", "Floor 1", "Floor 2", "Outdoor"], k=N)), 0.60),
        "temperature_c":    sensor_val(null_rate=0.20, unit="°C"),    # ← should be float, stored as string "25.3 °C"
        "humidity_pct":     sensor_val(null_rate=0.30, unit="%"),      # ← should be float, stored as "67.2 %"
        "pressure_hpa":     sensor_val(null_rate=0.65, unit="hPa"),   # ← high null + string
        "co2_ppm":          sensor_val(null_rate=0.70, unit="ppm"),   # ← high null + string
        "battery_level":    inject_nulls(pd.Series([f"{rng.integers(0, 101)}%" for _ in range(N)]), 0.45),  # ← "87%", should be int
        "last_ping":        inject_nulls(pd.Series([rand_date_str("%Y-%m-%dT%H:%M:%S") for _ in range(N)]), 0.25),
        "firmware_version": inject_nulls(pd.Series([f"v{rng.integers(1,5)}.{rng.integers(0,10)}.{rng.integers(0,20)}" for _ in range(N)]), 0.50),
    })
    save(df, "DS08_null_and_typecast.csv")

    null_rates = df.isnull().mean().round(2)
    high = null_rates[null_rates > 0.40]
    print(f"     → high_null cols (>40%): {high.to_dict()}")
    print(f"     → 'temperature_c' sample: {df['temperature_c'].dropna().head(5).tolist()}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS09: Tất cả lỗi tổng hợp (MEGA DIRTY)
# Domain: bệnh viện nâng cao
# Đặc tính: full duplicate + PK duplicate + NULL + DMV + type mismatch tất cả cùng lúc
# ─────────────────────────────────────────────────────────────────
def ds09_all_issues():
    """DS09 — MEGA DIRTY: kết hợp tất cả 5 loại lỗi đồng thời."""
    base_n = 600

    def messy_bp():
        """Blood pressure: lẽ ra là int, nhưng có dạng '120/80', '130 mmHg', None, 'N/A'."""
        opts = [
            lambda: f"{rng.integers(90, 160)}/{rng.integers(60, 100)}",
            lambda: f"{rng.integers(90, 160)} mmHg",
            lambda: str(rng.integers(90, 160)),
            lambda: random.choice(["N/A", "unknown", "error"]),
        ]
        fn = random.choice(opts)
        return fn()

    # 600 base records
    base = pd.DataFrame({
        "record_id":      [f"REC-{i:06d}" for i in range(1, base_n + 1)],
        "patient_id":     [f"PAT-{rng.integers(1, 400):05d}" for _ in range(base_n)],  # PK duplicate intentional
        "patient_name":   inject_dmv(pd.Series([rand_str(8).capitalize() for _ in range(base_n)]), 0.05, ["unknown", "N/A"]),
        "age":            inject_dmv(inject_nulls(pd.Series(rng.integers(0, 100, size=base_n).astype(float)), 0.10), 0.08, ["N/A", "0", "unknown"]),
        "diagnosis_code": inject_nulls(pd.Series(random.choices(["ICD-001", "ICD-002", "ICD-003", "ICD-004", "ICD-005"], k=base_n)), 0.20),
        "blood_pressure": [messy_bp() for _ in range(base_n)],          # ← type mismatch
        "test_result":    inject_dmv(pd.Series(random.choices(["Positive", "Negative", "Pending"], k=base_n)), 0.12, ["N/A", "unknown", "-"]),
        "admission_date": inject_nulls(pd.Series([rand_date_str("%d/%m/%Y") for _ in range(base_n)]), 0.08),  # ← wrong date format
        "cost_vnd":       inject_dmv(pd.Series([f"{rng.integers(100000, 5000000):,} VND" for _ in range(base_n)]), 0.10, ["N/A", "0"]),  # ← string
        "doctor_notes":   inject_nulls(pd.Series(random.choices(["stable", "critical", "recovering", "discharged"], k=base_n)), 0.50),
    })

    # 200 rows lấy từ base (PK duplicate)
    pk_dups = base.sample(n=200, random_state=SEED).copy()
    pk_dups["record_id"] = [f"REC-DUP-{i:04d}" for i in range(200)]  # record_id khác nhưng patient_id trùng

    # 200 rows full-row duplicate
    full_dups = base.sample(n=200, random_state=SEED + 1).copy()

    df = pd.concat([base, pk_dups, full_dups], ignore_index=True)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    save(df, "DS09_all_issues.csv")

    full_dup_count = df.duplicated().sum()
    pk_dup_count = df.duplicated(subset=["patient_id"]).sum()
    null_rates = df.isnull().mean().round(2)
    print(f"     → total rows = {len(df)}")
    print(f"     → full_row_dup = {full_dup_count}")
    print(f"     → patient_id PK dup = {pk_dup_count}")
    print(f"     → null_rates: {null_rates.to_dict()}")
    return df


# ─────────────────────────────────────────────────────────────────
# DS10: Edge cases (constant column, very low unique ratio, spaces in col names)
# Domain: logistics / shipping
# Đặc tính: cột constant, unique ratio rất thấp, cột có khoảng trắng trong tên
# ─────────────────────────────────────────────────────────────────
def ds10_edge_cases():
    """DS10 — Edge cases: constant column, khoảng trắng trong tên cột, low-cardinality."""
    carriers = ["Carrier A", "Carrier B"]  # chỉ 2 giá trị → near-constant

    df = pd.DataFrame({
        "shipment id":          [f"SHP-{i:06d}" for i in range(1, N + 1)],  # ← khoảng trắng trong tên cột
        "origin country":       ["Vietnam"] * N,                             # ← CONSTANT column
        "destination country":  random.choices(["USA", "UK", "Germany", "Japan", "Australia", "Canada"], k=N),
        "carrier name":         random.choices(carriers, k=N),               # ← near-constant (2 values)
        "weight kg":            inject_nulls(pd.Series(rng.uniform(0.1, 100.0, size=N).round(2)), 0.10),  # ← khoảng trắng
        "shipping cost vnd":    [f"{rng.integers(50000, 2000000):,}" for _ in range(N)],  # ← string, should be int
        "estimated days":       inject_dmv(pd.Series(rng.integers(1, 30, size=N).astype(float)), 0.10, ["N/A", "unknown"]),
        "tracking number":      [f"VN{rng.integers(100000000, 999999999):09d}VN" if rng.random() > 0.20 else None for _ in range(N)],
        "status":               random.choices(["in_transit", "delivered", "returned", "lost", "pending"], k=N),
        "insurance required":   random.choices(["yes", "no", "YES", "NO", "1", "0", True, False], k=N),  # ← mixed bool
    })

    save(df, "DS10_edge_cases.csv")

    print(f"     → 'origin country' unique values: {df['origin country'].nunique()} (constant)")
    print(f"     → 'carrier name' unique values: {df['carrier name'].nunique()} (near-constant)")
    print(f"     → column names with spaces: {[c for c in df.columns if ' ' in c]}")
    return df


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("  Generating 10 Dirty Test Datasets (N=1000 rows each)")
    print(f"  Output directory: {OUTPUT_DIR}")
    print("=" * 70)
    print()

    generators = [
        ("DS01 — Full-row duplicate",                  ds01_full_row_duplicate),
        ("DS02 — PK-based duplicate",                  ds02_pk_duplicate),
        ("DS03 — High NULL rate",                       ds03_high_null),
        ("DS04 — NULL + Disguised Missing Values",     ds04_null_and_dmv),
        ("DS05 — Type mismatch (string/date/bool)",    ds05_type_mismatch),
        ("DS06 — Mixed type (numbers as string)",      ds06_mixed_type),
        ("DS07 — PK duplicate + moderate NULL",        ds07_pk_dup_and_null),
        ("DS08 — High NULL + Type mismatch",           ds08_null_and_typecast),
        ("DS09 — ALL ISSUES combined (mega dirty)",    ds09_all_issues),
        ("DS10 — Edge cases",                           ds10_edge_cases),
    ]

    results = []
    for label, fn in generators:
        print(f"\n[{label}]")
        try:
            df = fn()
            results.append({"name": label, "status": "OK", "rows": len(df), "cols": len(df.columns)})
        except Exception as e:
            print(f"  [ERR] ERROR: {e}")
            results.append({"name": label, "status": f"ERROR: {e}", "rows": 0, "cols": 0})

    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    for r in results:
        status_icon = "OK" if r["status"] == "OK" else "FAIL"
        print(f"  [{status_icon}] {r['name']} -- {r['rows']} rows x {r['cols']} cols")

    # Generate manifest
    manifest_path = OUTPUT_DIR / "MANIFEST.md"
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write("# Dirty Dataset Manifest\n\n")
        f.write("| File | Domain | Primary Issues | Rows | Cols |\n")
        f.write("|------|--------|----------------|------|------|\n")
        rows_manifest = [
            ("DS01_full_row_duplicate.csv", "Bệnh nhân/Hospital", "Full-row duplicate (500 hàng x 2)", N, 10),
            ("DS02_pk_duplicate.csv", "Đơn hàng/E-commerce", "PK duplicate (order_id, 300 cases)", N, 10),
            ("DS03_high_null.csv", "Khảo sát/Survey", "Null rate >70% trên 5+ cột", N, 10),
            ("DS04_null_and_dmv.csv", "Nhân sự/HR", "NULL thật + DMV (N/A, unknown, -)", N, 10),
            ("DS05_type_mismatch.csv", "Giao dịch/Finance", "Type mismatch: amount, date, bool sai kiểu", N, 10),
            ("DS06_mixed_type.csv", "Điểm thi/Education", "Mixed type: float+string+None trong cột số", N, 10),
            ("DS07_pk_dup_and_null.csv", "Sản phẩm/Inventory", "PK duplicate + NULL vừa phải (20-35%)", N, 10),
            ("DS08_null_and_typecast.csv", "IoT Sensor", "NULL cao (50-80%) + string sensor values", N, 10),
            ("DS09_all_issues.csv", "Hồ sơ bệnh viện", "ALL: full-dup + PK-dup + NULL + DMV + typecast", "~1000", 10),
            ("DS10_edge_cases.csv", "Logistics", "Constant col, tên có dấu cách, near-constant, mixed bool", N, 10),
        ]
        for row in rows_manifest:
            f.write(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} |\n")

    print(f"\n  [Manifest] Saved: {manifest_path}")
    print(f"\n  All datasets saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
