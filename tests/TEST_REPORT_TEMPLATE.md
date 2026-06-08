# Báo Cáo Test — Agentic Data Cleaner
**Ngày test:** 2026-06-07  
**Người test:** [Điền tên]  
**Môi trường:** Local / Dev  
**Model LLM:** [GPT-4o / Claude Sonnet / ...]  
**Git commit:** [hash]  
**Test run dir:** `tests/test_results/run_<timestamp>/`

---

## 1. Môi trường & Chuẩn bị

| Mục | Giá trị |
|-----|---------|
| Python version | `.venv/Scripts/python --version` |
| LLM Provider | Xem `.env` → `DEFAULT_LLM_PROVIDER` |
| LLM Model | Xem `.env` → `DEFAULT_LLM_MODEL` |
| Docker (Postgres + Redis) | ☐ Running / ☐ Not running |
| Datasets generated | ☐ Yes (`tests/dirty_datasets/`) / ☐ No |
| Dataset count | 10 files × 1000 rows |

**Lệnh sinh dataset:**
```powershell
.venv\Scripts\python.exe tests/generate_test_datasets.py
```

---

## 2. Kết quả Profiler Test (Không cần LLM) — ĐÃ XÁC NHẬN

> **Kết quả thực tế từ run 2026-06-07 12:44:46:** 10/10 PASS

### Tổng hợp Profiler

| DS | Domain | Total Rows | Dup Rows | High-Null Cols (>50%) | PK Candidates | Near-Unique | Status |
|----|--------|-----------|---------|----------------------|--------------|-------------|--------|
| DS01 | Bệnh nhân | 1000 | **500** | - | - | - | ✅ PASS |
| DS02 | Đơn hàng | 1000 | 0 | - | unit_price | customer_id, order_date, shipping_addr | ✅ PASS |
| DS03 | Khảo sát | 1000 | 0 | q1_satisfaction, q2_recommend, q3_comment, q4_income, q5_education, q6_occupation | survey_id | respondent_id, submit_date | ✅ PASS |
| DS04 | Nhân sự | 1000 | 0 | - | emp_id, full_name | email | ✅ PASS |
| DS05 | Finance | 1000 | 0 | - | txn_id, account_from, account_to | amount, txn_date, fee | ✅ PASS |
| DS06 | Điểm thi | 1000 | 0 | - | student_id, student_name | - | ✅ PASS |
| DS07 | Inventory | 1000 | 0 | - | product_name | price | ✅ PASS |
| DS08 | IoT Sensor | 1000 | 0 | device_name, location, pressure_hpa, co2_ppm, firmware_version | sensor_id | - | ✅ PASS |
| DS09 | Bệnh viện | 1000 | **200** | - | - | - | ✅ PASS |
| DS10 | Logistics | 1000 | 0 | - | shipment id, shipping cost vnd | weight kg, tracking number | ✅ PASS |

**Pass Rate:** 10/10 (100%)

### Nhận xét chi tiết Profiler

**DS01 — Full-row duplicate:**
- `duplicate_rows = 500` ✅ (expected 500 — 500 hàng gốc × 2)
- `pk_candidates = []` — không detect PK vì tất cả cột đều có dup
- Profiler detect đúng đặc tính chính

**DS02 — PK duplicate:**
- `duplicate_rows = 0` — đúng (các row khác nhau, chỉ PK trùng)
- `near_unique = ['customer_id', 'order_date', 'shipping_addr']` — profiler detect cột gần-unique
- ⚠️ `order_id` KHÔNG nằm trong near_unique hay pk_candidates — vì unique_ratio < 0.8 (1000 rows, 300 dup → ratio = 0.7)
  - **Lưu ý**: Semantic profiler cần detect `order_id` là semantic PK

**DS03 — High NULL:**
- Detect đúng 6 cột với null >50% ✅
- Các cột q2_recommend (91%), q3_comment (92%) detect đúng

**DS07 — PK dup + NULL:**
- `duplicate_rows = 0` — đúng (PK dup nhưng không full-row dup)
- Null rates được profiler detect qua `high_null_columns` (chưa đủ threshold 50%)
- `pk_candidates = ['product_name']` — product_name unique do random string

**DS08 — High NULL + Typecast:**
- 5 cột high-null (>50%) detect đúng ✅
- `temperature_c`, `humidity_pct` lưu dạng "25.3 °C" — sẽ được detect bởi Semantic Profiler

**DS09 — Mega Dirty:**
- `duplicate_rows = 200` ✅ (200 full-row dup)
- PK dup (patient_id) không detect qua statistical profiler — cần semantic profiler

**DS10 — Edge Cases:**
- Cột có dấu cách trong tên được xử lý bình thường ✅
- `origin country` constant KHÔNG trong pk_candidates ✅ (unique ratio = 0 → CONSTANT category)

---

## 3. Kết quả Input Validator (Full Pipeline — Cần LLM)

**Lệnh chạy:**
```powershell
# Tất cả dataset
.venv\Scripts\python.exe tests/run_test_flow.py

# Chỉ 1 dataset
.venv\Scripts\python.exe tests/run_test_flow.py --dataset DS01
```

> **⚠️ Cần điền sau khi chạy với LLM:**

### Tổng hợp Validator

| DS | User Prompt (tóm tắt) | Expected Status | Actual Status | Khớp? | Pass? |
|----|----------------------|-----------------|---------------|-------|-------|
| DS01 | "Resolve all null, duplicate..." | `ready` | ___ | ☐ | ☐ |
| DS02 | "Clean all duplicate orders..." | `ready` | ___ | ☐ | ☐ |
| DS03 | "clean the data" (vague) | `needs_clarification` | ___ | ☐ | ☐ |
| DS04 | "Remove all null and DMV..." | `ready` | ___ | ☐ | ☐ |
| DS05 | "Fix all type errors: amount, date, bool" | `ready` | ___ | ☐ | ☐ |
| DS06 | "process this dataset" (vague) | `needs_clarification` | ___ | ☐ | ☐ |
| DS07 | "Resolve all null, duplicate..." | `ready` | ___ | ☐ | ☐ |
| DS08 | "fix errors" (vague) | `needs_clarification` | ___ | ☐ | ☐ |
| DS09 | "Resolve all null, duplicate..." | `ready` | ___ | ☐ | ☐ |
| DS10 | "clean the data" (vague) | `needs_clarification` | ___ | ☐ | ☐ |

**Pass Rate:** ___/10

### Chi tiết — "needs_clarification" cases

#### DS03 — High NULL (vague prompt: "clean the data")

**Expected behavior:**
- Validator nhận ra prompt vơ quá, hỏi về chiến lược xử lý NULL
- `clarifications.null.Q1_strategy` có 3 options có prefix `(Recommended)`
- Q2, Q3 là insight về semantic pattern của null

**Actual output:**
```json
{
  "status": "___",
  "reasoning": "___",
  "clarifications": {
    "null": {
      "Q1_strategy": {
        "question": "___",
        "options": ["___", "___", "___"],
        "consequences": {}
      },
      "Q2_semantic_insight": { "question": "___" },
      "Q3_semantic_insight": { "question": "___" }
    }
  }
}
```

**Checklist:**
- [ ] Status = `needs_clarification`
- [ ] `null.Q1_strategy` có đúng 3 options
- [ ] Có prefix `(Recommended)` trên 1 option
- [ ] `consequences` là dict mapping option → hậu quả
- [ ] Q2 và Q3 không trùng lặp nhau

---

#### DS08 — High NULL + Typecast (vague prompt: "fix errors")

**Expected behavior:**
- Validator hỏi về cả NULL strategy và typecast strategy
- Đề cập đến vấn đề "25.3 °C" cần clean trước khi cast

**Actual output:** (điền sau khi chạy)

**Checklist:**
- [ ] Status = `needs_clarification`
- [ ] Có clarifications cho cả `null` VÀ `typecast`
- [ ] `typecast.Q2_semantic_insight` đề cập DMV trước khi cast

---

### Chi tiết — "ready" cases

#### DS01 — Full-row Duplicate

**Expected behavior:**
- Validator set `ready` vì prompt đủ cụ thể
- `action_plan.duplicate` có nội dung về dedup strategy
- `resolved_by_user` = ["null", "duplicate", "typecast"]

**Actual output:**
```json
{
  "status": "___",
  "action_plan": {
    "null": "___",
    "duplicate": "___",
    "typecast": "___"
  },
  "resolved_by_user": ["___"]
}
```

**Checklist:**
- [ ] Status = `ready`
- [ ] `action_plan.duplicate` không rỗng
- [ ] `resolved_by_user` có "duplicate"

---

#### DS09 — Mega Dirty (ALL ISSUES)

**Expected behavior:**
- Validator detect tất cả 3 issues: null, duplicate, typecast
- `action_plan` có plan cho cả 3
- Reasoning đề cập blood_pressure type mismatch ("120/80" → int)

**Actual output:** (điền sau khi chạy)

**Checklist:**
- [ ] Status = `ready`
- [ ] `action_plan.null` không rỗng
- [ ] `action_plan.duplicate` không rỗng
- [ ] `action_plan.typecast` không rỗng
- [ ] Reasoning đề cập `blood_pressure` hoặc `cost_vnd` type mismatch

---

## 4. Bugs / Issues Phát Hiện

| # | DS | Component | Mô tả | Severity | Status |
|---|-----|-----------|-------|----------|--------|
| 1 | DS02 | StatProfiler | `order_id` KHÔNG trong near_unique (ratio=0.7 < 0.8). Semantic profiler cần nhận dạng qua tên cột | Low | Open |
| 2 | | | | | |

---

## 5. Kết luận

### Profiler (StatisticalProfiler + ingest_to_canonical)
- **Pass Rate: 10/10 (100%)** ✅
- Detect đúng: full-row duplicate, high-null columns, pk candidates, near-unique
- **Điểm cần cải thiện**: PK duplicate theo business key (như `order_id` với ratio 0.7) cần semantic analyzer bổ sung

### Input Validator (LLM)
- **Pass Rate: ___/10** (điền sau khi chạy full pipeline)
- 

### Kiến nghị
1. Bổ sung logic detect PK dup khi cột có tên chứa "id", "code", "key" ngay cả khi unique_ratio < 0.8
2. Test thêm với dataset DS09 để xác nhận multi-issue detection
3. Xem xét thêm assertion về nội dung `action_plan` (không chỉ status)
