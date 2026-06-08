# Dirty Dataset Manifest

| File | Domain | Primary Issues | Rows | Cols |
|------|--------|----------------|------|------|
| DS01_full_row_duplicate.csv | Bệnh nhân/Hospital | Full-row duplicate (500 hàng x 2) | 1000 | 10 |
| DS02_pk_duplicate.csv | Đơn hàng/E-commerce | PK duplicate (order_id, 300 cases) | 1000 | 10 |
| DS03_high_null.csv | Khảo sát/Survey | Null rate >70% trên 5+ cột | 1000 | 10 |
| DS04_null_and_dmv.csv | Nhân sự/HR | NULL thật + DMV (N/A, unknown, -) | 1000 | 10 |
| DS05_type_mismatch.csv | Giao dịch/Finance | Type mismatch: amount, date, bool sai kiểu | 1000 | 10 |
| DS06_mixed_type.csv | Điểm thi/Education | Mixed type: float+string+None trong cột số | 1000 | 10 |
| DS07_pk_dup_and_null.csv | Sản phẩm/Inventory | PK duplicate + NULL vừa phải (20-35%) | 1000 | 10 |
| DS08_null_and_typecast.csv | IoT Sensor | NULL cao (50-80%) + string sensor values | 1000 | 10 |
| DS09_all_issues.csv | Hồ sơ bệnh viện | ALL: full-dup + PK-dup + NULL + DMV + typecast | ~1000 | 10 |
| DS10_edge_cases.csv | Logistics | Constant col, tên có dấu cách, near-constant, mixed bool | 1000 | 10 |
