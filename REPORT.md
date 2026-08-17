# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đoàn Đình Đông  **MSV:** 2A202601900  
**Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

> **Ghi chú môi trường.** Máy chạy lab là Windows, không có `make`, nên mọi
> lệnh `make X` được thay bằng lời gọi Python tương đương
> (`.venv\Scripts\python.exe tools\verify.py`). Ngoài ra `verify.py` in ký tự
> `━ ✓ ✗`, mà console Windows mặc định dùng cp1252 nên crash với
> `UnicodeEncodeError`; phải đặt `PYTHONUTF8=1` và `PYTHONIOENCODING=utf-8`
> trước khi chạy. Cả hai đều là vấn đề môi trường, không phải thay đổi trong
> bài làm: `tools/verify.py`, `tools/explain.py`, `tools/common.py`,
> `seed/generate.py` và `expected/` được giữ nguyên vẹn — file duy nhất trong
> `tools/` bị sửa là `tools/compact.py`, đúng phần bài mở rộng A yêu cầu.

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 64.9s
  run 2/3 … 66.4s
  run 3/3 … 73.0s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Kết quả `crash-test` (bài mở rộng B):

```
  topic: 20,000 message · batch 500 · giết ở lô 7

  A. chạy một mạch, không sự cố      -> 20,000 hàng / 20,000 event_id khác nhau
  B. chạy và bị giết ở lô 7          -> thoát mã 137 · offset đã commit: 3,000
  C. khởi động lại, chạy nốt         -> 20,000 hàng / 20,000 event_id khác nhau

  ----------------------------------------------------------
  không mất bản ghi                 ✓
  không trùng bản ghi               ✓
  C == A                            ✓
  ----------------------------------------------------------
  BÀI MỞ RỘNG B: ĐẠT ✓
```

Tổng kết: **4 / 4 tiêu chí đạt**

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` — số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` — ổn định 3 lượt | ✓ | ✓ | ✓ |
| `gold_feature_daily` — số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` — ổn định 3 lượt | ✓ | ✓ | ✓ |
| `gold_doc_chunks` — số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` — số hàng | 312 | 312 | ✓ |
| `silver_tickets` — số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 11/11 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,73 ngày** | (ghi số) | ✓ |
| **Tổng verify** | 4/4 | 4/4 tiêu chí | ✓ |

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` = 38.750 hàng thay vì 12.480. Chạy lại pipeline thì con số tiếp tục tăng. Không có lỗi nào được báo, `dbt test` vẫn 9/9 pass. |
| **Nguyên nhân** | Model khai `materialized='incremental'` nhưng **không khai `unique_key`**. Thiếu khoá, dbt không có cách nào biết "hàng nào là cùng một hàng", nên nó generate ra `INSERT INTO ... SELECT` chứ không phải một phép ghi có thay thế. Chạy lại cùng một partition ngày vì thế là **ghi thêm**, không phải ghi đè. Nói cách khác: bản thân phép ghi không idempotent, nên **mọi** cơ chế retry ở tầng trên — `retries=2` trong `DEFAULT_ARGS`, nút Clear Task của Airflow, lần chạy bù của scheduler — đều bị biến từ cơ chế *phục hồi* thành cơ chế *nhân bản*. Đây là lý do sự cố không phát ra tín hiệu nào: về mặt kỹ thuật mọi lệnh đều chạy thành công. |
| | **Vì sao `delete+insert` theo ngày không cứu được.** Nguồn CDC có 1.310 bản ghi `op='u'`. Một ticket tạo ngày D1 rồi bị sửa ngày D2 sẽ lọt qua mệnh đề `WHERE _ingested_at` ở **hai partition ngày khác nhau** trong cùng một lượt chạy, nên xoá-partition-rồi-ghi-lại vẫn để lại hai hàng cho một ticket. Grain của bảng là **entity** (1 hàng / 1 ticket), khoá tự nhiên là `ticket_id`, nên chiến lược đúng là `merge` theo khoá đó — không phải theo partition ngày. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql` — thêm `unique_key='ticket_id'` và `incremental_strategy='merge'` vào `config()`. Mệnh đề `WHERE` theo `run_date` giữ nguyên (nó là tối ưu backfill, không phải lỗi).<br>`dags/ai_training_pipeline.py` — `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: **38.750** hàng, tăng thêm mỗi lượt · sau: **12.480** hàng, checksum `8622572a97` **giống hệt cả ba lượt** · `gold_training_set: 1 hàng / 1 ticket` ✓ không lặp. |

> **Hai tham số DAG không phải root cause.** `catchup=True` khiến Airflow tự
> schedule chạy bù mọi ngày quá khứ, và `max_active_runs` không giới hạn cho
> phép nhiều run ghi đồng thời vào cùng một bảng. Cả hai chỉ **tăng tần suất
> kích hoạt** lỗi. Sửa DAG mà không sửa model thì bảng vẫn phình — chỉ là
> phình chậm hơn. Root cause nằm ở tính không-idempotent của phép ghi.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` = 8.645 / 9.100 hàng (thiếu 455 ≈ 5%). Cột `ỔN ĐỊNH` vẫn ✓ — bảng chạy lại cho cùng kết quả, nhưng kết quả đó sai. Chỉ thiếu ở những ngày đã chạy xong từ lâu; ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2,73 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2,73 ngày, làm tròn lên ngày nguyên gần nhất. |
| **Nguyên nhân** | Điều kiện lọc incremental là `where event_date > (select max(event_date) from {{ this }})`. Nó lấy mốc theo **thời điểm sự kiện xảy ra** (`event_date`), trong khi đại lượng quyết định dữ liệu *đã có mặt trong kho hay chưa* là **thời điểm dữ liệu tới kho** (`_ingested_at`). Hai đại lượng này lệch nhau tới 3 ngày với ~5% bản ghi. Hệ quả: một event xảy ra 08-12 nhưng tới kho 08-15 gặp `max(event_date)` trong bảng đích đã là 08-14, nên `08-12 > 08-14` sai và nó bị bỏ qua. Và vì mốc chỉ **tăng đơn điệu**, không bao giờ lùi lại, nên bản ghi đó bị bỏ qua **vĩnh viễn** — không có lượt chạy nào sau này còn nhìn tới nó nữa. Đó là lý do lỗi chỉ hiện ở ngày cũ và bảng vẫn "ổn định": nó ổn định ở trạng thái thiếu. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql` — hai thay đổi, thiếu một là hỏng:<br>(a) nới cửa sổ: `where event_date >= (select coalesce(max(event_date), DATE '1900-01-01') from {{ this }}) - interval 3 day`<br>(b) thêm `unique_key=['event_date','customer_id']` + `incremental_strategy='delete+insert'`. |
| **Bằng chứng** | trước: **8.645** hàng · sau: **9.100** hàng (= 14 ngày × 650 khách) · checksum giống nhau cả ba lượt · `gold_training_set` không bị ảnh hưởng, vẫn 12.480. |

**Số liệu đo được trên `bronze_events`** (129.462 bản ghi, ngày ingest 08-03…08-16):

| p50 | p95 | **P99** | max | tỷ lệ tới muộn > 1 ngày |
|---|---|---|---|---|
| 0,13 ngày | 1,81 ngày | **2,73 ngày** | 2,94 ngày | 5,05 % |

Phân bố độ trễ theo ngày nguyên: `0 ngày = 108.862` · `1 ngày = 14.165` ·
`2 ngày = 3.842` · `3 ngày = 2.593`. Phân bố có hai cụm tách biệt — phần lớn
tới trong ngày, một đuôi nhỏ kéo tới 3 ngày.

**Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?**

> Lookback không phải chi phí trả một lần — nó là chi phí trả ở **mọi lượt
> chạy về sau**. Mỗi ngày lùi thêm là ~650 cặp `(ngày, khách)` bị tính lại mỗi
> đêm, vĩnh viễn, cộng thêm phần đọc lại từ `silver_events`. Bám theo `max` là
> để **một** giá trị ngoại lai duy nhất quyết định chi phí thường trực của
> pipeline: hôm nào có một bản ghi trễ 30 ngày thì window phải là 30 ngày, và
> con số đó không bao giờ giảm xuống được nữa.
>
> P99 cho một mốc **ổn định theo thống kê**: nó phủ 99% dữ liệu với chi phí đo
> được, và không nhảy theo từng sự cố đơn lẻ. Phần 1% còn lại không bị bỏ mặc
> mà được xử lý bằng **cơ chế khác**: giám sát tỷ lệ bản ghi rơi ngoài window
> và backfill thủ công khi cần. Đây là nguyên tắc chung — dùng phân vị cho
> đường chạy thường ngày, dùng quy trình cho phần đuôi.
>
> Ở bộ dữ liệu này P99 (2,73) và max (2,94) đều làm tròn lên thành 3 ngày nên
> hai lựa chọn trùng nhau; nhưng lý do chọn thì khác nhau, và trên dữ liệu
> thật hai con số hiếm khi gần nhau như vậy.

**Vì sao riêng việc nới window là chưa đủ.** Window rộng hơn nghĩa là cùng một
cặp `(event_date, customer_id)` được tính lại ở nhiều lượt chạy. Nếu model chỉ
biết `insert`, các lần tính sẽ cộng dồn — tức là **tái tạo đúng lỗi của nhiệm
vụ 1 trên một bảng khác**. Grain ở đây gồm hai cột nên `unique_key` phải là
một list. Chọn `delete+insert` chứ không phải `merge` vì đây là bảng **tổng
hợp**: cần thay thế nguyên cụm giá trị đã tính, không phải vá từng cột.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Model phân loại dự đoán kém hẳn từ 08-10, nhưng pipeline không dừng và `dbt test` vẫn 9/9 pass. `silver_tickets.priority` có 6.606 / 12.480 hàng NULL hoặc ngoài miền 1..4; `quarantine_tickets` rỗng. |
| **Nguyên nhân** | Macro chuẩn hoá là `try_cast(priority_raw as integer)` — và nó sai theo **hai hướng ngược nhau cùng lúc**. (i) Khi team backend đổi cách biểu diễn từ số sang nhãn chữ hôm 08-10, `try_cast('urgent')` trả về `NULL`, nên **7.142 bản ghi hoàn toàn hợp lệ bị mất giá trị** — model mất gần hết tín hiệu `priority` từ đúng ngày đó. (ii) Ngược lại, `try_cast` **chấp nhận** `'0'`, `'5'`, `'-1'` vì chúng đúng là integer, nên dữ liệu rác lọt thẳng vào Silver. Nói ngắn: `try_cast` kiểm tra **kiểu** nhưng không kiểm tra **miền giá trị**, và nó coi mọi thứ không ép kiểu được là rác thay vì hỏi "có phải cùng một thông tin, chỉ khác cách biểu diễn không?". <br><br>Sâu hơn một tầng: sự cố **im lặng** được vì `contract.enforced: false` và không có test miền giá trị nào trên cột `priority`. Một thay đổi schema phía nguồn có thể đi xuyên qua toàn bộ pipeline mà không chạm vào một cơ chế cảnh báo nào. Pipeline "không dừng" không phải vì nó khoẻ, mà vì nó **không có giác quan để phát hiện**. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | xem bảng bên dưới |
| **Cách khắc phục** | **(a)** `dbt/macros/normalize_priority.sql` — thay `try_cast` bằng `CASE` ba nhóm; nhánh số kèm `between 1 and 4`; nhãn chữ map theo tài liệu API; còn lại `null`.<br>**(b)** `dbt/models/silver/silver_tickets.sql` — tách thành `scored → valid → ranked`: **lọc bản ghi hỏng TRƯỚC, xếp hạng SAU**.<br>**(c)** `dbt/models/silver/quarantine_tickets.sql` — `where {{ normalize_priority('priority_raw') }} is null` (dùng đúng macro của Silver).<br>**(d)** `dbt/models/silver/schema.yml` — `contract.enforced: true`, thêm `not_null` + `accepted_values [1,2,3,4]` cho cột `priority`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (đúng grain: 1 hàng / 1 bản ghi CDC) · `dbt test` **11/11 pass** (bản gốc 9) · `silver_tickets.priority` **sạch**, không NULL và luôn ∈ 1..4 · `silver_tickets` vẫn đủ **12.480** ticket · `gold_training_set` giữ nguyên 12.480. |

### Ba nhóm giá trị (đo trên 14.300 bản ghi CDC)

| Nhóm | Giá trị (số bản ghi) | Bản chất | Xử lý | Tổng |
|---|---|---|---|---|
| **1 — số hợp lệ** | `1`=1.705 · `2`=1.683 · `3`=1.710 · `4`=1.748 | Đúng contract ban đầu | giữ nguyên | **6.846** |
| **2 — nhãn chuỗi** | `urgent`=1.819 · `high`=1.695 · `medium`=1.783 · `low`=1.845 | **Schema evolution** — nguồn đổi cách biểu diễn từ 08-10, ý nghĩa không đổi | **map** urgent→1, high→2, medium→3, low→4 | **7.142** |
| **3 — hỏng thật** | `0`=49 · `''`=43 · `unknown`=39 · `P1`=39 · `P2`=38 · `5`=37 · `NULL`=35 · `-1`=32 | Dữ liệu lỗi | **quarantine** | **312** |

Tiêu chí phân biệt nhóm 2 và nhóm 3: *giá trị này có mang đúng thông tin của
contract cũ, chỉ khác cách biểu diễn hay không?* Có thì map, không thì
quarantine. Xử lý nhóm 2 như nhóm 3 sẽ đẩy `quarantine_tickets` lên hàng nghìn
hàng và vứt bỏ 7.142 bản ghi tốt chỉ vì nguồn đổi format.

### Vì sao thứ tự "lọc trước, xếp hạng sau" quyết định số hàng

Toàn bộ 312 bản ghi hỏng đều là `op='u'` (bản cập nhật). Nếu xếp hạng trước
rồi mới lọc, một bản cập nhật hỏng chiếm mất vị trí `_rn = 1` rồi bị loại ở
bước sau, **kéo cả ticket biến mất** — trong khi ticket đó vẫn còn nguyên một
trạng thái hợp lệ từ lần cập nhật trước. Đo trực tiếp trên dữ liệu:

| Thứ tự | Số ticket trong Silver |
|---|---|
| lọc trước → xếp hạng sau | **12.480** ✓ |
| xếp hạng trước → lọc sau | 12.168 ✗ *(mất 312 ticket)* |

Ta loại **bản ghi** hỏng, không loại cả **ticket**.

### Câu hỏi thiết kế

**1. Nên chặn dữ liệu lỗi ở tầng Bronze hay tầng Silver?**

> Ở **Silver**. Bronze phải là bản sao trung thực của những gì nguồn đã gửi —
> đó là toàn bộ giá trị của nó. Nếu Bronze từ chối bản ghi lỗi thì ta **phá
> huỷ bằng chứng ngay tại thời điểm sự cố xảy ra**: về sau không còn cách nào
> biết nguồn thực sự đã gửi gì, không phân biệt được "nguồn gửi sai" với "ta
> parse sai", không replay lại được sau khi sửa logic, và không dựng lại được
> mốc thời gian để biết thay đổi bắt đầu từ hôm nào. Trong sự cố này, chính vì
> Bronze giữ nguyên `priority_raw` dạng VARCHAR mà ta mới truy ra được ngày
> 08-10 và đếm được chính xác 7.142 nhãn chữ so với 312 bản ghi hỏng.
>
> Nguyên tắc: Bronze *ghi nhận*, Silver *phán xét*. Việc phán xét cần được
> phép sai và được phép sửa lại — mà muốn sửa lại thì dữ liệu gốc phải còn.

**2. Vì sao không để `dbt test` fail và dừng cả DAG khi gặp bản ghi lỗi?**

> Cân nhắc quy mô: **312 bản ghi hỏng trên 14.300** (2,2%). Để 312 bản ghi đó
> chặn 12.168 ticket, hơn 130.000 event và 31.200 chunk hoàn toàn bình thường
> không đến được tay người dùng là một đánh đổi sai — nó biến một sự cố chất
> lượng dữ liệu cục bộ thành một sự cố ngừng dịch vụ toàn hệ thống.
>
> Ngoài ra, dừng DAG **không** làm dữ liệu lỗi biến mất; nó chỉ chuyển vấn đề
> thành một sự cố vận hành lúc 2 giờ sáng, và tạo áp lực xử lý vội — mà cách
> xử lý vội phổ biến nhất là tắt test đi cho DAG chạy tiếp, tức là mất luôn cả
> cơ chế phát hiện.
>
> Cách đúng là **tách riêng và chạy tiếp**: bản ghi lỗi vào `quarantine_tickets`
> kèm `reject_reason` đọc được, pipeline phục vụ tiếp phần hợp lệ, và hàng đợi
> quarantine được người trực xử lý trong giờ làm việc. Dừng pipeline là phản
> ứng đúng khi lỗi mang tính **hệ thống** (contract vỡ hoàn toàn, tỷ lệ lỗi
> vọt lên hàng chục phần trăm) — không phải khi tỷ lệ lỗi nhỏ và đã cô lập
> được. Ngưỡng đó nên là một con số được khai báo, không phải một phản xạ.
>
> Đây cũng là lý do cần **cả** contract **lẫn** test, chứ không phải một trong
> hai: contract ràng buộc **kiểu** và dừng model khi kiểu sai — nó bắt được
> đúng loại sự cố "backend đổi kiểu cột" mà lần này ta đã bỏ lọt. Test ràng
> buộc **miền giá trị** — contract một mình vẫn cho `priority = 99` đi qua vì
> 99 đúng là integer.

---

## 4 · Bài mở rộng

### Bài A — Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard load 38 giây, ba tháng trước 2 giây, không ai sửa dòng code nào. |
| **Nguyên nhân** | Hai lỗi độc lập cộng lại. **(i) Small-file problem.** `data/gold_events/` tích tụ thành **5.000 file** cho vỏn vẹn 130.683 hàng (16,3 MB — trung bình ~26 hàng/file). DuckDB đọc Parquet theo lô và làm tròn LÊN theo từng file, nên mỗi file vài chục hàng vẫn tốn khối lượng đọc tương đương ~1.000 hàng: **5.000.000 đơn vị công quét cho 130.683 hàng thật**, tức gấp 38 lần lượng dữ liệu. Đây là kiểu sự cố "không ai sửa gì mà vẫn chậm dần" — nó tích tụ theo thời gian chứ không do một thay đổi nào. **(ii) Predicate không sargable.** `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call, nên engine không so được kết quả function với tên thư mục partition, cũng không so được với thống kê min/max của row group — mọi cơ chế cắt tỉa đều bị vô hiệu và nó buộc phải đọc từng hàng rồi mới biết hàng đó có cần hay không. |
| **Cách khắc phục** | `tools/compact.py` — `COPY ... TO 'data/gold_events_v2' (PARTITION_BY (event_date), ROW_GROUP_SIZE 2048)` với `ORDER BY event_date, customer_name, event_time`.<br>`queries/dashboard.sql` — đọc dataset mới với `hive_partitioning = true`, và viết lại điều kiện ngày thành `event_date = DATE '2026-08-09'` (cột đứng một mình một vế). |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324** (giảm **536,3×**, cần ≥ 10×) · `files` **5.000 → 14** · `result hash` **4379e4c5d9f3 → 4379e4c5d9f3** không đổi · 130.683 hàng giữ nguyên. |

**Ba quyết định và lý do:**

- **Partition theo `event_date`** (14 giá trị → 14 thư mục, ~9.335 hàng/thư mục).
  Không partition theo `customer_name`: 650 giá trị × 14 ngày = 9.100 file tí
  hon — tức là chữa small-file problem bằng cách tạo ra một small-file problem
  lớn hơn cái ban đầu.
- **`ORDER BY customer_name`** để các hàng của cùng một khách nằm liền nhau,
  nhờ đó thống kê min/max của row group trở thành một khoảng hẹp và có tác
  dụng lọc. Với dữ liệu xếp ngẫu nhiên, mọi row group đều có
  min ≈ `ACME`, max ≈ `Cust_0650` — min/max tồn tại nhưng vô dụng.
- **`ROW_GROUP_SIZE = 2048`** thay vì mặc định 122.880. Một ngày chỉ có ~9.335
  hàng, **nhỏ hơn một row group mặc định**, nên cả ngày sẽ gói trong đúng một
  row group và min/max của nó phủ toàn bộ 650 khách — việc sắp xếp ở trên sẽ
  thành công cốc. 2048 chia một ngày thành 5 row group; kiểm chứng trên file
  ngày 08-09: row group 0–1 chứa `ACME`, row group 2–4 có min/max không chứa
  `ACME` nên bỏ qua được.

> **Ghi chú trung thực về phép đo.** Metric `OPERATOR_ROWS_SCANNED` mà
> `tools/explain.py` dùng phản ánh mức **partition pruning** (mở 1 thư mục
> thay vì 5.000 file): nó báo cùng con số 9.324 kể cả khi bỏ hẳn điều kiện
> `customer_name`. Phần cắt tỉa row group vì thế không hiện ra trong con số
> này, dù thống kê min/max ở trên cho thấy nó có thật.

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `crash-test` giết consumer ở lô 7 rồi khởi động lại: **mất** dữ liệu (lô 7 không bao giờ được ghi). |
| **Nguyên nhân** | Thứ tự thao tác là `commit() → crash → write()`. Commit offset là lời tuyên bố "mọi message tới vị trí này đã xử lý xong", nhưng nó được nói **trước khi** điều đó thành sự thật. Tiến trình chết ở giữa hai bước thì offset đã dịch sang lô 8 trong khi dữ liệu lô 7 chưa hề chạm tới kho; lần khởi động lại đọc từ offset 3.500 và 500 message bốc hơi vĩnh viễn — không có gì trong hệ thống ghi nhận rằng chúng từng tồn tại. Đây là ngữ nghĩa **at-most-once**, và mất dữ liệu im lặng là dạng hỏng tệ nhất trong ba dạng. |
| **Cách khắc phục** | `ingest/consumer.py`, hai phần bắt buộc đi cùng nhau:<br>**(a)** đảo thành `write_batch() → crash → commit()` — chuyển sang **at-least-once**;<br>**(b)** thêm `primary key` cho `event_id` trong `DDL` và đổi `INSERT` thành `insert ... on conflict (event_id) do update set ...` — làm phép ghi **idempotent**. |
| **Bằng chứng** | `crash-test`: A = 20.000 hàng / 20.000 event_id · B = chết ở lô 7, offset commit 3.000 · C = ghi tiếp 17.000 message → **20.000 hàng / 20.000 event_id khác nhau**. Không mất ✓ · không trùng ✓ · C == A ✓ → **BÀI MỞ RỘNG B: ĐẠT ✓**. `verify` vẫn 4/4, ba nhiệm vụ chính không bị ảnh hưởng. |

**Vì sao (a) một mình là chưa đủ.** Đảo thứ tự chỉ đổi at-most-once thành
at-least-once — tức đổi "mất dữ liệu" lấy "trùng dữ liệu". Lô 7 chắc chắn
được phát lại sau restart, và với `INSERT` thuần thì phát lại nghĩa là thêm
500 hàng trùng. **Exactly-once không tồn tại ở tầng giao vận**; thứ chọn được
là **at-least-once cộng với một phép ghi idempotent**, và (b) chính là nửa
còn lại đó.

**`DO UPDATE` khác `DO NOTHING` ở điểm nào khi một message được replay với
nội dung đã đổi?**

> `DO NOTHING` giữ nguyên bản ghi cũ và **âm thầm đánh rơi bản cập nhật**: kho
> lệch với nguồn mà không có dấu hiệu nào — đúng kiểu hỏng im lặng mà cả lab
> này đang chống. `DO UPDATE` luôn cho ra trạng thái của lần phát gần nhất,
> nên kho **hội tụ** về đúng nguồn.
>
> Với dữ liệu chỉ-ghi-thêm (message bất biến, replay y hệt bản cũ) hai lựa
> chọn cho kết quả như nhau. Nhưng `DO UPDATE` đúng trong **cả hai** trường
> hợp, còn `DO NOTHING` chỉ đúng trong một — nên không có lý do gì chọn cái
> kia. Chọn `DO UPDATE`.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline **hai lần liên tiếp** rồi so số hàng và checksum. Một hệ thống không cho cùng kết quả khi chạy lại thì mọi cơ chế retry ở tầng trên đều là cơ chế nhân bản — và điều đó không phát ra lỗi nào. Cụ thể với dbt: mọi model `incremental` phải khai đủ `unique_key` và `incremental_strategy`; thiếu khoá là đủ để kết luận model đang `INSERT`. |
| 2 | Đo **phân bố của `_ingested_at − event_time`**, không chỉ nhìn giá trị trung bình. Rồi đối chiếu: mốc mà bộ lọc incremental đang dùng có phải là đại lượng quyết định "dữ liệu đã có mặt chưa" không? Lọc theo thời điểm *sự kiện xảy ra* trong khi dữ liệu tới kho muộn là lỗi bỏ sót vĩnh viễn, và nó không bao giờ tự lộ ra vì bảng vẫn "ổn định". |
| 3 | Đối chiếu **phân bố giá trị của cột ở Silver với chính cột đó ở Bronze**, kèm mốc thời gian. Chênh lệch giữa hai phân bố là chỗ logic chuẩn hoá đang âm thầm nuốt dữ liệu. Và kiểm tra ngay: contract có bật không, cột quan trọng có test miền giá trị không — nếu không thì hệ thống **không có giác quan** để phát hiện nguồn đổi schema, và sự cố tiếp theo cũng sẽ im lặng đúng như lần này. |
