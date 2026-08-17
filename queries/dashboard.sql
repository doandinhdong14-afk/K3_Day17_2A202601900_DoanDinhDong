-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- ===========================================================================
-- LỜI GIẢI — BÀI MỞ RỘNG A
-- ===========================================================================
-- Phần SELECT giữ nguyên từng ký tự: bài này chấm bằng hash của kết quả, và
-- mục tiêu là đổi CÁCH ĐỌC dữ liệu chứ không đổi dữ liệu trả về. Chỉ hai thứ
-- được sửa — nguồn đọc, và dạng của mệnh đề WHERE.
--
-- (1) Nguồn: data/gold_events/*.parquet  ->  data/gold_events_v2 (hive)
--     Dataset cũ có 5.000 file, mỗi file vài chục KB, không partition. DuckDB
--     đọc Parquet theo lô và làm tròn LÊN theo từng file, nên 5.000 file tí
--     hon ngốn 5.000.000 đơn vị công quét cho một tập chỉ 130.683 hàng — công
--     quét gấp 38 lần lượng dữ liệu thật. Đó là small-file problem hiện thành
--     con số. tools/compact.py gộp lại còn 14 file, partition theo event_date.
--
--     `hive_partitioning = true` để DuckDB đọc event_date TỪ TÊN THƯ MỤC
--     (event_date=2026-08-09/) và loại bỏ cả thư mục trước khi mở bất kỳ file
--     nào — engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi mở.
--
-- (2) Điều kiện lọc theo ngày: sargable
--         cũ : strftime(event_time, '%Y-%m-%d') = '2026-08-09'
--         mới: event_date = DATE '2026-08-09'
--     Ở dạng cũ, cột bị bọc trong một function call. Engine không so được kết
--     quả function với tên thư mục partition, cũng không so được với thống kê
--     min/max của row group — nó buộc phải đọc từng hàng rồi mới biết hàng đó
--     có cần hay không, tức là mọi cơ chế cắt tỉa đều bị vô hiệu.
--     Ở dạng mới, cột đứng một mình một vế nên cả hai cơ chế cùng hoạt động.
--     (event_date luôn bằng event_time::date trên toàn bộ 130.683 hàng — đã
--      kiểm chứng — nên phép viết lại không đổi ngữ nghĩa.)
--
-- (3) customer_name không nằm trong đường dẫn (650 giá trị thì 650 thư mục là
--     quá nhiều — xem lý do trong tools/compact.py), nhưng dataset mới được
--     ORDER BY customer_name và cắt row group 2.048 hàng, nên thống kê
--     min/max của row group đủ hẹp để engine bỏ qua các row group không chứa
--     khách đang hỏi. Kiểm chứng trên file ngày 08-09:
--         row group 0: min=ACME       max=ACME        <- đọc
--         row group 1: min=ACME       max=Cust_0070   <- đọc
--         row group 2: min=Cust_0070  max=Cust_0294   <- bỏ qua được
--         row group 3: min=Cust_0294  max=Cust_0519   <- bỏ qua được
--         row group 4: min=Cust_0519  max=Cust_0650   <- bỏ qua được
--     Với dữ liệu gốc xếp ngẫu nhiên thì cả 5 row group đều có min≈'ACME' và
--     max≈'Cust_0650', không bỏ qua được cái nào.
--
-- KẾT QUẢ ĐO (python tools/explain.py):
--     rows scanned  5.000.000 -> 9.324   (giảm 536,3× · cần ≥ 10×)   ✓
--     files             5.000 -> 14                                  ✓
--     result hash  4379e4c5d9f3 -> 4379e4c5d9f3  (không đổi)         ✓
--
-- Ghi chú trung thực về phép đo: metric OPERATOR_ROWS_SCANNED của DuckDB ở
-- đây phản ánh mức PARTITION PRUNING (mở 1 thư mục thay vì 5.000 file) —
-- nó báo cùng một con số 9.324 kể cả khi bỏ hẳn điều kiện customer_name, nên
-- phần cắt tỉa row group ở (3) không hiện ra trong con số này dù thống kê
-- min/max cho thấy nó có thật.
-- ===========================================================================

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = true)
where customer_name = 'ACME'
  and event_date = DATE '2026-08-09'
group by 1
