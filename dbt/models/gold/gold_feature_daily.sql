-- ---------------------------------------------------------------------------
-- gold_feature_daily — đặc trưng theo ngày cho agent định tuyến.
-- Grain: 1 hàng / 1 cặp (event_date, customer_id).
-- ---------------------------------------------------------------------------
-- KHUNG THỰC HIỆN — NHIỆM VỤ 2
--
--   Mỗi ngày vận hành, model chỉ tính lại phần "mới". Định nghĩa "mới" nằm ở
--   khối is_incremental() bên dưới:
--
--       WHERE event_date <toán tử> (
--                 SELECT max(event_date) FROM <bảng đích>
--             ) <lùi lại bao nhiêu ngày?>
--
--   Câu hỏi cần trả lời trước khi sửa:
--     1. Đo phân bố của (_ingested_at - event_time) trong bronze_events.
--        P99 bằng bao nhiêu? Bao nhiêu phần trăm bản ghi tới kho muộn hơn
--        một ngày so với lúc sự kiện xảy ra?
--     2. Một bản ghi có event_date = 08-12 nhưng _ingested_at = 08-15: hôm
--        08-15, max(event_date) trong bảng đích đang là bao nhiêu? Bản ghi
--        đó có thoả điều kiện lọc hiện tại không? Ngày hôm sau thì sao?
--     3. Window tính lại nên lùi bao nhiêu ngày? Căn cứ vào P99 hay vào max?
--        Mỗi ngày lùi thêm phải trả giá gì ở MỌI lượt chạy sau này?
--     4. Khi window mở rộng, cùng một (event_date, customer_id) sẽ được tính
--        lại nhiều lần. Cần thêm gì vào config() để lần tính sau THAY THẾ
--        lần tính trước thay vì cộng dồn? Grain này có mấy cột khoá?
--
--   Cảnh báo: sửa điều kiện lọc mà không xử lý ý 4 sẽ làm bảng mất tính ổn
--   định — make verify in riêng hai cột "ỔN ĐỊNH" và "SỐ HÀNG" để bạn thấy
--   rõ hai vấn đề này tách nhau.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- LỜI GIẢI — NHIỆM VỤ 2
--
-- Đo được trên bronze_events (129.462 bản ghi, ngày ingest 08-03..08-16):
--     p50 = 0,13 ngày · p95 = 1,81 ngày · P99 = 2,73 ngày · max = 2,94 ngày
--     tỷ lệ tới kho muộn hơn 1 ngày = 5,05%
--     phân bố trễ: 0 ngày=108.862 · 1 ngày=14.165 · 2 ngày=3.842 · 3 ngày=2.593
--
-- (1) unique_key = ['event_date','customer_id'] + delete+insert
--     Grain của bảng có HAI cột. Khi window được nới ra, cùng một cặp
--     (ngày, khách) sẽ được tính lại ở nhiều lượt chạy; không khai khoá thì
--     dbt sinh INSERT và các lần tính cộng dồn — đúng lỗi của nhiệm vụ 1
--     tái diễn trên bảng khác. delete+insert (chứ không phải merge) vì đây là
--     bảng tổng hợp: cần THAY THẾ nguyên cụm, không phải vá từng cột.
--
-- (2) Lookback 3 ngày, so sánh >= thay vì >
--     Căn cứ P99 = 2,73 ngày -> làm tròn lên 3 ngày.
--     Chọn P99 chứ không chọn max vì lookback là chi phí trả ở MỌI lượt chạy
--     về sau, không phải một lần: mỗi ngày lùi thêm là ~650 cặp phải tính lại
--     mỗi đêm, vĩnh viễn. Phần đuôi vượt window được xử lý bằng cảnh báo/
--     backfill thủ công, không bằng cách nới window cho mọi ngày.
-- ---------------------------------------------------------------------------

{{ config(
    materialized         = 'incremental',
    unique_key           = ['event_date', 'customer_id'],
    incremental_strategy = 'delete+insert',
    on_schema_change     = 'fail'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
-- Lookback window = 3 ngày (P99 độ trễ = 2,73 ngày, làm tròn lên).
-- coalesce để lượt chạy đầu trên bảng rỗng không cho ra NULL (NULL nuốt sạch
-- mọi hàng vì `event_date >= NULL` không bao giờ đúng).
where event_date >= (
        select coalesce(max(event_date), DATE '1900-01-01') from {{ this }}
      ) - interval 3 day
{% endif %}

group by 1, 2, 3, 4
