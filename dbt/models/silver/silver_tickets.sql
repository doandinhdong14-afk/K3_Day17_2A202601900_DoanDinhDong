-- ---------------------------------------------------------------------------
-- silver_tickets — trạng thái mới nhất của mỗi ticket, dựng lại từ luồng CDC.
-- ---------------------------------------------------------------------------
-- Model này materialized = 'table': dựng lại toàn bộ mỗi lần chạy, nên luôn
-- ổn định.
--
-- Phần xử lý CDC dưới đây đã đúng và không cần sửa:
--   * mỗi ticket lấy bản ghi có (event_time, cdc_seq) lớn nhất  -> _rn = 1
--   * ticket có op = 'd' bị loại khỏi Silver
--
-- ===========================================================================
-- NHIỆM VỤ 3 — phần 2/3
-- ===========================================================================
-- Giá trị `priority` được chuẩn hoá bằng macro
-- `dbt/macros/normalize_priority.sql` — sửa logic quy đổi ở đó, không sửa ở
-- đây. Việc còn lại của file này là **loại bỏ những bản ghi không chuẩn hoá
-- được** ra khỏi Silver.
--
-- ⚠️ THỨ TỰ QUYẾT ĐỊNH SỐ HÀNG CỦA BẢNG.
--
--    Hiện tại: xếp hạng (row_number) trước, rồi mới chọn.
--    Nếu bạn chỉ thêm điều kiện lọc vào cuối, ticket nào có bản ghi MỚI NHẤT
--    bị hỏng sẽ biến mất hoàn toàn khỏi Silver — số ticket tụt từ 12.480
--    xuống 12.168, và gold_training_set hụt theo.
--
--    Đúng phải là: LỌC bỏ bản ghi hỏng trước → SAU ĐÓ mới xếp hạng.
--    Bạn loại BẢN GHI hỏng, không loại cả TICKET: ticket đó vẫn còn trạng
--    thái hợp lệ từ lần cập nhật trước.
--
--    `make verify` có một dòng riêng ("silver_tickets giữ đủ 12.480 ticket")
--    để bắt đúng lỗi này.
--
-- Xong file này thì sang models/silver/quarantine_tickets.sql (phần 3/3).
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- LỜI GIẢI — NHIỆM VỤ 3, phần 2/3
--
-- Pipeline được tách thành BA bước theo đúng thứ tự bắt buộc:
--     scored  -> tính priority_clean cho MỌI bản ghi CDC
--     valid   -> LỌC bỏ bản ghi macro trả về NULL   (bước then chốt)
--     ranked  -> XẾP HẠNG trên phần còn lại
--
-- Vì sao thứ tự này, đo trên dữ liệu thật:
--     lọc trước → rank sau  = 12.480 ticket  ✓ đúng kỳ vọng
--     rank trước → lọc sau  = 12.168 ticket  ✗ mất 312 ticket
--
-- Toàn bộ 312 bản ghi hỏng đều là op='u' (bản cập nhật). Nếu xếp hạng trước,
-- một bản cập nhật hỏng sẽ chiếm mất vị trí _rn = 1 rồi bị loại ở bước sau,
-- kéo theo cả ticket biến mất — trong khi ticket đó vẫn còn nguyên một trạng
-- thái hợp lệ từ lần cập nhật trước đó. Lọc trước thì bản ghi hỏng bị gạt ra
-- khỏi cuộc xếp hạng, và bản hợp lệ gần nhất lên thay.
--
-- Ta loại BẢN GHI hỏng, không loại cả TICKET.
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

with scored as (

    select
        *,
        {{ normalize_priority('priority_raw') }}             as priority_clean
    from {{ source('bronze', 'bronze_tickets_cdc') }}

),

valid as (

    -- Bản ghi không quy đổi được priority bị loại KHỎI CUỘC XẾP HẠNG.
    -- Chúng không biến mất khỏi hệ thống: quarantine_tickets nhặt đúng
    -- những bản ghi này bằng chính điều kiện ngược lại.
    select * from scored where priority_clean is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from valid

),

latest as (select * from ranked where _rn = 1)

select
    ticket_id,
    customer_id,
    customer_name,
    segment,
    priority_clean                                           as priority,
    category,
    channel,
    status,
    csat,
    first_response_sec,
    subject,
    body,
    event_time                                               as updated_at,
    _ingested_at
from latest
where op <> 'd'
