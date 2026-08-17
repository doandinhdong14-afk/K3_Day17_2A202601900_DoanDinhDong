{#
    ==========================================================================
    NHIỆM VỤ 3 — phần 1/3.  Đây là nơi bạn sửa.
    ==========================================================================

    Macro trong dbt = một đoạn SQL đặt tên, dùng lại được ở nhiều model.
    Gọi nó bằng {{ normalize_priority('priority_raw') }} và dbt sẽ chèn nội
    dung bên dưới vào đúng chỗ đó.

    Macro này đang được dùng ở HAI nơi:
        models/silver/silver_tickets.sql      -> để lấy giá trị đã chuẩn hoá
        models/silver/quarantine_tickets.sql  -> để tìm bản ghi KHÔNG chuẩn
                                                 hoá được
    Nhờ vậy hai model không thể lệch nhau: sửa ở đây là cả hai cùng đổi.

    --------------------------------------------------------------------------
    Cột `priority` phải là số nguyên 1..4. Hãy xem nguồn đang gửi gì:

        select priority_raw, count(*) from bronze_tickets_cdc group by 1 order by 2 desc;

    Bạn sẽ thấy BA nhóm giá trị, và ba nhóm này KHÔNG xử lý giống nhau:

      Nhóm 1   '1' '2' '3' '4'
               Đúng contract cũ.                            -> GIỮ NGUYÊN

      Nhóm 2   'urgent' 'high' 'medium' 'low'
               Từ 2026-08-10 team backend đổi cách ghi: dùng nhãn chữ thay
               cho số. Ý nghĩa KHÔNG đổi, chỉ đổi cách biểu diễn.
               Theo tài liệu API: urgent=1, high=2, medium=3, low=4.
                                                            -> QUY VỀ SỐ

      Nhóm 3   'P1' 'unknown' '0' '5' '-1' '' NULL
               Dữ liệu hỏng thật.                           -> TRẢ VỀ NULL
               (NULL ở đây là tín hiệu "không hợp lệ" — quarantine_tickets
                dùng chính tín hiệu đó để nhặt bản ghi lỗi ra.)

    ⚠️ Lỗi hay gặp nhất: xử lý nhóm 2 như nhóm 3. Nếu bạn để nhãn chữ rơi
       vào NULL thì quarantine sẽ có hàng nghìn hàng thay vì vài trăm, và
       bạn vừa vứt đi một nửa dữ liệu tốt chỉ vì nguồn đổi format.

    ⚠️ Chú ý `try_cast` hiện tại sai theo HAI hướng ngược nhau: nó biến nhãn
       chữ thành NULL, ĐỒNG THỜI lại chấp nhận '0', '5', '-1' vì chúng đúng
       là số — dù contract nói chỉ 1..4.
    ==========================================================================
#}

{#
    LỜI GIẢI — nhóm 1 giữ nguyên, nhóm 2 quy về số, nhóm 3 trả NULL.

    Thứ tự các nhánh WHEN là phần quan trọng nhất:

      * Nhánh số phải kèm `between 1 and 4`. Nếu chỉ try_cast không thôi thì
        '0', '5', '-1' vẫn lọt vào Silver — chúng đúng là integer, chỉ sai
        MIỀN GIÁ TRỊ. Đây chính là hướng sai thứ hai của try_cast cũ.
      * Nhánh nhãn chữ đặt sau, vì try_cast của 'urgent' đã là NULL nên
        không tranh chấp với nhánh trên.
      * Mọi thứ còn lại ('P1', 'P2', 'unknown', '', NULL) rơi xuống else null.

    lower(trim(...)) để không phụ thuộc hoa/thường và khoảng trắng thừa —
    hai thứ mà một nguồn upstream đổi format hay mang theo.

    Đo trên dữ liệu thật: nhóm 1 = 6.846 · nhóm 2 = 7.142 · nhóm 3 = 312.
#}
{% macro normalize_priority(col) %}
    case
        -- Nhóm 1: đã là số VÀ nằm trong miền hợp lệ của contract.
        when try_cast(trim({{ col }}) as integer) between 1 and 4
            then try_cast(trim({{ col }}) as integer)
        -- Nhóm 2: schema evolution — nguồn đổi cách biểu diễn từ 2026-08-10.
        -- Ý nghĩa không đổi, nên quy đổi theo tài liệu API chứ không vứt đi.
        when lower(trim({{ col }})) = 'urgent' then 1
        when lower(trim({{ col }})) = 'high'   then 2
        when lower(trim({{ col }})) = 'medium' then 3
        when lower(trim({{ col }})) = 'low'    then 4
        -- Nhóm 3: dữ liệu hỏng thật. NULL = tín hiệu "không hợp lệ" mà
        -- quarantine_tickets dùng để nhặt bản ghi lỗi ra.
        else null
    end
{% endmacro %}


{#
    Lý do bị loại — để người trực đọc log là hiểu ngay phải làm gì.
    Bắt đầu bằng một câu chung cũng được; phân biệt được vài loại lỗi thì tốt
    hơn (rỗng / NULL / là số nhưng ngoài khoảng / là chuỗi lạ).
#}
{% macro priority_reject_reason(col) %}
    case
        when {{ col }} is null              then 'priority NULL từ nguồn'
        when trim({{ col }}) = ''           then 'priority rỗng'
        when try_cast(trim({{ col }}) as integer) is not null
            then 'priority là số nhưng ngoài miền 1..4 (nhận: '
                 || trim({{ col }}) || ')'
        else 'priority là chuỗi không nằm trong bảng quy đổi (nhận: '
             || trim({{ col }}) || ')'
    end
{% endmacro %}
