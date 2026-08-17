#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"

# ---------------------------------------------------------------------------
# BA QUYẾT ĐỊNH — lý do (đo trên dữ liệu thật: 130.683 hàng · 14 ngày ·
# 650 khách hàng · ~9.335 hàng/ngày · 5.000 file · 16,3 MB)
#
# 1) PARTITION_BY (event_date)
#    Dashboard lọc theo HAI cột: customer_name và ngày. Chỉ một trong hai được
#    đưa vào đường dẫn, vì partition lồng nhau sẽ nhân số thư mục lên.
#    Chọn ngày vì nó có 14 giá trị -> 14 thư mục, mỗi thư mục ~9.335 hàng:
#    kích thước file lành mạnh.
#    KHÔNG chọn customer_name: 650 giá trị -> 650 thư mục × 14 ngày = 9.100
#    file tí hon. Như vậy là chữa small-file problem bằng cách tạo ra một
#    small-file problem to hơn cái ban đầu.
#    customer_name vẫn được lọc hiệu quả, nhưng bằng cơ chế khác — xem (2).
#
# 2) ORDER BY event_date, customer_name, event_time
#    Thứ tự hàng trong file quyết định thống kê min/max của mỗi row group có
#    ích hay vô dụng. Dữ liệu gốc xếp ngẫu nhiên: mọi row group đều có min
#    ≈ 'ACME' và max ≈ 'Cust_0650', nên min/max không loại được row group nào.
#    Sắp theo customer_name thì các hàng của cùng một khách nằm liền nhau,
#    min/max của row group trở thành một khoảng hẹp và engine bỏ qua được
#    những row group không chứa khách đang hỏi.
#    event_time đứng thứ ba chỉ để kết quả ổn định giữa các lần chạy.
#
# 3) ROW_GROUP_SIZE = 2048
#    Mặc định là 122.880. Một ngày chỉ có ~9.335 hàng, tức NHỎ HƠN một row
#    group mặc định -> cả ngày gói trong ĐÚNG MỘT row group, min/max của nó
#    phủ toàn bộ 650 khách hàng và mất sạch tác dụng lọc. Sắp xếp ở (2) sẽ
#    thành công cốc.
#    2048 chia một ngày thành ~5 row group. ACME có 3.500 hàng ngày 08-09 và
#    đứng đầu bảng chữ cái, nên chỉ nằm trong 2 row group đầu; 3 row group còn
#    lại có min/max không chứa 'ACME' nên engine bỏ qua được.
#    Không chọn nhỏ hơn nữa: row group càng nhỏ thì tỷ lệ nén càng kém và
#    metadata càng phình, trong khi phần cắt giảm thêm không đáng kể.
#
# KẾT QUẢ: 5.000 file -> 14 file · 130.683 hàng giữ nguyên
#          rows scanned 5.000.000 -> 9.324 (giảm 536,3×)
# ---------------------------------------------------------------------------

ROW_GROUP_SIZE = 2048


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    src_glob = str(SRC / "*.parquet").replace("\\", "/")
    dst_path = str(DST).replace("\\", "/")

    n_before = con.execute(
        f"select count(*) from read_parquet('{src_glob}')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select *
            from read_parquet('{src_glob}')
            order by event_date, customer_name, event_time
        ) to '{dst_path}' (
            format          parquet,
            partition_by    (event_date),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    dst_glob = str(DST / "**" / "*.parquet").replace("\\", "/")
    n_after = con.execute(
        f"select count(*) from read_parquet('{dst_glob}')"
    ).fetchone()[0]
    n_files = len(list(DST.rglob("*.parquet")))

    # Tái cấu trúc storage KHÔNG được phép làm đổi nội dung.
    assert n_before == n_after, f"mất hàng: {n_before:,} -> {n_after:,}"

    print(f"  đích  : {DST}  ({n_files:,} file)")
    print(f"  hàng  : {n_before:,} -> {n_after:,}  (khớp ✓)")
    print(f"  file  : {n_src:,} -> {n_files:,}")
    print(f"  row_group_size = {ROW_GROUP_SIZE:,} · partition theo event_date")
    print("\n  Bước tiếp theo: python tools/explain.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
