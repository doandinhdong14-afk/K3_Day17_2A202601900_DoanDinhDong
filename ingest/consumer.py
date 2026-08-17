#!/usr/bin/env python3
"""Consumer đọc topic `ai-events` và ghi xuống bảng stream — NHIỆM VỤ 5.

Chạy tay:
    python ingest/consumer.py --db data/crash/crash.duckdb \
        --topic data/crash/topic.jsonl --offset data/crash/offsets.json

Kịch bản sự cố (tools/crash_test.py tự lo):
    thêm --crash-at-batch 7  -> tiến trình tự chết ở lô thứ 7, y hệt kill -9.

KHUNG THỰC HIỆN — NHIỆM VỤ 5

  Chạy `make crash-test` trước. Đọc kết quả: bạn MẤT bản ghi hay bạn có bản
  ghi TRÙNG? Con số đó xác định consumer đang ở ngữ nghĩa nào.

      at-most-once   : commit offset TRƯỚC khi ghi  -> crash = mất dữ liệu
      at-least-once  : commit offset SAU khi ghi    -> crash = trùng dữ liệu
      exactly-once   : không tồn tại ở tầng giao vận

  Hai hạng mục cần xử lý, thiếu một là chưa đủ:

    (a) Thứ tự thao tác trong consume() — xem khối được đánh dấu bên dưới.
        Đổi thứ tự chuyển ngữ nghĩa từ nhóm này sang nhóm kia. Câu hỏi: nếu
        tiến trình chết ở điểm maybe_crash(), lô hiện tại đã được ghi chưa,
        offset đã dịch chưa, và lần khởi động lại sẽ đọc từ đâu?

    (b) Tính idempotent của write_batch() — đổi thứ tự ở (a) khiến một số lô
        được phát lại. Với câu lệnh INSERT hiện tại, phát lại nghĩa là gì?

            INSERT INTO <bảng> VALUES (...)
            ON CONFLICT (<cột khoá>) DO <UPDATE ... | NOTHING>

        DuckDB chỉ chấp nhận mệnh đề ON CONFLICT khi cột khoá có ràng buộc
        PRIMARY KEY hoặc UNIQUE — xem hằng DDL ngay bên dưới.

        Câu hỏi cho báo cáo: DO UPDATE và DO NOTHING khác nhau ở đâu khi một
        message được phát lại với nội dung ĐÃ ĐỔI? Bạn chọn cái nào, vì sao?
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from ingest.log_client import LogConsumer  # noqa: E402

TABLE = "bronze_events_stream"

# LỜI GIẢI — hạng mục (b), phần 1/2.
# `primary key` trên event_id là điều kiện BẮT BUỘC để DuckDB chấp nhận mệnh
# đề ON CONFLICT. Nó cũng chính là thứ biến "ghi lại" thành một thao tác an
# toàn: khoá tự nhiên của message được khai báo cho database biết, nên
# database có cơ sở để nhận ra "đây là message cũ" thay vì tạo thêm một hàng.
DDL = f"""
create table if not exists {TABLE} (
    event_id      varchar primary key,
    ticket_id     varchar,
    customer_id   varchar,
    customer_name varchar,
    event_type    varchar,
    latency_ms    integer,
    event_time    timestamp,
    _ingested_at  timestamp
);
"""


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> None:
    """Ghi một lô message xuống kho — LỜI GIẢI hạng mục (b), phần 2/2.

    INSERT thuần không idempotent: ghi lại cùng một event_id là tạo thêm một
    hàng. Đảo thứ tự ở (a) khiến một số lô CHẮC CHẮN được phát lại, nên nếu
    dừng ở đó thì ta chỉ đổi "mất dữ liệu" lấy "trùng dữ liệu" — vẫn sai.

    `on conflict (event_id) do update` biến phép ghi thành idempotent: ghi N
    lần cho cùng một kết quả. Đây là nửa còn lại của cặp
    "at-least-once + ghi idempotent", thứ thay thế cho exactly-once vốn không
    tồn tại ở tầng giao vận.

    Vì sao DO UPDATE mà không phải DO NOTHING: nếu một message được phát lại
    với NỘI DUNG ĐÃ ĐỔI (nguồn sửa bản ghi rồi gửi lại cùng event_id),
    DO NOTHING giữ nguyên bản cũ và âm thầm đánh rơi bản cập nhật — kho lệch
    với nguồn mà không có dấu hiệu nào. DO UPDATE luôn cho ra trạng thái của
    lần phát gần nhất, tức là hội tụ về đúng nguồn. Với dữ liệu chỉ-ghi-thêm
    hai cái tương đương, nhưng DO UPDATE đúng trong cả hai trường hợp nên
    không có lý do chọn cái kia.
    """
    con.executemany(
        f"""
        insert into {TABLE} values (?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (event_id) do update set
            ticket_id     = excluded.ticket_id,
            customer_id   = excluded.customer_id,
            customer_name = excluded.customer_name,
            event_type    = excluded.event_type,
            latency_ms    = excluded.latency_ms,
            event_time    = excluded.event_time,
            _ingested_at  = excluded._ingested_at
        """,
        [
            (
                r["event_id"], r["ticket_id"], r["customer_id"], r["customer_name"],
                r["event_type"], r["latency_ms"], r["event_time"], r["_ingested_at"],
            )
            for r in batch
        ],
    )


def maybe_crash(batch_no: int, crash_at: int | None) -> None:
    """Mô phỏng `kill -9`: chết ngay, không rollback, không flush."""
    if crash_at is not None and batch_no == crash_at:
        print(f"  [consumer] 💥 tiến trình bị giết ở lô {batch_no}", flush=True)
        os._exit(137)


def consume(
    db: str,
    topic: str,
    offset_file: str,
    batch_size: int = 500,
    crash_at: int | None = None,
) -> int:
    con = duckdb.connect(db)
    con.execute(DDL)

    written = 0
    with LogConsumer(topic, offset_file) as consumer:
        batch_no = 0
        while True:
            batch = consumer.poll(batch_size)
            if not batch:
                break
            batch_no += 1

            # ── LỜI GIẢI — hạng mục (a): GHI TRƯỚC, COMMIT SAU ────────────
            #
            # Thứ tự CŨ (commit -> crash -> write) = at-most-once:
            #   offset đã dịch sang lô 7 nhưng dữ liệu lô 7 chưa kịp ghi.
            #   Khởi động lại đọc từ lô 8 -> 500 message bốc hơi vĩnh viễn,
            #   và không có gì trong hệ thống ghi nhận rằng chúng từng tồn
            #   tại. Mất dữ liệu im lặng là dạng hỏng tệ nhất.
            #
            # Thứ tự MỚI (write -> crash -> commit) = at-least-once:
            #   lô 7 đã nằm trong kho nhưng offset vẫn trỏ vào đầu lô 7.
            #   Khởi động lại đọc LẠI lô 7 -> không mất gì, nhưng ghi trùng.
            #   Phần "ghi trùng" được write_batch() vô hiệu hoá bằng
            #   ON CONFLICT DO UPDATE.
            #
            # Nguyên tắc: commit offset là lời tuyên bố "dữ liệu này đã an
            # toàn". Tuyên bố đó chỉ được phép nói SAU khi nó thành sự thật.
            write_batch(con, batch)           # ghi dữ liệu
            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
            consumer.commit()                 # ghi nhận offset
            # ─────────────────────────────────────────────────────────────

            written += len(batch)

    con.close()
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--offset", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--crash-at-batch", type=int, default=None)
    a = ap.parse_args()
    n = consume(a.db, a.topic, a.offset, a.batch_size, a.crash_at_batch)
    print(f"  [consumer] đã ghi {n:,} message")
    return 0


if __name__ == "__main__":
    sys.exit(main())
