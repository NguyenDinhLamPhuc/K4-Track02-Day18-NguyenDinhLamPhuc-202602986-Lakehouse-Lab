
# Kiến trúc lakehouse cho LLM observability

**Nguyễn Đình Lâm Phúc · MSSV 202602986 · K4-Track02-Day18**  
**Tình huống A · Architecture brief · Ngày 04/10/2026**

## 1. Vấn đề và ràng buộc

Hệ thống API mô hình ngôn ngữ sinh 1 tỷ request/ngày, trung bình 5 KB/request, tương đương 5 TB/ngày trước nén. Dashboard cost và latency theo tenant cần cập nhật mỗi 5 phút. Nội dung prompt/response được giữ 7 ngày để điều tra; aggregates giữ 365 ngày. PII phải được che trước khi người dùng đọc. Ngân sách storage tối đa 5.000 USD/tháng.

Khó khăn chính là ingest liên tục, tránh đếm trùng khi retry, truy vấn tenant lớn mà không quét toàn bộ payload, và hết hạn dữ liệu mà vẫn bảo đảm khả năng phục hồi. Thiết kế chọn Delta Lake trên object storage, xử lý bằng Spark, tách payload khỏi metrics. Dashboard chỉ đọc Gold; điều tra đọc payload đã che PII qua API kiểm soát quyền. Các chỉ tiêu dưới đây là mục tiêu thiết kế, chưa phải kết quả chạy ở quy mô production.

### Hợp đồng và giả định bổ sung

- Quy đổi thập phân: 1 TB = 1.000 GB; tháng tính 30 ngày dữ liệu và 730 giờ dịch vụ. Peak giả định bằng 5 lần trung bình.
- Trung bình `10^9 / 86.400 = 11.574 request/s`; peak khoảng 57.870 request/s, tương đương 289 MB/s raw.
- Giả định 10.000 tenant, 5 model, một region, dữ liệu đến muộn tối đa 24 giờ. Event có `request_id`, `tenant_id`, `event_time`, `ingest_time`, `schema_version` và `redaction_version`.
- “Đầy đủ prompt/response” nghĩa là giữ nội dung sau che PII. Không có khả năng khôi phục phần đã che. Nếu nghiệp vụ đòi nội dung nguyên bản, phải sửa hợp đồng trước triển khai.
- Hết 7 ngày theo `ingest_time`, payload bị chặn đọc. Bản vật lý chờ dọn tối đa thêm 7 ngày, chỉ service maintenance truy cập. Đây là giả định cần chủ hệ thống chấp nhận; yêu cầu xóa vật lý đúng giờ thứ 168 sẽ cần thiết kế khác.
- Refresh đo từ lúc nhận event đến lúc Gold công bố, p95 ≤ 300 giây ở tải thiết kế. Dữ liệu muộn được sửa vào cửa sổ lịch sử, dashboard hiển thị watermark.

### Phạm vi phục vụ

Dashboard gồm request count, tổng chi phí, error rate, p50/p95 latency theo tenant/model/cửa sổ 5 phút. Payload chỉ phục vụ tra cứu theo tenant và thời gian trong 7 ngày. Không cung cấp tìm kiếm toàn văn trên toàn bộ 5 TB/ngày.

<div class="page-break"></div>

## 2. Kiến trúc và luồng dữ liệu

```text
LLM API: request_id + tenant + timestamps
                 |
       Redaction gateway [Security]
       che PII; fail closed; không log raw
                 |
       Durable queue: 12h, 3 replicas
                 |
       Spark ingestion: micro-batch 60s
                 v
 BRONZE Delta [ACID + schema enforcement]
 payload đã che + metrics; partition ingest_date
 đọc <=7 ngày; reject schema -> quarantine đã che
                 |
       dedup request_id + MERGE [Medallion]
                 v
 SILVER Delta: metrics, không prompt/response
 partition event_date; cluster tenant_id
                 |
       recompute affected windows; commit version
                 v
 GOLD Delta: tenant/model/5-minute aggregates
                  [Retention: 365 ngày]
                 |
       Query API + cache -> Dashboard (<=5 phút)

 Incident API -> quyền tenant + cutoff -> BRONZE

 Catalog + ACL + schema owner --------> cả 3 lớp
 Lineage: offsets -> table versions -> Gold release
 Maintenance: compaction / expiry / orphan inventory
 Recovery: pin versions, replay [Time travel]
```

**Luồng ghi:** gateway che PII trước queue. Khi bộ lọc không sẵn sàng, producer retry; không chuyển raw sang dead-letter queue. Queue chỉ ACK sau khi ghi bền vững. Bronze commit xong mới checkpoint offset. Khóa `(tenant_id, request_id)` chống trùng ở Silver; một writer chịu trách nhiệm mỗi bảng, retry khi conflict.

**Luồng tổng hợp:** Silver giữ metrics 7 ngày hoạt động để sửa cửa sổ đến muộn. Gold được tính lại từ tập Silver của các cửa sổ bị ảnh hưởng, rồi `MERGE` thay thế giá trị; không cộng dồn lại batch retry. p50/p95 tính từ sự kiện hoặc histogram cố định có sai số công bố, không lấy trung bình các percentile. Tổng hợp dài hạn chỉ dùng count, sum và histogram có thể gộp.

**Luồng đọc:** API tự gắn tenant từ danh tính xác thực; không tin tenant do client truyền. Người dùng không có quyền đọc object trực tiếp. Catalog lưu tên bảng, vị trí và schema; quyền truy cập thực tế còn được thực thi tại API và storage. Gold release chứa phiên bản Silver nguồn, phiên bản mã và watermark; API chỉ công bố release đã đối soát.

**Áp dụng Day18:** medallion phân tách trách nhiệm; ACID và schema enforcement bảo vệ commit; clustering giảm scan theo tenant; maintenance kiểm soát file nhỏ; version pinning và lineage giúp replay; retention tách khả năng đọc với thu hồi byte. Đây là các cơ chế nằm trên luồng xử lý, không chỉ là danh sách công nghệ.

<div class="page-break"></div>

## 3. Quyết định kiến trúc và alternatives bị loại

### Q1 — Table format và engine

**Chọn Delta Lake + Spark** cho cả ba lớp: ưu tiên `MERGE`, schema enforcement và phục hồi theo version, phù hợp thao tác Day18. Ghim phiên bản engine/thư viện sau compatibility test. **Loại Parquet thuần** vì phải tự xây commit nguyên tử và xử lý retry. **Loại Iceberg cho MVP này** vì lợi ích nhiều engine chưa phải yêu cầu, trong khi nhóm phải bổ sung kiểm thử catalog và cơ chế cập nhật khác. Iceberg vẫn đáng xem xét khi mở thêm engine độc lập.

### Q2 — Catalog và governance

**Chọn Hive Metastore với database bền vững**, một endpoint ghi và backup hằng ngày; API là cổng truy cập bắt buộc. Catalog không được xem là cơ chế bảo mật duy nhất. **Loại path ghi cứng trong notebook** vì dễ lệch vị trí bảng và thiếu owner/schema tập trung. **Loại catalog chỉ dành cho Iceberg REST** vì không thay thế trực tiếp transaction log Delta của thiết kế. Chi phí đổi table format chưa có lợi ích tương ứng.

### Q3 — Partition và clustering

**Chọn Bronze theo ngày ingest, Silver/Gold theo ngày event**, gom file mục tiêu 256–512 MB; cluster Bronze/Silver theo tenant trong partition vừa đóng. **Loại partition từng tenant** vì 10.000 tenant tạo nhiều partition nhỏ và skew. **Loại partition theo request_id** vì cardinality gần số dòng, phá hiệu quả listing và compaction. Z-order có thể cải thiện data skipping nhưng phải đo bằng số byte đọc; không hứa tốc độ cố định. Xem [Delta optimizations](https://docs.delta.io/optimizations-oss/).

### Q4 — Payload và compression

**Chọn payload đã che trong Bronze, chỉ metrics ở Silver**, Parquet nén ZSTD nếu compatibility test đạt. Dùng tỷ lệ nén 4:1 như giả định lập ngân sách. **Loại sao chép toàn bộ payload qua ba lớp** vì tăng storage, I/O và phạm vi PII. **Loại một object cho mỗi request** vì một tỷ object/ngày làm metadata, request phí và truy xuất ngẫu nhiên trở thành nút thắt. Object được ghi theo batch có giới hạn kích thước.

### Q5 — Retention và khôi phục

**Chọn cutoff đọc 7 ngày + cửa sổ file đã loại 7 ngày**, Gold 365 ngày; expire bảng trước, vacuum sau, không dùng TTL object mù với file đang được tham chiếu. **Loại archive payload một năm** vì trái mục tiêu giảm retention. **Loại vacuum ngay sau mỗi DELETE** vì reader hoặc replay có thể còn cần file cũ. Time travel phụ thuộc file còn tồn tại; log không thay thế dữ liệu đã xóa. Xem [Delta utility commands](https://docs.delta.io/delta-utility/).

### Q6 — Streaming và tính đúng

**Chọn micro-batch 60 giây**, queue bền vững, dedup và ghi idempotent; dành 240 giây còn lại cho xử lý/công bố. **Loại batch mỗi giờ** vì vi phạm refresh 5 phút. **Loại API ghi từng event trực tiếp vào bảng** vì quá nhiều commit/file nhỏ, khó kiểm soát retry ở 58.000 request/s. Không tuyên bố exactly-once xuyên tất cả thành phần chỉ nhờ ACID của một bảng.

<div class="page-break"></div>

## 4. Failure modes và vận hành lúc 3 giờ sáng

| Sự cố | Phát hiện cụ thể | Cô lập, rollback và phục hồi |
|---|---|---|
| Job mới ép sai kiểu `cost_usd`, hoặc nhân chi phí 1.000 lần | Schema reject > 0; đối soát tổng cost theo batch và canary so với nguồn | Dừng publish Gold; giữ release tốt gần nhất. Đọc Silver version đã pin, sửa code rồi dựng lại các cửa sổ lỗi. Không RESTORE toàn bảng khi có commit mới hợp lệ chưa tách được. |
| Consumer crash sau commit, trước checkpoint | Queue lag; cùng batch/offset xuất hiện lần nữa; đối chiếu số distinct request | Restart từ checkpoint. Dedup Silver và ghi đè aggregate cửa sổ bảo đảm replay không cộng đôi. Nếu Gold sai, quay release pointer về bản tốt rồi rebuild. |
| Redaction bỏ sót PII sau khi schema payload đổi | Canary PII tổng hợp và DLP scan mẫu; bất kỳ canary lọt qua đều báo động | Khóa đọc partition liên quan, rollback bộ lọc, tạm dừng ingest. Service hạn chế quyền re-redact các bản đang lưu và cache; không phục hồi version chứa PII. Audit phải ghi phạm vi ảnh hưởng. |
| Maintenance xóa file còn cần hoặc consumer lag quá cửa sổ | Dry-run inventory; kiểm tra active readers, checkpoint age; lỗi missing-file | Dừng sweep và ingest vào bảng lỗi, không tiếp tục vacuum. Phục hồi từ bản dự phòng nếu còn trong retention, hoặc replay queue khi còn offset. Ngoài cả hai cửa sổ, báo mất dữ liệu; không giả định time travel cứu được. |
| Tenant lớn làm skew, file nhỏ tăng và refresh > 5 phút | p95 event-to-Gold, kích thước file, shuffle spill và lag theo tenant | Giới hạn truy vấn điều tra; tăng worker trong trần ngân sách, tách tenant nóng thành lane xử lý riêng. Rollback lịch clustering mới nếu gây tranh tài nguyên; không giảm chất lượng thống kê để báo đạt SLA. |

### Hợp đồng maintenance

Compaction chỉ chạy trên partition đủ ổn định; không rewrite toàn bộ bảy ngày mỗi giờ. Gold dùng buffer để tránh tạo một file rất nhỏ cho mỗi nhóm tenant. Lịch clustering dự kiến 4 giờ/ngày và phải dừng nếu lag ingest vượt 120 giây.

Trước sweep: lập inventory, so với tập file được snapshot còn giữ tham chiếu, áp dụng khoảng an toàn lớn hơn transaction dài nhất và kiểm tra không có writer đang sử dụng. Thử bằng file orphan cài sẵn trong môi trường test. Không suy rộng hành vi `VACUUM` giữa Spark Delta và `deltalake` của lab; cần canary theo engine thực tế.

### Giới hạn phục hồi và bảo mật

Mục tiêu RPO bằng 0 cho event đã ACK trong lỗi một consumer; chưa cam kết RPO bằng 0 khi mất cả region. RTO mục tiêu 30 phút cho rollback code và công bố lại Gold gần nhất. Queue 12 giờ là giới hạn replay nhanh, không phải backup dài hạn.

Retention áp dụng cho cả cache, quarantine, backup, file rewrite và phiên bản object cũ. Backup catalog không chứa payload. Không cho analyst truy cập snapshot cũ để vượt cutoff 7 ngày. Nếu yêu cầu xóa vật lý trong đúng 7 ngày được xác nhận, phải đánh giá lại thiết kế retention trước khi nghiệm thu.

<div class="page-break"></div>

## 5. Storage và compute: phép tính kiểm tra được

**Đơn giá dưới đây là giả định ngân sách, không phải báo giá nhà cung cấp:** object storage 25 USD/TB-tháng; queue disk 100 USD/TB-tháng; compute 0,05 USD/vCPU-giờ, giả định đã phân bổ RAM tương ứng. Cần thay bằng báo giá và benchmark trước mua hạ tầng. Không dùng hệ số replica nội bộ của dịch vụ object storage để nhân thêm vào dung lượng bị tính phí.

### Storage ở trạng thái ổn định

| Thành phần | Phép tính | TB |
|---|---|---:|
| Bronze đang đọc và file hết hạn chờ dọn | `5 / 4 TB/ngày × (7 + 7) ngày` | 17,5000 |
| Silver metrics; 200 byte/event trước nén | `10^9 × 200 / 10^12 / 4 × 14` | 0,7000 |
| File cũ do compaction/rewrite | Dự trù thêm, cần đo inventory | 3,0000 |
| Gold: 10.000 tenant × 5 model × 288 cửa sổ/ngày | `14,4 triệu × 200 byte / 3 × 365 / 10^12` | 0,3504 |
| Log, catalog backup, audit, quarantine | Hạn mức riêng, không lưu raw | 0,5000 |
| **Tổng object storage** | Tổng các hàng trên | **22,0504** |
| Queue, nén 4:1, giữ 12 giờ, ba replica | `5 / 4 × 0,5 × 3` | **1,8750** |

Storage cơ sở: `22,0504 × 25 + 1,875 × 100 = 738,76 USD/tháng`. Dành thêm 250 USD/tháng cho request/listing: **988,76 USD/tháng**, còn 4.011,24 USD so với trần 5.000. Request phí là khoản dự phòng cần đo, không phải kết quả suy ra từ dung lượng.

Gold giả định 200 byte gồm histogram gọn và các tổng; nếu histogram cần 1 KB thì dung lượng Gold tăng 5 lần. File rewrite dự trù 3 TB tương đương khoảng `3 / 7 = 0,429 TB/ngày` tồn tại thêm bảy ngày; vượt mức này phải tăng dự báo, không giấu trong tỷ lệ nén.

### Compute và khả năng đáp ứng

| Nhóm | vCPU thường trực | USD/tháng |
|---|---:|---:|
| Gateway redaction | 64 | `64 × 730 × 0,05 = 2.336` |
| Ingestion + Silver + Gold | 48 | 1.752 |
| Query API/engine | 16 | 584 |
| Queue brokers | 8 | 292 |
| Catalog, scheduler, monitoring | 8 | 292 |

Compute thường trực: **5.256 USD/tháng**. Maintenance: `32 vCPU × 4 giờ/ngày × 30 × 0,05 = 192 USD`. Tổng compute **5.448 USD/tháng**; storage, request dự phòng và compute cộng lại **6.436,76 USD/tháng**. Trần đề bài áp dụng cho storage, không phải tổng này. Chưa gồm nhân sự, thuế, hỗ trợ và egress ngoài region.

Giả định redaction tốn 1 ms CPU/request: peak cần 57,87 core; 64 core chỉ dư khoảng 10%, phải đo và scale nếu không đủ. Nếu tốn 5 ms thì cần khoảng 290 core. Spark 48 core phải đạt tối thiểu `57.870 / 48 = 1.206 event/s/core` ở peak; đây là ngưỡng benchmark, chưa phải năng lực đã chứng minh.

**Độ nhạy:** nếu Bronze, Silver và queue không nén được, chi phí tăng `(52,5 + 2,1) × 25 + 5,625 × 100 = 1.927,50 USD`, thành khoảng **2.916,26 USD/tháng** trước các thay đổi khác. Nếu đồng thời rewrite vượt dự trù, phải tính lại. Cảnh báo dự báo storage ở 4.000 USD; tạm dừng rewrite không thiết yếu ở 4.500 USD, không tự rút ngắn retention đã cam kết.

<div class="page-break"></div>

## 6. MVP một tuần và tiêu chí nghiệm thu

MVP chạy dữ liệu tổng hợp cho 100 tenant, ba model, đủ bảy ngày event-time. Một lát cắt gồm redaction → queue → ba bảng Delta → API dashboard; giữ nguyên khóa dedup, version manifest và cutoff. Mục tiêu chứng minh cơ chế, không coi benchmark laptop là bằng chứng chịu tải một tỷ request/ngày.

| Ngày | Sản phẩm kiểm tra được | Tiêu chí nghiệm thu |
|---|---|---|
| 1 | Data contract và generator có ground truth | 1 triệu event, 5% duplicate, 2% late event; tập canary PII có nhãn; xác định rõ số request duy nhất và tổng cost. |
| 2 | Gateway, queue và Bronze | 100% canary đã biết được che; schema sai bị cách ly; raw không xuất hiện trong log/queue. Công bố giới hạn coverage, không suy ra nhận diện mọi PII thực tế. |
| 3 | Silver dedup và Gold windows | Count/cost khớp ground truth; retry cùng batch không đổi kết quả; histogram p95 lệch không quá một bin so với phép tính chuẩn. |
| 4 | API, tenant isolation và release manifest | Tenant A không đọc được B; truy vấn ngày hết hạn bị từ chối; mỗi release truy ngược được input versions và code hash. |
| 5 | Thử lỗi commit/checkpoint và replay | Kill consumer ngay sau commit rồi restart ba lần: distinct count và tổng cost không đổi; Gold sửa đúng cửa sổ có late data. |
| 6 | Compaction, retention và capacity probe | Báo byte/file trước-sau, scan bytes theo tenant, queue lag; file còn tham chiếu không bị xóa. Chạy tải tăng dần, ghi throughput và CPU thực tế. |
| 7 | Diễn tập rollback, báo cáo và quyết định go/no-go | Rollback release lỗi trong 30 phút; lưu kết quả, giới hạn và dự toán đã cập nhật theo tỷ lệ nén đo được. |

### Kiểm chứng cơ chế khó nhất

Khó nhất là bảo đảm tổng hợp đúng sau retry và dữ liệu đến muộn. Tạo batch B có duplicate, cho Bronze commit rồi cố tình dừng trước checkpoint. Restart để B được đọc lại; sau đó gửi event cũ 23 giờ. So sánh Gold với phép tính batch độc lập trên tập event duy nhất. So sánh count/cost chính xác, percentile theo sai số histogram đã công bố; kiểm tra version manifest không tham chiếu output chưa hoàn tất.

Retention test dùng bảng tạm, đồng hồ ứng dụng điều khiển được và dữ liệu có tuổi giả lập; không tắt safety check trên bảng dùng chung. Kiểm tra quyền đọc ở ranh giới 7 ngày tách biệt với kiểm tra thu hồi vật lý. Với vacuum thực, chờ đủ retention hoặc dùng fixture file đã cũ trong môi trường cô lập; chưa thực thi thì ghi rõ chưa đạt.

### Cổng chấp nhận trước production

MVP phải đạt đúng dữ liệu và cách ly tenant trước tối ưu tốc độ. Capacity test riêng cần giữ 57.870 event/s trong ít nhất 30 phút và đo p95 event-to-Gold ≤ 300 giây; kiểm thử thêm burst và tenant skew. Nếu thiếu hạ tầng để chạy, báo “chưa xác minh capacity”, không ngoại suy tuyến tính thành kết luận đạt SLA. Chỉ triển khai khi dự toán theo số đo vẫn dưới trần storage và chủ hệ thống chấp nhận hợp đồng xóa vật lý.
