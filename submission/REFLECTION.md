# Reflection

Tôi chọn anti-pattern quản lý vòng đời embedding tách rời dữ liệu nguồn. Với chatbot RAG tra cứu tài liệu học tập, tài liệu thường được sửa hoặc xóa nhưng chỉ mục vector bên ngoài có thể chưa cập nhật. Chatbot vì vậy vẫn truy xuất nội dung cũ, trả lời sai hoặc sử dụng tài liệu đã bị thu hồi. Tình huống này tương ứng với lifecycle bug được minh họa trong NB7.

Để phòng tránh, tôi đề xuất lưu `document_id`, phiên bản nguồn và phiên bản mô hình embedding cùng vector trong bảng lakehouse. Pipeline cần đồng bộ cả cập nhật lẫn xóa sang chỉ mục, có cơ chế thử lại và xử lý lặp an toàn. Khi truy xuất, hệ thống kiểm tra tài liệu còn hiệu lực và phiên bản khớp với nguồn. Ngoài ra, cần đối soát định kỳ và kiểm thử tình huống xóa tài liệu nhưng chỉ mục chưa cập nhật.

**Khai báo AI:** Sử dụng OpenAI Codex để kiểm tra thông tin môi trường, đề xuất phòng tránh trong reflection.
