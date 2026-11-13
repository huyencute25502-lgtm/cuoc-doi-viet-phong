# deploy-render — file sẵn để đẩy máy chủ phòng lên Render

Thư mục này chứa đúng 4 file cần upload lên repo GitHub `cuoc-doi-viet-phong`
(để Render deploy từ repo đó). Đã kiểm tra: `server.js` không lỗi cú pháp,
đọc cổng từ biến môi trường `PORT` nên chạy được trên Render không cần sửa gì.

- `server.js` — máy chủ phòng (Express + Socket.io, 397 dòng, đã kiểm chứng local 16/16 PASS)
- `package.json` — khai báo `express` + `socket.io`, lệnh start `node server.js`, Node >= 18
- `package-lock.json` — khóa đúng phiên bản thư viện để Render cài đặt giống hệt bản đã test
- `test-client.html` — trang test phòng ở đường dẫn `/test` trên máy chủ

KHÔNG upload thư mục `node_modules` (Render tự chạy `npm install`).

Các bước đầy đủ xem ở `../HOST-MOI.md` (mục "Các bước Huyền cần làm").
Sau khi Render cấp URL công khai, báo URL cho Muse để kiểm chứng sim 16/16.
