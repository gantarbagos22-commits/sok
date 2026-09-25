# MIG Duel Kick 10 - Node.js Backend

Backend utama proyek ini menggunakan Node.js, Express, dan WebSocket (`ws`).

## Struktur runtime

- `server.js` — backend HTTP + WebSocket.
- `public/index.html` — halaman utama.
- `public/frontend.js` — frontend.
- `public/logo.webp` — aset logo.
- `package.json` — dependency dan perintah start.

## Deployment Debian VPS

Gunakan Node.js 18 atau lebih baru.

```bash
npm install
npm start
```

Port default adalah `3000` dan dapat diubah dengan environment variable `PORT`.

Contoh:

```bash
PORT=3000 npm start
```

Backend bind ke `0.0.0.0`.

## Catatan

Hanya `server.js` yang digunakan sebagai backend. Backup server, source dump lama, dan konfigurasi development yang tidak diperlukan runtime telah dihapus agar deployment tidak ambigu.
