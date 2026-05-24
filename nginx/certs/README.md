# TLS Certificates

## Development (self-signed — уже готово)
`privkey.pem` и `fullchain.pem` — самоподписанные, для локальной разработки.
Браузер покажет предупреждение — это нормально для dev.

## Production (Let's Encrypt)
```bash
# На сервере с публичным IP и доменом:
apt install certbot
certbot certonly --standalone -d chat.yourdomain.com

# Скопировать сертификаты:
cp /etc/letsencrypt/live/chat.yourdomain.com/fullchain.pem nginx/certs/
cp /etc/letsencrypt/live/chat.yourdomain.com/privkey.pem  nginx/certs/

# Автообновление (cron):
0 0 1 * * certbot renew --quiet && docker compose restart nginx
```
