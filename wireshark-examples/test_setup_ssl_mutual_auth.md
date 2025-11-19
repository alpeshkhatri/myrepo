# how to generate self sign cert.
```bash
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt
```

# mutual auth Requested.
```bash
openssl s_server -accept localhost:4433 -cert server.crt -key server.key -CAfile ca.crt -verify 2 -tls1_3
```
# mutual auth Required.
```bash
openssl s_server -accept localhost:4433 -cert server.crt -key server.key -CAfile ca.crt -Verify 2 -tls1_3
```
# in TLS 1.3 most of ssl handshake is encrypted. The full handshake does not show up in normal wireshark. keylogfile is needed for WireShark to decrypt the SSL handshake.
```bash
openssl s_client -connect localhost:4433 -showcerts -keylogfile sslkeylog.txt
```
# example mutual auth requested vs required

<img width="1810" height="612" alt="Screenshot 2025-11-19 104124" src="https://github.com/user-attachments/assets/29cb7f07-9eab-4cd0-ae19-1e79242a9cb0" />
