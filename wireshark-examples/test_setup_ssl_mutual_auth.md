# how to generate self sign cert.
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt


# mutual auth Requested.
openssl s_server -accept localhost:4433 -cert server.crt -key server.key -CAfile ca.crt -verify 2 -tls1_3
# mutual auth Required.
openssl s_server -accept localhost:4433 -cert server.crt -key server.key -CAfile ca.crt -Verify 2 -tls1_3

# certificate request packets are encrypted. Does not show up in normal wireshark. keylogfile is needed for WireShark to decrypt.
openssl s_client -connect localhost:4433 -showcerts -keylogfile sslkeylog.txt

