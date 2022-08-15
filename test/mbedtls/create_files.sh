#!/bin/bash
rm dummy_file* *.pem 
head -c 100000 </dev/urandom > dummy_file
openssl genrsa -out private.pem 2048
openssl rsa -in private.pem -outform PEM -pubout -out public.pem
openssl dgst -binary -sha256 dummy_file > dummy_file.sha256
openssl pkeyutl -sign -in dummy_file.sha256 -inkey private.pem -pkeyopt digest:sha256 -out dummy_file.signature