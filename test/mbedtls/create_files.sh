#!/bin/bash
rm dummy_file* *.pem 
head -c 100000 </dev/urandom > dummy_file
openssl genrsa -out private.pem 2048
openssl rsa -in private.pem -outform PEM -pubout -out public.pem
openssl dgst -binary -sha256 dummy_file > dummy_file.sha256
openssl pkeyutl -sign -in dummy_file.sha256 -inkey private.pem -pkeyopt digest:sha256 -out dummy_file.signature

# RSA-3072 fixture: a 3072-bit key produces a 384-byte PKCS#1 v1.5 SHA-256 signature over the same hash.
# Used to verify the RSA-3072 path in IsValidSha256Signature while keeping the 2048 fixture above for the
# backward-compatibility regression assertion.
openssl genrsa -out private_3072.pem 3072
openssl rsa -in private_3072.pem -outform PEM -pubout -out public_3072.pem
openssl pkeyutl -sign -in dummy_file.sha256 -inkey private_3072.pem -pkeyopt digest:sha256 -out dummy_file.signature_3072
