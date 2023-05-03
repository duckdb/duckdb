#!/bin/bash

openssl dgst -binary -sha256 $1 > hash

cat hash
