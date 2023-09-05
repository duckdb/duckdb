import os
import pyotp
import subprocess

# deletes old dev wheels from pypi. evil hack.
# how many days to retain dev releases - all dev releases older than 10 days are deleted
retain_days = 10

pypi_username = os.getenv('PYPI_CLEANUP_USERNAME', "")
pypi_password = os.getenv("PYPI_CLEANUP_PASSWORD", "")
pypi_otp = os.getenv("PYPI_CLEANUP_OTP", "")
if pypi_username == "":
    print(f'need username in PYPI_CLEANUP_USERNAME env variable')
    exit(1)
if pypi_password == "":
    print(f'need {pypi_username}\' PyPI password in PYPI_CLEANUP_PASSWORD env variable')
    exit(1)
if pypi_otp == "":
    print(f'need {pypi_username}\' PyPI OTP secret in PYPI_CLEANUP_OTP env variable')
    exit(1)

proc = subprocess.Popen(['pypi-cleanup', '-u', pypi_username, '-p', 'duckdb', '-d', str(retain_days), '--do-it', '-v'], stdin=subprocess.PIPE)
# insert otp
proc.communicate(input=(pyotp.TOTP(pypi_otp).now() + '\n').encode('utf8'))
