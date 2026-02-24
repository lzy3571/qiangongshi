@echo off
echo Starting dependency download for Synology NAS (Linux x86_64)...
mkdir packages 2>nul

echo Downloading wheels for Python 3.8 (Common on Synology DSM 7)...
pip download -r requirements.txt -d packages --platform manylinux_2_17_x86_64 --only-binary=:all: --python-version 3.8

echo.
echo Download complete!
echo Now copy the entire 'qiangongshi-New' folder to your NAS.
pause
