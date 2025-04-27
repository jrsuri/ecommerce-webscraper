@echo off
chcp 65001
cd /d "%~dp0"

python "ecommerce_webscraper.py"
pause