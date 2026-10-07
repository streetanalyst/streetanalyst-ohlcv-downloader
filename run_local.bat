@echo off
REM Local Windows run. Set key once with: setx ALPHA_VANTAGE_KEY yourkey  (then open a new terminal)
cd /d "%~dp0"
python download_ohlcv.py
pause
