@echo off
taskkill /F /IM podcast-studio.exe >nul 2>&1
cargo run %*
