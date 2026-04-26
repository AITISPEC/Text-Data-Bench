@echo off
chcp 65001 >nul
color a
title Text Data Bench Launcher
echo ==============================
echo   AITISPEC - Text Data Bench
echo ==============================
echo.

:: Переход в папку, где находится bat-файл
cd /d "%~dp0"

:: Проверка существования виртуального окружения
if not exist ".venv\Scripts\activate.bat" (
    echo [ОШИБКА] Виртуальное окружение не найдено!
    echo Запустите сначала install.ps1 -create_env
    pause
    exit /b 1
)

:: Активация окружения
call .venv\Scripts\activate.bat

:: Запуск приложения
python python -m text_data_bench
