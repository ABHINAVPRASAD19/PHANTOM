@echo off
REM PHANTOM Windows Start Script

echo Starting PHANTOM Orchestrator...

REM Check for virtual environment
if exist venv\Scripts\activate (
    echo Activating virtual environment...
    call venv\Scripts\activate
) else (
    echo Warning: venv not found. Using system python.
)

REM Run the server
python -m uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload

pause
