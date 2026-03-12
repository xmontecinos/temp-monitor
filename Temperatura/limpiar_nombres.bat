@echo off
setlocal enabledelayedexpansion

echo Limpiando prefijos de IP...

for %%f in (*_MMLTask_*) do (
    set "fullname=%%f"
    
    :: Busca la posición del primer guion bajo y corta la cadena
    for /f "tokens=1* delims=_" %%a in ("%%f") do (
        set "newname=%%b"
        
        :: Solo renombra si el archivo existe y el nombre nuevo no está vacío
        if not "!newname!"=="" (
            if not exist "!newname!" (
                ren "%%f" "!newname!"
                echo Renombrado: %%f --^> !newname!
            )
        )
    )
)

echo.
echo ¡Listo! Todas las IPs han sido removidas.
pause