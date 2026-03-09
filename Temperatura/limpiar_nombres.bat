@echo off
setlocal enabledelayedexpansion

:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.85_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.85_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.8_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.8_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.10_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.10_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.11_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.11_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.12_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.12_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.13_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.13_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.14_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.14_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.15_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.15_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.16_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.16_=!"
    ren "%%f" "!newname!"
)
:: Este comando busca todos los archivos que empiecen con la IP y el guion bajo
for %%f in (172.27.37.17_*) do (
    set "filename=%%f"
    :: Reemplaza la cadena específica por nada
    set "newname=!filename:172.27.37.17_=!"
    ren "%%f" "!newname!"
)
echo Proceso terminado. Nombres limpiados.
pause