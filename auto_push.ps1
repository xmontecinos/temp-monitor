while($true) {
    # 1. Revisa si hay cambios
    git add .
    
    # 2. Intenta hacer el commit (solo si hay algo nuevo)
    git commit -m "Auto-update: Monitoreo de Temperaturas $(Get-Date)"
    
    # 3. Sube los cambios a GitHub
    git push origin main
    
    Write-Host "✅ Sincronización completada. Próxima revisión en 5 minutos..." -ForegroundColor Green
    
    # 4. Espera 300 segundos (5 minutos)
    Start-Sleep -Seconds 300
}
# Actualización automática