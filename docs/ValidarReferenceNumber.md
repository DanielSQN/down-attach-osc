# Validar un Reference Number en los archivos de metadata (Windows / PowerShell)

Cómo comprobar, desde la terminal de Windows, si un `Reference Number` quedó en algún CSV de metadata (`*_attachments.csv`) — útil para validar que un SR que había fallado se reprocesó bien.

> Se usa `Select-String` (búsqueda de texto en streaming, línea por línea), no `Import-Csv`, porque con muchos archivos grandes (p. ej. 70 archivos de ~50 MB) parsear columnas sería lento y consumiría mucha memoria. `Select-String` recorre todo en segundos.

Abrí **PowerShell** y definí una vez las variables:

```powershell
$sr  = "0002859140"                              # el Reference Number a validar
$out = "C:\Users\daniel\Downloads\metadatos"     # carpeta con los *_attachments.csv
```

## 1. ¿En qué archivo(s) de metadata está?

```powershell
Select-String -Path "$out\*_attachments.csv" -Pattern "\b$sr\b" |
    Select-Object -ExpandProperty Filename -Unique
```

- Imprime el/los nombre(s) de archivo que contienen el SR → **está en metadata, se reprocesó bien y tiene adjuntos.** ✅
- No imprime nada → ver el punto 4 (matiz importante).

> `\b$sr\b` usa límites de palabra para coincidencia exacta: evita que, por ejemplo, `285914` haga match dentro de `0002859140`, o que `0002859140` matchee dentro de un número más largo.

## 2. Solo saber sí / no

```powershell
if (Select-String -Path "$out\*_attachments.csv" -Pattern "\b$sr\b" -Quiet) {
    "SI  -> $sr esta en metadata"
} else {
    "NO  -> $sr no aparece (ver matiz en el punto 4)"
}
```

`-Quiet` corta la búsqueda en la primera coincidencia, así que es aún más rápido cuando solo querés confirmar presencia.

## 3. Ver las filas concretas (archivo + línea + contenido)

```powershell
Select-String -Path "$out\*_attachments.csv" -Pattern "\b$sr\b" |
    Select-Object Filename, LineNumber, Line | Format-List
```

Cada resultado es un adjunto de ese SR. La columna `Line` es la fila completa del CSV (incluye `FileName`, `DmDocumentId`, `FileContentsHref`, etc.).

## 4. ⚠️ Si no aparece: desempatar

Que un SR **no** esté en el CSV **no siempre** significa que falló. Un SR consultado con éxito pero **sin adjuntos** no genera ninguna fila. Las dos posibilidades son:

1. Sigue pendiente o falló → hay que reprocesarlo.
2. Se procesó bien pero no tiene adjuntos → correcto, no hay nada que listar.

Para distinguirlas, mirá si el archivo de entrada de ese SR quedó **completo**:

```powershell
# Archivos que terminaron sin errores (se procesaron 100%):
Get-Content "$out\_processed_files.json" | ConvertFrom-Json

# Si aparece algún .progress, ese archivo AÚN tiene SR pendientes:
Get-ChildItem "$out\*.progress"
```

- Si el archivo de entrada correspondiente está en `_processed_files.json` y **no** tiene `.progress` al lado → todos sus SR se consultaron con éxito. Que ese SR no esté en el CSV es porque **no tiene adjuntos**, no porque falló.
- Si tiene un `.progress`, ese archivo todavía tiene pendientes: reprocesá y volvé a validar.

Para saber a qué archivo de entrada (y por ende a qué `_attachments.csv`) pertenece un SR, buscalo en la carpeta de entrada:

```powershell
$inputFolder = "C:\Users\daniel\Downloads\ServiceRequest"
Select-String -Path "$inputFolder\*.csv" -Pattern "\b$sr\b" |
    Select-Object -ExpandProperty Filename -Unique
```

## 5. Validar en lote varios SR del errors.log

Extrae los SR fallidos del log y valida cuáles ya están en metadata:

```powershell
$out = "C:\Users\daniel\Downloads\metadatos"

$fallidos = Select-String -Path errors.log -Pattern 'adjuntos de SR ([^\s:]+):' |
    ForEach-Object { $_.Matches[0].Groups[1].Value } | Sort-Object -Unique

foreach ($sr in $fallidos) {
    if (Select-String -Path "$out\*_attachments.csv" -Pattern "\b$sr\b" -Quiet) {
        "OK      $sr"
    } else {
        "REVISAR $sr"   # o no tiene adjuntos, o sigue pendiente (ver punto 4)
    }
}
```

Los que queden en `REVISAR` se contrastan con el punto 4 para decidir si hay que reprocesarlos.
