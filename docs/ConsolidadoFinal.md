# Consolidado final de la migración

Cómo cerrar la migración: juntar lo que produjeron **todas las máquinas** en un solo lugar, sacar el gran total, el índice maestro de búsqueda y la lista definitiva de lo no migrado.

Aplica cuando ya corrieron `GetAttachmentBinary` (adjuntos) y, si se usó, `GetMetadataClobAndMessages` (mensajes) con `destination: gcp`.

---

## 1. Qué hay en el bucket

Cada archivo procesado deja tres cosas: los **objetos** (los adjuntos/contenidos), un **índice** (qué quedó y dónde) y los **controles** (totales y errores de esa corrida).

```
gs://<bucket>/<prefix>/
├── 0002859140/                                   <- adjuntos, una carpeta por Reference Number
│   └── documento.pdf
│
├── _index/                                       <- 1 archivo por CSV procesado (sin host)
│   └── ServiceRequest_7_6_001_attachments_index.csv
│
└── _control/                                     <- auditoría, una subcarpeta POR MÁQUINA
    ├── LAPTOP-DQ85420/
    │   ├── ServiceRequest_7_6_001_attachments_resumen.csv
    │   └── ServiceRequest_7_6_001_attachments_errores.csv
    └── VM-MIGRACION-02/
        └── ...
```

Para mensajes (`gcp_prefix` propio, p. ej. `mensajes`):

```
gs://<bucket>/mensajes/
├── message_content/0002765583/3000057733516.html   <- .html o .txt según MessageTypeCd
└── _index/ServiceRequest_7_6_001_messages.csv      <- metadata + Location de cada mensaje
```

| Archivo | Contenido | Una fila por |
|---|---|---|
| `_index/<base>_index.csv` | `Reference Number`, `DmDocumentId`, `AttachedDocumentId`, `FileName`, `UploadedFileContentType`, `UploadedFileLength`, `StoredAs`, `Location`, `metadata_file` | adjunto **confirmado en el bucket** |
| `_control/<host>/<base>_resumen.csv` | `metadata_file`, `host`, `job_id`, `processed_at`, `total_solicitudes`, `total_adjuntos`, `cargados`, `downloaded`, `skipped_existing`, `errores`, `sin_href` | archivo procesado |
| `_control/<host>/<base>_errores.csv` | `Reference Number`, `FileName`, `StoredAs`, `Location`, `Status`, `Code`, `Error` | adjunto **fallido** |
| `_index/<base>_messages.csv` | campos del mensaje + `MessageContent`, `ContentFormat`, `Location` | mensaje |

> El índice **solo** incluye adjuntos confirmados en destino; los fallidos están en `_errores.csv`. Índice + errores = todas las filas del CSV de metadatos que tenían `FileContentsHref`.

### Si falta algún índice en `_index/`

Compara los índices del bucket contra tus CSVs de metadatos:

```powershell
$enBucket = (gcloud storage ls "$BUCKET/$PREFIX/_index/") | ForEach-Object { Split-Path $_ -Leaf }
Get-ChildItem "C:\...\METADATA\*_attachments.csv" | ForEach-Object {
    $esperado = "$($_.BaseName)_index.csv"
    if ($enBucket -notcontains $esperado) { "FALTA: $esperado" }
}
```

Falta un índice si el archivo se procesó con una versión anterior del programa (antes de que existiera el índice). El local sí está en tu `output_folder`, así que basta subirlo:

```powershell
gcloud storage cp "C:\...\ADJUNTOS_ESTADO\*_index.csv" "$BUCKET/$PREFIX/_index/"
```

Alternativa (regenera el índice desde cero y lo sube): relanzar ese archivo con `metadata_csv`. Como todo ya está en el bucket, la corrida es rápida (`skipped_existing`).

### Regenerar TODOS los índices (p. ej. tras agregar columnas)

Si los índices del bucket se generaron con una versión que no traía alguna columna (como `DmDocumentId`), se regeneran todos de una sola pasada con `force: true` (ignora el manifiesto y vuelve a tomar todos los archivos) y `overwrite: false` (no re-descarga nada: todo sale `skipped_existing`):

```jsonc
{
  "metadata_folder": "C:\\...\\METADATA",
  "output_folder": "C:\\...\\ADJUNTOS_ESTADO",
  "destination": "gcp",
  "gcp_bucket": "dev-tablas-migracion-crm",
  "gcp_prefix": "adjuntos",
  "force": true,        // vuelve a tomar los archivos ya procesados
  "overwrite": false,   // NO re-sube binarios; solo verifica y reescribe el índice
  "batch_size": 0,
  "max_workers": 7
}
```

Los binarios que falten (los que en su día fallaron) sí se descargan en esa pasada — algo deseable: quedan migrados e indexados.

---

## 2. Descargar todo a la máquina de cierre

```powershell
$BUCKET = "gs://dev-tablas-migracion-crm"
$PREFIX = "adjuntos"
$BASE   = "C:\Users\dq85420\Downloads\CIERRE"

New-Item -ItemType Directory -Force -Path "$BASE\indices", "$BASE\resumenes", "$BASE\errores" | Out-Null

gcloud storage cp "$BUCKET/$PREFIX/_index/*_index.csv"          "$BASE\indices\"
gcloud storage cp "$BUCKET/$PREFIX/_control/**/*_resumen.csv"   "$BASE\resumenes\"
gcloud storage cp "$BUCKET/$PREFIX/_control/**/*_errores.csv"   "$BASE\errores\"
```

> Los `_resumen.csv` y `_errores.csv` de máquinas distintas pueden llamarse igual (mismo CSV reintentado en otro nodo). Si `cp` te avisa de sobrescritura, bájalos conservando la carpeta de host:
> `gcloud storage cp -r "$BUCKET/$PREFIX/_control/*" "$BASE\control_por_host\"`

---

## 3. Índice maestro (búsqueda SR → ruta)

```powershell
Import-Csv "$BASE\indices\*.csv" | Export-Csv "$BASE\indice_maestro.csv" -NoTypeInformation -Encoding UTF8
```

Un solo CSV con **todos** los adjuntos migrados. Como cada fila trae `metadata_file`, la unión es autodescriptiva y sirve aunque un mismo SR haya aparecido en varios archivos de entrada.

### Usar el índice para cargar a otro sistema

El índice trae todo lo necesario para una carga aguas abajo: el `Location` de dónde leer el binario, el `Reference Number` para relacionarlo con la solicitud, y el `DmDocumentId` como **id externo** para que la carga sea idempotente (reintentar no duplica) y auditable contra Oracle.

```powershell
# Filas listas para cargar; usa AttachedDocumentId cuando DmDocumentId venga vacío
Import-Csv "$BASE\indice_maestro.csv" | ForEach-Object {
    [pscustomobject]@{
        sr_number            = $_.'Reference Number'
        file_name            = $_.FileName
        documento_externo_id = if ($_.DmDocumentId) { $_.DmDocumentId } else { $_.AttachedDocumentId }
        gcs_path             = $_.Location
        content_type         = $_.UploadedFileContentType
        size                 = $_.UploadedFileLength
    }
} | Export-Csv "$BASE\carga_externa.csv" -NoTypeInformation -Encoding UTF8
```

Comprobación previa recomendada: que no haya ids externos repetidos ni vacíos.

```powershell
$c = Import-Csv "$BASE\carga_externa.csv"
($c | Where-Object { -not $_.documento_externo_id }).Count          # debe ser 0
($c | Group-Object documento_externo_id | Where-Object Count -gt 1).Count   # debe ser 0
```

### Buscar los adjuntos de una solicitud

```powershell
$idx = Import-Csv "$BASE\indice_maestro.csv"
$idx | Where-Object { $_.'Reference Number' -eq '0002859140' } |
       Select-Object DmDocumentId, FileName, Location
```

Total de adjuntos migrados y solicitudes distintas:

```powershell
$idx.Count
($idx | Group-Object 'Reference Number').Count
```

---

## 4. Gran total de la migración

```powershell
$res = Import-Csv "$BASE\resumenes\*.csv"

$res | Measure-Object total_adjuntos, cargados, downloaded, skipped_existing, errores, sin_href -Sum |
    Select-Object Property, Sum

# Desglose por máquina (cuánto hizo cada nodo):
$res | Group-Object host | ForEach-Object {
    [pscustomobject]@{
        host      = $_.Name
        archivos  = $_.Count
        adjuntos  = ($_.Group | Measure-Object total_adjuntos -Sum).Sum
        cargados  = ($_.Group | Measure-Object cargados -Sum).Sum
        errores   = ($_.Group | Measure-Object errores -Sum).Sum
    }
}
```

**Ojo con los duplicados**: si un archivo se procesó dos veces (reintento, o en otra máquina), aparece con dos filas de resumen y sumaría doble. Quedarse con la corrida más reciente de cada archivo:

```powershell
$ultimos = $res | Sort-Object metadata_file, processed_at |
           Group-Object metadata_file | ForEach-Object { $_.Group[-1] }
$ultimos | Measure-Object total_adjuntos, cargados, errores -Sum | Select-Object Property, Sum
$ultimos.Count   # archivos únicos procesados: debe ser el total de CSVs de metadatos
```

---

## 5. Lo NO migrado (la lista de cierre)

```powershell
$err = Import-Csv "$BASE\errores\*.csv"
$err.Count

# Por código de error: 404 = el adjunto no existe en Oracle; 503/TIMEOUT/STREAM = transitorio
$err | Group-Object Code | Sort-Object Count -Descending | Select-Object Name, Count

# Por solicitud, para reclamar/revisar:
$err | Group-Object 'Reference Number' | Sort-Object Count -Descending | Select-Object -First 20

$err | Export-Csv "$BASE\no_migrados.csv" -NoTypeInformation -Encoding UTF8
```

Interpretación:

| `Code` | Qué significa | Acción |
|---|---|---|
| `404`, `403`, `410` | El adjunto no existe o no es accesible en Oracle. Reintentar no cambia nada. | Documentar como no migrable. |
| `503`, `500`, `429` | Oracle estaba caído o saturado. | Relanzar el archivo; suele recuperarse solo. |
| `TIMEOUT`, `CONN`, `STREAM` | Corte de red o de stream. Si recurre siempre para el mismo adjunto, es determinístico. | Relanzar una vez; si insiste, documentar. |

Para reintentar un archivo puntual (sin `overwrite`, para que lo ya subido se omita):

```json
{
  "metadata_csv": "C:\\...\\METADATA\\ServiceRequest_7_6_001_attachments.csv",
  "output_folder": "C:\\...\\ADJUNTOS_ESTADO",
  "destination": "gcp",
  "gcp_bucket": "dev-tablas-migracion-crm",
  "gcp_prefix": "adjuntos",
  "max_workers": 7
}
```

---

## 6. Cuadre final (la verificación que cierra la migración)

La igualdad que debe cumplirse por cada archivo, y en el total:

```
total_adjuntos = cargados + errores
cargados       = downloaded + skipped_existing
```

Y entre fuentes distintas:

```powershell
$idx = Import-Csv "$BASE\indice_maestro.csv"
$ultimos = $res | Sort-Object metadata_file, processed_at |
           Group-Object metadata_file | ForEach-Object { $_.Group[-1] }

$enIndice   = $idx.Count
$enResumen  = ($ultimos | Measure-Object cargados -Sum).Sum
$enErrores  = ($ultimos | Measure-Object errores  -Sum).Sum
$esperados  = ($ultimos | Measure-Object total_adjuntos -Sum).Sum

"Índice:    $enIndice"
"Cargados:  $enResumen   (debe coincidir con el índice)"
"Errores:   $enErrores"
"Esperados: $esperados   (debe ser cargados + errores)"
```

Si **índice ≠ cargados**, casi siempre es porque algún archivo se reprocesó y su índice se regeneró después del resumen que estás sumando: vuelve a bajar los `_index/` y repite.

Contraste contra el origen (los CSVs de metadatos, que son la verdad de cuántos adjuntos había):

```powershell
# Suma de filas con FileContentsHref de todos los *_attachments.csv
$origen = 0
Get-ChildItem "C:\...\METADATA\*_attachments.csv" | ForEach-Object {
    $origen += (Import-Csv $_.FullName | Where-Object { $_.FileContentsHref }).Count
}
"En origen: $origen  |  Esperados según resúmenes: $esperados"
```

Las filas **sin** `FileContentsHref` no son un fallo: son adjuntos sin binario descargable y se cuentan aparte en la columna `sin_href`.

---

## 7. Consolidado de mensajes (si se usó `GetMetadataClobAndMessages`)

```powershell
gcloud storage cp "$BUCKET/mensajes/_index/*_messages.csv" "$BASE\mensajes\"
$msg = Import-Csv "$BASE\mensajes\*.csv"

$msg.Count                                             # mensajes totales
($msg | Group-Object ContentFormat | Select-Object Name, Count)   # cuántos .html vs .txt
($msg | Where-Object { -not $_.Location }).Count       # sin contenido (mensajes sin MessageId)

$msg | Export-Csv "$BASE\mensajes_maestro.csv" -NoTypeInformation -Encoding UTF8
```

Buscar los mensajes de una solicitud:

```powershell
$msg | Where-Object { $_.SrNumber -eq '0002765583' } |
       Select-Object MessageId, CreationDate, MessageTypeCd, ContentFormat, Location
```

---

## 8. Entregables del cierre

| Archivo | Para qué |
|---|---|
| `indice_maestro.csv` | Localizar cualquier adjunto por `Reference Number` (SR → ruta `gs://`). |
| `mensajes_maestro.csv` | Lo mismo para los contenidos de mensajes. |
| `no_migrados.csv` | Lista definitiva de lo que no se pudo migrar, con causa. |
| Totales del punto 4 | Gran total: solicitudes, adjuntos esperados, cargados y errores. |

Conviene subir los tres consolidados al bucket para que queden junto a los datos:

```powershell
gcloud storage cp "$BASE\indice_maestro.csv" "$BASE\no_migrados.csv" "$BUCKET/$PREFIX/_control/"
```
