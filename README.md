# down-attach-osc

API para descargar metadatos y binarios de adjuntos de solicitudes de servicio (Service Requests) de Oracle Service Cloud / Fusion CRM, usando el REST API `crmRestApi` con autenticación **Basic**.

Los dos métodos son **asíncronos**: encolan un job en segundo plano y devuelven de inmediato un `job_id`; el avance y el resultado se consultan en `GET /jobs/{job_id}`.

| Método | Descripción |
|---|---|
| `POST /GetMetadataAttachments` | Toma un **lote** (`batch_size`, por defecto 10) de archivos `ServiceRequest_X_Y_00Z.csv` aún no procesados de la carpeta de entrada (columnas `"Service Request ID","Reference Number"`), consulta los adjuntos de cada `Reference Number` (paginación por `offset` mientras `hasMore` sea `true`) y genera **un CSV de metadatos por archivo de entrada** con todas las columnas de metadatos más el `href` del enclosure `FileContents`. |
| `POST /GetAttachmentBinary` | Toma un CSV de metadatos (`metadata_csv`) o un **lote** de una carpeta de CSVs de metadatos (`metadata_folder`) y, por cada fila, descarga el binario desde el `href` de `FileContents`, guardándolo con el nombre del `FileName` en una subcarpeta por SR. |
| `GET /jobs/{job_id}` | Estatus y avance de un job (`running`, `completed`, `completed_with_errors`, `failed`, `interrupted`). |
| `GET /jobs` | Lista de los últimos jobs. |

Especificación campo a campo de cada método (petición, respuesta, errores y archivos generados):

- [docs/GetMetadataAttachments.md](docs/GetMetadataAttachments.md)
- [docs/GetAttachmentBinary.md](docs/GetAttachmentBinary.md)
- [docs/Jobs.md](docs/Jobs.md)

## Estrategia de lotes y reanudación

- **Lotes**: cada llamada procesa como máximo `batch_size` archivos (por defecto 10; `0` = todos los pendientes). La respuesta indica cuántos quedan pendientes (`pending_after_batch`), así que basta con volver a llamar al método hasta que responda "No hay archivos pendientes".
- **Jobs en paralelo**: cada job reserva sus archivos al iniciar, así que se pueden lanzar varias llamadas en lote sobre la misma carpeta y cada job toma archivos distintos — sin traslapes ni corrupción. Pedir explícitamente (`files` / `metadata_csv`) un archivo que otro job está procesando responde `409`. Ojo: cada job usa sus propios `OSC_MAX_WORKERS` workers; N jobs en paralelo = N × workers llamadas simultáneas a Oracle.
- **No reprocesar**: los archivos completados **sin errores** quedan registrados en un manifiesto dentro de la carpeta de salida (`_processed_files.json` para metadatos, `_downloaded_files.json` para binarios) y no se vuelven a tomar en corridas siguientes. Con `"force": true` se ignora el manifiesto y se reprocesa todo.
- **Escritura en tiempo real (metadatos)**: pensado para archivos de entrada grandes (50k+ líneas). Cada SR consultado se agrega de inmediato al CSV de salida y queda registrado en un checkpoint (`<salida>.progress`). Si el proceso se corta o algunos SR fallan, la siguiente corrida del mismo archivo retoma **solo los SR faltantes** — nunca se repite lo ya consultado. Al completarse el archivo sin errores, el checkpoint se elimina y el archivo pasa al manifiesto.
- **Reintentos**: un archivo que terminó con errores (algún SR o descarga que falló) **no** se marca como procesado, por lo que la siguiente corrida lo vuelve a tomar automáticamente — en metadatos reintenta solo los SR que fallaron (gracias al checkpoint) y en binarios los archivos ya existentes en disco se omiten (`skipped_existing`), salvo que se envíe `"overwrite": true`.
- **Persistencia de jobs**: cada job se guarda en `jobs/<job_id>.json`, así que el estatus se puede consultar aun después de reiniciar el servidor (un job cortado por un reinicio aparece como `interrupted`; basta relanzar el método, el manifiesto evita repetir lo ya hecho).
- **Ctrl+C seguro**: al detener el servidor con Ctrl+C, los jobs en curso cancelan las llamadas pendientes, dejan su estatus como `interrupted` y el proceso termina en segundos (las llamadas ya en vuelo terminan, acotadas por `OSC_TIMEOUT`). Al relanzar el servidor y repetir el llamado, el checkpoint retoma exactamente donde quedó.
- **Verificación**: al terminar cada job, `result.summary` trae un `all_ok` y los totales (SRs esperados/consultados/fallidos en metadatos; adjuntos esperados/en disco/faltantes en binarios). En binarios se recalcula que cada archivo esperado exista físicamente en disco. Cada archivo del lote incluye además su propio bloque `verification`.
- **Log solo de errores**: además de la consola, todos los errores (SRs o descargas que fallan, y verificaciones con faltantes) se escriben en un archivo aparte — `errors.log` por defecto, configurable con `ERROR_LOG_FILE` — para revisarlos sin ruido. Si no hubo errores, queda vacío.

## Requisitos

- Python 3.10 o superior — <https://www.python.org/downloads/> (en Windows, marcar **"Add Python to PATH"** al instalar)
- Git — <https://git-scm.com/download/win>
- Acceso de red al dominio de Oracle y un usuario con permisos sobre el API de Service Requests

## Instalación en Windows

Abrir **PowerShell** y ejecutar:

```powershell
# 1. Clonar el repositorio
git clone https://github.com/DanielSQN/down-attach-osc.git
cd down-attach-osc

# 2. Crear y activar el entorno virtual
py -3 -m venv venv
.\venv\Scripts\Activate.ps1
# Si PowerShell bloquea el script de activación, ejecutar antes:
#   Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Crear el archivo .env con las credenciales
copy .env.example .env
notepad .env
```

Contenido del `.env`:

```ini
OSC_DOMAIN=miempresa.fa.us2.oraclecloud.com   # dominio, sin https://
OSC_USERNAME=usuario.integracion
OSC_PASSWORD=su_contraseña

# Opcionales
OSC_MAX_WORKERS=5   # llamadas en paralelo
OSC_TIMEOUT=60      # timeout por llamada (segundos)
```

> El `.env` está en `.gitignore`: las credenciales nunca se suben al repositorio.

## Ejecutar el API

```powershell
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Con el servidor levantado, la documentación interactiva (Swagger) queda en <http://127.0.0.1:8000/docs> — desde ahí se pueden probar todos los métodos.

## Uso

### 1. GetMetadataAttachments

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/GetMetadataAttachments `
  -ContentType "application/json" `
  -Body '{
    "input_folder":  "C:\\Users\\daniel\\Downloads\\ServiceRequest20260710_2038\\ServiceRequest",
    "output_folder": "C:\\Users\\daniel\\Downloads\\metadatos",
    "batch_size": 10
  }'
```

Parámetros del body:

| Campo | Requerido | Descripción |
|---|---|---|
| `input_folder` | Sí | Carpeta con los `ServiceRequest_*.csv` a leer. |
| `output_folder` | Sí | Carpeta donde se generan los CSVs de metadatos y el manifiesto. |
| `files` | No | Lista de nombres de archivo específicos (dentro de `input_folder`) a procesar, p. ej. `["ServiceRequest_1_1_001.csv", "ServiceRequest_2_3_001.csv"]`. Si se envía, se procesan exactamente esos, ignorando `batch_size` y el manifiesto. |
| `batch_size` | No | Máximo de archivos pendientes a tomar (por defecto 10; `0` = todos). |
| `force` | No | `true` = reprocesar aunque estén en el manifiesto. |

Respuesta inmediata:

```json
{
  "job_id": "6a7f9497a6e8",
  "status": "running",
  "files_in_batch": ["ServiceRequest_1_1_001.csv", "..."],
  "pending_after_batch": 8,
  "status_url": "/jobs/6a7f9497a6e8"
}
```

Genera un `<nombre>_attachments.csv` por archivo de entrada, con columnas: `Service Request ID`, `Reference Number`, `AttachedDocumentId`, `DatatypeCode`, `FileName`, `DmDocumentId`, `UploadedFileContentType`, `UploadedFileLength`, `Title`, `CreationDate`, `CreatedBy`, `FileContentsHref`.

### 2. GetAttachmentBinary

Por carpeta (lotes, recomendado) o por archivo puntual (`"metadata_csv": "...ruta..."`):

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/GetAttachmentBinary `
  -ContentType "application/json" `
  -Body '{
    "metadata_folder": "C:\\Users\\daniel\\Downloads\\metadatos",
    "output_folder":   "C:\\Users\\daniel\\Downloads\\adjuntos",
    "batch_size": 10
  }'
```

Descarga en `output_folder\<Reference Number>\<FileName>`, conservando el nombre original. Solo cuando dos adjuntos del mismo SR comparten el mismo `FileName`, esos se prefijan con el `DmDocumentId` (`<DmDocumentId>_<FileName>`) para que no se pisen ni se pierda ninguno.

### 3. Consultar el estatus de un job

```powershell
Invoke-RestMethod http://127.0.0.1:8000/jobs/6a7f9497a6e8
```

```json
{
  "job_id": "6a7f9497a6e8",
  "type": "GetMetadataAttachments",
  "status": "running",
  "progress": {
    "total_files": 10,
    "processed_files": 3,
    "current_file": "ServiceRequest_1_4_001.csv",
    "srs_consulted": 2140,
    "sr_errors": 2,
    "pending_after_batch": 8
  }
}
```

Al terminar, `status` pasa a `completed` (o `completed_with_errors`) y `result.results` trae el detalle por archivo, incluyendo la lista de errores. Los errores por SR o adjunto no detienen el job.
