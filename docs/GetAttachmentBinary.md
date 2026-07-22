# POST /GetAttachmentBinary

Lee uno o varios CSVs de metadatos (generados por [GetMetadataAttachments](GetMetadataAttachments.md)) y, por cada fila, descarga el binario del adjunto desde la URL de `FileContentsHref`, guardándolo como `output_folder/<Reference Number>/<FileName>`.

Es **asíncrono**: responde de inmediato con un `job_id`; el avance y el resultado se consultan en [`GET /jobs/{job_id}`](Jobs.md).

- **URL**: `http://<host>:8000/GetAttachmentBinary`
- **Método**: `POST`
- **Content-Type**: `application/json`

---

## Petición (body)

Debe enviarse **exactamente uno** de `metadata_csv` o `metadata_folder`.

| Campo | Tipo | Requerido | Default | Descripción |
|---|---|---|---|---|
| `metadata_csv` | string | Uno de los dos | `null` | Ruta absoluta de **un** CSV de metadatos a procesar. Se procesa siempre (no consulta el manifiesto), pero los binarios ya existentes en disco se omiten salvo `overwrite`. |
| `metadata_folder` | string | Uno de los dos | `null` | Carpeta con CSVs de metadatos; se procesan por lotes según `batch_size`, saltando los registrados en el manifiesto `_downloaded_files.json`. Se ignoran archivos que no terminen en `.csv` o que empiecen con `_`. |
| `output_folder` | string | Sí | — | Carpeta raíz donde se guardan los binarios (una subcarpeta por `Reference Number`). Se crea si no existe. Aquí también vive el manifiesto `_downloaded_files.json`. |
| `batch_size` | entero | No | `10` | Máximo de CSVs de metadatos pendientes a tomar. `0` = todos. Solo aplica con `metadata_folder`. |
| `overwrite` | booleano | No | `false` | `true` = volver a descargar y sobrescribir binarios que ya existen en disco. Con `false` se omiten (`skipped_existing`), lo que hace baratos los reintentos. |
| `force` | booleano | No | `false` | `true` = ignorar el manifiesto `_downloaded_files.json` y volver a tomar todos los CSVs de la carpeta. Solo aplica con `metadata_folder`. |
| `max_workers` | entero (1–64) | No | `OSC_MAX_WORKERS` | Descargas en paralelo solo para este job, sin editar el `.env`. |
| `destination` | string | No | `"local"` | Destino de los binarios: `"local"` (guarda en `output_folder`) o `"gcp"` (sube a un bucket de GCP). |
| `gcp_bucket` | string | Requerido si `destination=gcp` | `null` | Nombre del bucket de GCP destino. |
| `gcp_prefix` | string | No | `""` | Prefijo (carpeta) dentro del bucket. Los objetos quedan en `<prefix>/<Reference Number>/<FileName>`. |

### Destino GCP

Con `destination: "gcp"` el binario se transmite **directamente desde Oracle al bucket** (sin pasar por disco local). El objeto se nombra igual que en local: `<gcp_prefix>/<Reference Number>/<FileName>` (con el prefijo `DmDocumentId` solo en colisiones). Requiere una **cuenta de servicio** con permiso de escritura en el bucket, configurada en el `.env`:

```ini
GCP_SERVICE_ACCOUNT_FILE=C:\\ruta\\cuenta-servicio.json
```

Si se deja vacío, usa las credenciales por defecto de GCP (ADC / `GOOGLE_APPLICATION_CREDENTIALS`). El `output_folder` sigue siendo obligatorio: ahí viven el manifiesto `_downloaded_files.json` y los checkpoints (los binarios NO se guardan en local). Para la reanudación, al iniciar cada archivo se lista una vez el prefijo del bucket y se omiten los objetos ya presentes (`skipped_existing`).

> Requiere la dependencia `google-cloud-storage` (incluida en `requirements.txt`). Si falta, `destination=gcp` responde 500.
>
> **Detalle completo de la conexión a GCP** (crear la cuenta de servicio, estructura del JSON, permisos del bucket, red/proxy y cómo probar): ver [GCP_CuentaDeServicio.md](GCP_CuentaDeServicio.md).

### Ejemplo

```json
{
  "metadata_folder": "C:\\Users\\daniel\\Downloads\\metadatos",
  "output_folder":   "C:\\Users\\daniel\\Downloads\\adjuntos",
  "batch_size": 10
}
```

---

## Respuesta inmediata (200)

Cuando hay trabajo por hacer y el job queda lanzado:

| Campo | Tipo | Descripción |
|---|---|---|
| `job_id` | string | Identificador del job en segundo plano. Usarlo en `GET /jobs/{job_id}`. |
| `status` | string | Siempre `"running"` en esta respuesta. |
| `files_in_batch` | array de string | Nombres de los CSVs de metadatos que este job va a procesar. |
| `pending_after_batch` | entero | CSVs que quedan pendientes después de este lote (`0` cuando se usó `metadata_csv`). |
| `status_url` | string | Ruta relativa para consultar el job: `/jobs/{job_id}`. |

Cuando **no** hay CSVs pendientes (solo en modo `metadata_folder`):

| Campo | Tipo | Descripción |
|---|---|---|
| `job_id` | null | No se lanzó ningún job. |
| `message` | string | Explicación: todos los CSVs están registrados en `_downloaded_files.json`. |
| `total_files` | entero | Total de CSVs encontrados en `metadata_folder`. |

---

## Errores

| Código | Causa |
|---|---|
| `400` | `metadata_csv` no existe; `metadata_folder` no existe o no contiene CSVs. El detalle viene en el campo `detail`. |
| `409` | El `metadata_csv` pedido está siendo procesado por otro job en este momento. |
| `422` | Body inválido; se enviaron ambos (o ninguno) de `metadata_csv` / `metadata_folder`; o `destination=gcp` sin `gcp_bucket`. |
| `500` | Falta alguna variable en el `.env` (`OSC_DOMAIN`, `OSC_USERNAME`, `OSC_PASSWORD`); o no se pudo inicializar GCP (credenciales/bucket/dependencia). |

> **Jobs en paralelo**: se pueden lanzar varias llamadas con `metadata_folder` a la vez; cada job reserva sus CSVs al iniciar y los demás toman los siguientes pendientes, sin traslapes.

---

## Resultado final del job

Al terminar, `GET /jobs/{job_id}` devuelve `status` `completed`, `completed_with_errors` o `interrupted`. El campo `result.results` es un array con un objeto por CSV de metadatos procesado:

| Campo | Tipo | Descripción |
|---|---|---|
| `metadata_file` | string | Nombre del CSV de metadatos. |
| `total_rows` | entero | Filas del CSV con `FileContentsHref` (adjuntos a descargar). |
| `downloaded` | entero | Binarios descargados en esta corrida. |
| `skipped_existing` | entero | Binarios omitidos por ya existir en disco. |
| `errors` | array | Descargas fallidas: objetos `{ "srNumber": string, "fileName": string, "error": string }`. Se reintentan solos en la siguiente corrida. |
| `destination` | string | `"local"` o `"gcp"`, el destino usado. |
| `verification` | objeto | Confirmación de que cada adjunto quedó guardado: `{ "expected", "stored", "missing_count", "missing_sample", "ok" }`. Una subida/escritura que no lanzó error queda confirmada; `ok` es `true` si no falta ninguno. `missing_sample` lista hasta 20 rutas relativas faltantes. |
| `error` | string | Solo presente si el CSV completo no se pudo procesar (p. ej. sin columna `FileContentsHref`). |

Además, `result.summary` agrega el total del job: `{ "files", "expected", "downloaded", "skipped_existing", "missing", "all_ok" }`. `all_ok: true` significa que todos los adjuntos esperados quedaron en disco.

---

## Archivo de control (auditoría)

Por cada CSV de metadata procesado se genera en `output_folder` un **`<nombre>_control.csv`** con una fila por adjunto, que registra dónde quedó cada uno y con qué estado (aplica a destino local y GCP):

| Columna | Descripción |
|---|---|
| `Reference Number` | El srNumber del adjunto. |
| `FileName` | Nombre original del adjunto. |
| `StoredAs` | Ruta relativa del objeto (`<Reference Number>/<FileName>`, con prefijo `DmDocumentId` en colisiones). |
| `Location` | Ubicación final: `gs://<bucket>/<prefix>/<StoredAs>` (GCP) o la ruta local completa. |
| `Status` | `downloaded`, `skipped_existing` o `error`. |
| `Error` | Mensaje del error si `Status=error`. |

Se reescribe completo en cada corrida (una corrida completa procesa todas las filas, descargando u omitiendo), así refleja el estado actual. La ruta del control también viene en `result.results[].control_file`.

## Archivos generados

- Binarios en `output_folder/<Reference Number>/<FileName>`. Los adjuntos se guardan con su `FileName` original. **Solo cuando dos o más adjuntos del mismo SR comparten el mismo `FileName`** (colisión dentro del CSV), esos se prefijan con el `DmDocumentId` (único por adjunto) → `<DmDocumentId>_<FileName>`, para que ninguno se pierda; si no viniera `DmDocumentId` se usa `AttachedDocumentId`. Los que no colisionan mantienen su nombre tal cual. Los caracteres inválidos para Windows en el nombre (`< > : " / \ | ? *`) se reemplazan por `_`. Si la fila no trae `FileName`, se usa `Title` y en último caso `adjunto`.
- `_downloaded_files.json` — manifiesto (en `output_folder`) de CSVs de metadatos completados sin errores; no se vuelven a tomar en modo `metadata_folder`.

> Nota técnica: la descarga envía `Accept: */*` (el enclosure devuelve binario; pedir JSON produce 406 en Oracle).
