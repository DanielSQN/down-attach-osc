# POST /GetMetadataAttachments

Lee archivos `ServiceRequest_*.csv` de una carpeta de entrada, consulta los adjuntos de cada `Reference Number` en Oracle Service Cloud (con autenticación Basic y paginación por `offset`) y genera un CSV de metadatos por cada archivo de entrada.

Es **asíncrono**: responde de inmediato con un `job_id`; el avance y el resultado se consultan en [`GET /jobs/{job_id}`](Jobs.md).

- **URL**: `http://<host>:8000/GetMetadataAttachments`
- **Método**: `POST`
- **Content-Type**: `application/json`

---

## Petición (body)

| Campo | Tipo | Requerido | Default | Descripción |
|---|---|---|---|---|
| `input_folder` | string | Sí | — | Ruta absoluta de la carpeta con los CSVs de entrada. Cada archivo debe tener las columnas `"Service Request ID"` y `"Reference Number"`. Se ignoran archivos que no terminen en `.csv` o que empiecen con `_`. En Windows, escapar las barras: `"C:\\Users\\..."`. |
| `output_folder` | string | Sí | — | Carpeta donde se generan los CSVs de metadatos, el manifiesto `_processed_files.json` y los checkpoints `.progress`. Se crea si no existe. |
| `files` | array de string | No | `null` | Nombres de archivos específicos (dentro de `input_folder`) a procesar, p. ej. `["ServiceRequest_1_1_001.csv"]`. Si se envía, se procesan exactamente esos e **ignora** `batch_size` y el manifiesto. Si alguno no existe, responde 400. |
| `batch_size` | entero | No | `10` | Máximo de archivos **pendientes** a tomar en esta llamada. `0` = todos los pendientes. Solo aplica cuando no se envía `files`. |
| `force` | booleano | No | `false` | `true` = ignora el manifiesto y el checkpoint: reprocesa desde cero (borra el CSV de salida y el `.progress` de cada archivo del lote). |

### Ejemplo

```json
{
  "input_folder":  "C:\\Users\\daniel\\Downloads\\ServiceRequest20260710_2038\\ServiceRequest",
  "output_folder": "C:\\Users\\daniel\\Downloads\\metadatos",
  "batch_size": 10
}
```

---

## Respuesta inmediata (200)

Cuando hay archivos pendientes y el job queda lanzado:

| Campo | Tipo | Descripción |
|---|---|---|
| `job_id` | string | Identificador del job en segundo plano (12 caracteres hex). Usarlo en `GET /jobs/{job_id}`. |
| `status` | string | Siempre `"running"` en esta respuesta. |
| `files_in_batch` | array de string | Nombres de los archivos de entrada que este job va a procesar. |
| `pending_after_batch` | entero | Archivos que quedan pendientes después de este lote (volver a llamar al método para tomarlos). |
| `status_url` | string | Ruta relativa para consultar el job: `/jobs/{job_id}`. |

```json
{
  "job_id": "6a7f9497a6e8",
  "status": "running",
  "files_in_batch": ["ServiceRequest_1_1_001.csv", "ServiceRequest_1_2_001.csv"],
  "pending_after_batch": 8,
  "status_url": "/jobs/6a7f9497a6e8"
}
```

Cuando **no** hay archivos pendientes (todos en el manifiesto):

| Campo | Tipo | Descripción |
|---|---|---|
| `job_id` | null | No se lanzó ningún job. |
| `message` | string | Explicación: todos los archivos están registrados en `_processed_files.json`. |
| `total_files` | entero | Total de CSVs encontrados en `input_folder`. |

---

## Errores

| Código | Causa |
|---|---|
| `400` | `input_folder` no existe; la carpeta no contiene CSVs; algún nombre de `files` no existe en la carpeta. El detalle viene en el campo `detail`. |
| `409` | Algún archivo de `files` está siendo procesado por otro job en este momento. Esperar a que ese job termine o pedir otros archivos. |
| `422` | Body inválido (campo faltante o de tipo incorrecto). |
| `500` | Falta alguna variable en el `.env` (`OSC_DOMAIN`, `OSC_USERNAME`, `OSC_PASSWORD`). |

> **Jobs en paralelo**: se pueden lanzar varias llamadas en lote a la vez sobre la misma carpeta; cada job reserva sus archivos al iniciar y los demás toman los siguientes pendientes, sin traslapes.

---

## Resultado final del job

Al terminar, `GET /jobs/{job_id}` devuelve `status` `completed` (todo sin errores), `completed_with_errors` (algún SR falló) o `interrupted` (cortado por Ctrl+C/reinicio). El campo `result.results` es un array con un objeto por archivo procesado:

| Campo | Tipo | Descripción |
|---|---|---|
| `input_file` | string | Nombre del CSV de entrada. |
| `output_file` | string | Ruta del CSV de metadatos generado. |
| `service_requests` | entero | Reference Numbers únicos del archivo. |
| `resumed_srs` | entero | SRs que ya estaban consultados en el checkpoint y no se repitieron. |
| `attachments` | entero | Filas (adjuntos) totales en el CSV de salida. |
| `errors` | array | SRs que fallaron: objetos `{ "srNumber": string, "error": string }`. Se reintentan solos en la siguiente corrida. |
| `error` | string | Solo presente si el archivo completo no se pudo procesar (p. ej. columna faltante). |

---

## CSV de salida generado

Un archivo `<nombre_entrada>_attachments.csv` por cada CSV de entrada (codificación UTF-8 con BOM), con una fila por adjunto:

| Columna | Origen | Descripción |
|---|---|---|
| `Service Request ID` | CSV de entrada | ID interno de la solicitud, copiado del archivo de entrada. |
| `Reference Number` | CSV de entrada | Número de solicitud (srNumber) usado para llamar al API. |
| `AttachedDocumentId` | API Oracle | ID del documento adjunto en la solicitud. |
| `DatatypeCode` | API Oracle | Tipo de dato del adjunto (p. ej. `FILE`, `WEB_PAGE`). |
| `FileName` | API Oracle | Nombre del archivo adjunto. Con este nombre se guarda el binario. |
| `DmDocumentId` | API Oracle | ID del documento en el gestor documental (UCM). |
| `UploadedFileContentType` | API Oracle | Content-Type del binario (p. ej. `application/pdf`). |
| `UploadedFileLength` | API Oracle | Tamaño del binario en bytes. |
| `Title` | API Oracle | Título del adjunto. |
| `CreationDate` | API Oracle | Fecha de creación del adjunto. |
| `CreatedBy` | API Oracle | Usuario que cargó el adjunto. |
| `FileContentsHref` | API Oracle (links) | URL del enclosure para descargar el binario: el link con `rel = "enclosure"` y `name = "FileContents"`. Es la entrada de [GetAttachmentBinary](GetAttachmentBinary.md). |

Archivos auxiliares en `output_folder`:

- `_processed_files.json` — manifiesto de archivos completados sin errores (no se vuelven a tomar).
- `<salida>.progress` — checkpoint con los SRs ya consultados de un archivo **en curso o incompleto**; permite reanudar sin repetir. Se elimina al completarse el archivo sin errores.
