# POST /GetMetadataClobAndMessages

Por cada `Reference Number` de los archivos de entrada, consulta en Oracle los campos CLOB (`arin_comentarios_cifrado_c`, `col_tex_plantilla_c`) y el array `messages`, y genera **dos CSV por archivo de entrada** más un archivo **`.html` o `.txt` por cada mensaje** (la extensión la define el `MessageTypeCd`). Los contenidos pueden guardarse en **disco local o subirse a un bucket de GCP**.

Es **asíncrono**: responde con un `job_id`; el avance y el resultado se consultan en [`GET /jobs/{job_id}`](Jobs.md).

- **URL**: `http://<host>:8000/GetMetadataClobAndMessages`
- **Método**: `POST` · **Content-Type**: `application/json`

## Llamadas a Oracle

Por cada SR:

1. `GET /crmRestApi/resources/11.13.18.05/serviceRequests/<srNumber>?fields=arin_comentarios_cifrado_c,col_tex_plantilla_c,messages&onlyData=true`
2. Por cada mensaje del array `messages`: `GET .../serviceRequests/<srNumber>/child/messages/<MessageId>/enclosure/MessageContent`

## Petición (body)

| Campo | Tipo | Requerido | Default | Descripción |
|---|---|---|---|---|
| `input_folder` | string | Sí | — | Carpeta con los `ServiceRequest_*.csv` (columnas `"Service Request ID"`, `"Reference Number"`). |
| `output_folder` | string | Sí | — | Carpeta de salida (CSVs, manifiesto y checkpoints; y los contenidos si `destination=local`). Se crea si no existe. |
| `files` | array de string | No | `null` | Archivos específicos a procesar; ignora `batch_size` y el manifiesto. |
| `batch_size` | entero | No | `10` | Máximo de archivos pendientes por llamada. `0` = todos. |
| `force` | booleano | No | `false` | `true` = reprocesa desde cero (borra CSVs y checkpoint del lote). |
| `max_workers` | entero (1–64) | No | `OSC_MAX_WORKERS` | SR en paralelo solo para este job. |
| `overwrite_html` | booleano | No | `false` | `true` = vuelve a guardar los contenidos ya existentes en destino. |
| `destination` | string | No | `"local"` | `"local"` (en `output_folder`) o `"gcp"` (bucket). |
| `gcp_bucket` | string | Si `gcp` | — | Bucket destino de los contenidos. |
| `gcp_prefix` | string | No | `""` | Prefijo dentro del bucket. |
| `html_message_types` | array de string | No | `OSC_HTML_MESSAGE_TYPES` | `MessageTypeCd` que se guardan como `.html`; el resto va como `.txt`. |
| `worker_index` / `worker_count` | entero | No | `0` / `1` | Partición multi-máquina (ver [MultiMaquina.md](MultiMaquina.md)). |

```json
{
  "input_folder":  "C:\\...\\ServiceRequest",
  "output_folder": "C:\\...\\clob_messages",
  "destination": "gcp",
  "gcp_bucket": "dev-tablas-migracion-crm",
  "gcp_prefix": "mensajes",
  "batch_size": 10
}
```

## Respuesta inmediata (200)

Igual que los otros métodos: `job_id`, `status: "running"`, `files_in_batch`, `pending_after_batch`, `status_url`. Si no hay pendientes, `job_id: null` con `message` y `total_files`.

## Errores

| Código | Causa |
|---|---|
| `400` | `input_folder` no existe / sin CSVs / algún `files` no existe. |
| `409` | Algún archivo de `files` está en proceso por otro job. |
| `422` | Body inválido. |
| `500` | Falta alguna variable en el `.env`. |

## Archivos generados (por cada archivo de entrada)

### `<nombre>_clob.csv`

Una fila por SR, con los CLOB **decodificados a texto** (vienen en base64; si el campo es `null` queda vacío):

| Columna | Descripción |
|---|---|
| `Reference Number` | El srNumber consultado. |
| `arin_comentarios_cifrado_c` | Texto decodificado (base64 → UTF-8). |
| `col_tex_plantilla_c` | Texto decodificado (base64 → UTF-8). |

### `<nombre>_messages.csv`

Una fila por mensaje de cada SR:

| Columna | Origen |
|---|---|
| `MessageId`, `CreationDate`, `CreatedBy`, `SrId`, `SrNumber`, `MessageTypeCd`, `ChannelTypeCd`, `ChannelId`, `StatusCd`, `ProcessingStatusCd`, `NotificationProcessingStatusCd`, `TemplateName` | Campos del objeto `messages` |
| `MessageContent` | Ruta relativa del archivo del mensaje, p. ej. `message_content/0002765583/3000057733516.html` |
| `ContentFormat` | `"html"` o `"txt"` (según `MessageTypeCd`). |
| `Location` | **Ruta completa en destino**: `gs://<bucket>/<prefix>/message_content/<SR>/<MessageId>.html` con `destination=gcp`, o la ruta local con `destination=local`. |

Un mensaje sin `MessageId` no tiene contenido descargable: sus tres columnas quedan vacías y la fila se conserva con su metadata.

**Este CSV es el índice**: trae la metadata de cada mensaje **más la ruta donde quedó su contenido**. Con `destination=gcp` se sube además a `gs://<bucket>/<prefix>/_index/<nombre>_messages.csv` al terminar el archivo, para consultarlo junto a los contenidos sin depender de la máquina que lo procesó. Solo se sube si el archivo quedó completo (sin errores reintentables), para no publicar un índice parcial.

### Contenido de los mensajes: `.html` vs `.txt`

Cada contenido se guarda como `message_content/<srNumber>/<MessageId>.<ext>` (bajo `output_folder` en local, o bajo el `gcp_prefix` en el bucket). Se eligió archivo (no inline en el CSV) para evitar el límite de 32.767 caracteres por celda de Excel y los saltos de línea del contenido.

La extensión la decide el **`MessageTypeCd`** del mensaje:

| `MessageTypeCd` | Extensión | Content-Type en GCP |
|---|---|---|
| `ORA_SVC_RESPONSE` (respuestas salientes del agente) | `.html` | `text/html; charset=utf-8` |
| Cualquier otro (`ORA_SVC_CUSTOMER_ENTRY`, `ORA_SVC_INTERNAL_NOTE`, `ORA_SVC_SYSTEM_RESPONSE`, `ORA_SVC_SYSTEM_NOTE`, vacío…) | `.txt` | `text/plain; charset=utf-8` |

`ORA_SVC_RESPONSE` es el único que Oracle devuelve como HTML en las pruebas. Si aparece otro tipo con HTML, se agrega **sin tocar código**: variable `OSC_HTML_MESSAGE_TYPES` en el `.env` (lista separada por comas) o el campo `html_message_types` en el body de la llamada.

> Al fijar el `Content-Type` correcto, los `.html` se abren renderizados desde el bucket y los `.txt` como texto plano, en vez de descargarse como binario genérico.

## Resultado del job

Al terminar, `result.summary` = `{ files, expected_srs, consulted, failed_srs, messages, html_saved, txt_saved, skipped_existing, all_ok }`. Cada archivo en `result.results` trae `clob_file`, `messages_file`, `index_location` (URI del índice en el bucket, `null` en local), `destination`, `service_requests`, `resumed_srs`, `messages`, `html_saved`, `txt_saved`, `skipped_existing`, `errors` y `verification` (`{ expected_srs, consulted, failed, ok }`).

## Reanudación y robustez

Mismo comportamiento que los demás métodos: checkpoint por SR (`<nombre>_messages.csv.progress`), manifiesto `_processed_clob_messages.json`, reintentos automáticos ante 5xx/429/red, circuit breaker, Ctrl+C seguro y jobs en paralelo. Si se corta, relanzar retoma solo los SR faltantes; los contenidos ya guardados se omiten (`skipped_existing`), salvo `overwrite_html`. Con `destination=gcp` la reanudación se apoya en un listado del prefijo hecho **una vez por job**, igual que en `GetAttachmentBinary`.

> **Si ya corrió este método con la versión anterior** (que guardaba todo como `.html`): los mensajes que ahora corresponden a `.txt` no se reconocen como existentes y se volverán a descargar con la extensión correcta; los `.html` viejos de esos mensajes quedan huérfanos y conviene borrarlos. Los `ORA_SVC_RESPONSE` no cambian y se omiten normalmente.

> **Nota sobre `messages`**: los mensajes se leen del array que devuelve la consulta con `fields=...,messages`. Si un SR tuviera muchísimos mensajes y Oracle los truncara en esa respuesta inline, avisar para añadir paginación por el recurso hijo `child/messages`.
