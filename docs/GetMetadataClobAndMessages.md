# POST /GetMetadataClobAndMessages

Por cada `Reference Number` de los archivos de entrada, consulta en Oracle los campos CLOB (`arin_comentarios_cifrado_c`, `col_tex_plantilla_c`) y el array `messages`, y genera **dos CSV por archivo de entrada** más un archivo **HTML por cada mensaje**.

Es **asíncrono**: responde con un `job_id`; el avance y el resultado se consultan en [`GET /jobs/{job_id}`](Jobs.md).

- **URL**: `http://<host>:8000/GetMetadataClobAndMessages`
- **Método**: `POST` · **Content-Type**: `application/json`

## Llamadas a Oracle

Por cada SR:

1. `GET /crmRestApi/resources/11.13.18.05/serviceRequests/<srNumber>?fields=arin_comentarios_cifrado_c,col_tex_plantilla_c,messages&onlyData=true`
2. Por cada mensaje del array `messages`: `GET .../serviceRequests/<srNumber>/child/messages/<MessageId>/enclosure/MessageContent` (devuelve HTML)

## Petición (body)

| Campo | Tipo | Requerido | Default | Descripción |
|---|---|---|---|---|
| `input_folder` | string | Sí | — | Carpeta con los `ServiceRequest_*.csv` (columnas `"Service Request ID"`, `"Reference Number"`). |
| `output_folder` | string | Sí | — | Carpeta de salida (CSVs, HTMLs, manifiesto y checkpoints). Se crea si no existe. |
| `files` | array de string | No | `null` | Archivos específicos a procesar; ignora `batch_size` y el manifiesto. |
| `batch_size` | entero | No | `10` | Máximo de archivos pendientes por llamada. `0` = todos. |
| `force` | booleano | No | `false` | `true` = reprocesa desde cero (borra CSVs y checkpoint del lote). |
| `max_workers` | entero (1–64) | No | `OSC_MAX_WORKERS` | SR en paralelo solo para este job. |
| `overwrite_html` | booleano | No | `false` | `true` = vuelve a descargar los HTML ya existentes en disco. |

```json
{
  "input_folder":  "C:\\...\\ServiceRequest",
  "output_folder": "C:\\...\\clob_messages",
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
| `MessageContent` | Ruta relativa (dentro de `output_folder`) al archivo HTML del mensaje, p. ej. `message_content/0002765583/3000057733516.html` |

### HTML del contenido de mensajes

Cada `MessageContent` se guarda como `output_folder/message_content/<srNumber>/<MessageId>.html`. Se eligió archivo (no inline en el CSV) para evitar el límite de 32.767 caracteres por celda de Excel y los saltos de línea del HTML; queda abrible directo en el navegador.

## Resultado del job

Al terminar, `result.summary` = `{ files, expected_srs, consulted, failed_srs, messages, all_ok }`. Cada archivo en `result.results` trae `clob_file`, `messages_file`, `service_requests`, `resumed_srs`, `messages`, `errors` y `verification` (`{ expected_srs, consulted, failed, ok }`).

## Reanudación y robustez

Mismo comportamiento que los demás métodos: checkpoint por SR (`<nombre>_messages.csv.progress`), manifiesto `_processed_clob_messages.json`, reintentos automáticos ante 5xx/429/red, circuit breaker, Ctrl+C seguro y jobs en paralelo. Si se corta, relanzar retoma solo los SR faltantes; los HTML ya en disco se omiten (salvo `overwrite_html`).

> **Nota sobre `messages`**: los mensajes se leen del array que devuelve la consulta con `fields=...,messages`. Si un SR tuviera muchísimos mensajes y Oracle los truncara en esa respuesta inline, avisar para añadir paginación por el recurso hijo `child/messages`.
