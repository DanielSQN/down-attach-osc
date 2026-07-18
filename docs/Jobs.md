# Consulta de jobs y health check

Los dos métodos de negocio ([GetMetadataAttachments](GetMetadataAttachments.md) y [GetAttachmentBinary](GetAttachmentBinary.md)) corren en segundo plano y devuelven un `job_id`. Estos endpoints permiten consultar su avance y resultado. Cada job se persiste en `jobs/<job_id>.json`, por lo que puede consultarse incluso después de reiniciar el servidor.

---

## GET /jobs/{job_id}

Devuelve el detalle completo de un job.

- **URL**: `http://<host>:8000/jobs/{job_id}`
- **Método**: `GET`

### Parámetros de ruta

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `job_id` | string | Sí | El identificador devuelto al lanzar el método. |

### Respuesta (200)

| Campo | Tipo | Descripción |
|---|---|---|
| `job_id` | string | Identificador del job. |
| `type` | string | `"GetMetadataAttachments"` o `"GetAttachmentBinary"`. |
| `status` | string | Estado actual. Valores: ver tabla de estados más abajo. |
| `created_at` | string (ISO 8601 UTC) | Momento en que se lanzó el job. |
| `finished_at` | string \| null | Momento en que terminó; `null` mientras corre. |
| `params` | objeto | El body exacto con el que se lanzó el método (para trazabilidad). |
| `progress` | objeto | Contadores de avance en vivo; campos según el tipo de job (ver abajo). |
| `result` | objeto \| null | Solo al terminar: `{ "results": [...] }` con un objeto por archivo procesado (campos documentados en el MD de cada método). |
| `error` | string \| null | Solo si `status = "failed"`: mensaje del error fatal que tumbó el job. |

### Estados posibles (`status`)

| Valor | Significado |
|---|---|
| `running` | El job está en ejecución. |
| `completed` | Terminó y todos los archivos se procesaron sin errores. |
| `completed_with_errors` | Terminó, pero algún SR o descarga falló (detalle en `result.results[].errors`); los fallidos se reintentan en la siguiente corrida del método. |
| `interrupted` | Cortado por apagado del servidor (Ctrl+C o reinicio), **o por el circuit breaker** (demasiados fallos transitorios consecutivos: servicio caído/mantenimiento — el motivo queda en el campo `error`). Lo ya hecho quedó en disco; verificar con `GET /health` que el servicio volvió y relanzar el método: retoma desde el checkpoint/manifiesto. |
| `failed` | Error fatal inesperado; ver campo `error`. |

### Campos de `progress` — job GetMetadataAttachments

| Campo | Tipo | Descripción |
|---|---|---|
| `total_files` | entero | Archivos de entrada en el lote de este job. |
| `processed_files` | entero | Archivos ya terminados. |
| `pending_after_batch` | entero | Archivos de la carpeta que quedaron fuera del lote. |
| `current_file` | string \| null | Archivo en proceso en este momento (`null` entre archivos o al terminar). |
| `current_file_srs` | entero | Reference Numbers únicos del archivo en curso. |
| `current_file_pending` | entero | SRs del archivo en curso que faltaban al iniciar (descontando el checkpoint). |
| `srs_consulted` | entero | SRs consultados con éxito, acumulado del job. Se persiste a disco cada 25; el valor en vivo puede ir unos pasos adelante del archivo JSON. |
| `sr_errors` | entero | SRs cuya consulta falló, acumulado del job. |

### Campos de `progress` — job GetAttachmentBinary

| Campo | Tipo | Descripción |
|---|---|---|
| `total_files` | entero | CSVs de metadatos en el lote de este job. |
| `processed_files` | entero | CSVs ya terminados. |
| `pending_after_batch` | entero | CSVs de la carpeta que quedaron fuera del lote. |
| `current_file` | string \| null | CSV en proceso en este momento. |
| `current_file_rows` | entero | Filas (adjuntos) del CSV en curso. |
| `downloaded` | entero | Binarios descargados, acumulado del job. |
| `skipped_existing` | entero | Binarios omitidos por ya existir en disco, acumulado. |
| `download_errors` | entero | Descargas fallidas, acumulado. |

### Errores

| Código | Causa |
|---|---|
| `404` | No existe un job con ese `job_id` (ni en memoria ni en `jobs/`). |

### Ejemplo

```json
{
  "job_id": "6a7f9497a6e8",
  "type": "GetMetadataAttachments",
  "status": "running",
  "created_at": "2026-07-17T14:02:11+00:00",
  "finished_at": null,
  "params": {
    "input_folder": "C:\\Users\\daniel\\Downloads\\ServiceRequest",
    "output_folder": "C:\\Users\\daniel\\Downloads\\metadatos",
    "files": null,
    "batch_size": 10,
    "force": false
  },
  "progress": {
    "total_files": 10,
    "processed_files": 3,
    "pending_after_batch": 8,
    "current_file": "ServiceRequest_1_4_001.csv",
    "current_file_srs": 50000,
    "current_file_pending": 21500,
    "srs_consulted": 178500,
    "sr_errors": 12
  },
  "result": null,
  "error": null
}
```

---

## GET /jobs

Lista los últimos jobs (de ambos tipos), el más reciente primero.

- **URL**: `http://<host>:8000/jobs`
- **Método**: `GET`

### Parámetros de query

| Campo | Tipo | Requerido | Default | Descripción |
|---|---|---|---|---|
| `limit` | entero | No | `20` | Máximo de jobs a devolver. |

### Respuesta (200)

| Campo | Tipo | Descripción |
|---|---|---|
| `jobs` | array | Resumen de cada job, ordenados por `created_at` descendente. |

Cada elemento de `jobs` trae un subconjunto de los campos del detalle: `job_id`, `type`, `status`, `created_at`, `finished_at` y `progress` (sin `params`, `result` ni `error` — para eso usar `GET /jobs/{job_id}`).

---

## GET /health

Hace una llamada mínima al API de Oracle (1 registro, solo el campo `SrNumber`) para saber si el servicio responde. Útil para verificar que un mantenimiento terminó antes de relanzar los jobs. No usa reintentos: refleja el estado actual.

- **URL**: `http://<host>:8000/health`
- **Método**: `GET`

### Respuesta (200)

| Campo | Tipo | Descripción |
|---|---|---|
| `oracle_ok` | booleano | `true` si Oracle respondió `200` a la llamada de prueba. |
| `status_code` | entero | Código HTTP devuelto por Oracle (ausente si ni siquiera hubo respuesta). |
| `error` | string | Solo si la llamada falló por red/timeout: el detalle del error. |
| `elapsed_ms` | entero | Milisegundos que tardó la llamada. |

```powershell
# Esperar a que Oracle vuelva de mantenimiento (revisa cada 60 s):
while (-not (Invoke-RestMethod http://127.0.0.1:8000/health).oracle_ok) { Start-Sleep 60 }
```
