# Procesar muchos archivos en varias máquinas / procesos

Guía para el caso de **70 archivos de ~50k líneas** repartidos en varias máquinas subiendo al **mismo bucket de GCP**.

## Cómo se comporta el programa con varios jobs / máquinas

- **Cada job es independiente**: su propio cliente de GCP (su propio pool de conexiones dimensionado a `max_workers`) y su propia autenticación. Varios jobs no comparten conexión ni se estorban a ese nivel.
- **El bucket es compartido**: las subidas son peticiones HTTP independientes. Objetos distintos → sin problema. El mismo objeto subido por dos jobs → gana el último, contenido idéntico (inofensivo, solo desperdicio).
- **Reanudación por el bucket**: cada job lista el prefix y omite lo ya subido (`skipped_existing`).
- **Límite**: la reserva de "qué archivo tomar" es **en memoria por proceso**. Dos máquinas NO se coordinan solas. **Hay que particionar** para que no tomen los mismos archivos.

## La técnica recomendada: partición por worker

Los métodos de lote (`GetMetadataAttachments`, `GetAttachmentBinary` con `metadata_folder`, `GetMetadataClobAndMessages`) aceptan `worker_index` y `worker_count`. Cada máquina lanza **el mismo llamado** cambiando solo `worker_index`, y procesa una porción **disjunta y determinística** de los archivos (por posición en la lista ordenada). Sin coordinación, sin locks, sin listas de `files` a mano.

Reglas:
1. **Mismo `worker_count`** en todas las máquinas; **`worker_index` distinto** (0..N-1).
2. **Cada máquina su propia `output_folder` local** (manifiesto, checkpoints, jobs y controles).
3. **Mismo `gcp_bucket` y `gcp_prefix`** en todas (los objetos no se solapan porque los archivos no se solapan).

### Ejemplo: 70 archivos en 3 máquinas

```jsonc
// Máquina A
{ "metadata_folder": "...\\METADATA", "output_folder": "D:\\estadoA",
  "destination": "gcp", "gcp_bucket": "dev-...", "gcp_prefix": "adjuntos",
  "batch_size": 0, "max_workers": 15, "worker_index": 0, "worker_count": 3 }

// Máquina B  -> worker_index: 1
// Máquina C  -> worker_index: 2
```

`batch_size: 0` hace que cada worker tome toda su partición en un job. La A procesa los archivos 0,3,6,…; la B los 1,4,7,…; la C los 2,5,8,…

## Flujo completo de la migración (2 fases)

1. **Metadatos** (`GetMetadataAttachments`) en las N máquinas con `worker_index`/`worker_count` → genera los `*_attachments.csv`.
2. **Binarios** (`GetAttachmentBinary` → `destination: gcp`) en las N máquinas con la misma partición → sube a GCP y genera los controles.

Cada máquina monitorea sus jobs con `GET /jobs`. Antes de empezar, validar con `GET /health` (Oracle) y `GET /health/gcp` (bucket).

## Consolidar el control al final

Cada máquina sube sus controles a `gs://<bucket>/<prefix>/_control/<host>/`. Para el gran total:

```powershell
gcloud storage ls "gs://dev-.../adjuntos/_control/**/*_resumen.csv"
# descargar y unir los *_resumen.csv (una fila por archivo): total de solicitudes y adjuntos cargados de toda la migracion
# y los *_errores.csv para lo que haya que reintentar
```

### Índice maestro de búsqueda (SR → ruta)

Cada archivo procesado sube además su índice a `gs://<bucket>/<prefix>/_index/<nombre>_index.csv` (una fila por adjunto confirmado: `Reference Number`, `FileName`, `StoredAs`, `Location`, `metadata_file`). Concatenando los índices de todos los archivos se obtiene el índice maestro de la migración: localizar los adjuntos de cualquier SR es un filtro por `Reference Number`, sin saber en qué archivo ni máquina se procesó.

```powershell
# Descargar todos los indices y unirlos en uno maestro:
gcloud storage cp "gs://dev-.../adjuntos/_index/*_index.csv" .\indices\
Import-Csv .\indices\*.csv | Export-Csv .\indice_maestro.csv -NoTypeInformation
```

## Recomendaciones de capacidad

- **Concurrencia total = suma de `max_workers` de todas las máquinas.** Vigilar que Oracle no devuelva 429/5xx (si pasa, bajar `max_workers`; el circuit breaker frena si el servicio cae).
- **Reintentos baratos**: si una máquina se corta, se relanza con su mismo `worker_index` y retoma desde el bucket (los ya subidos se omiten).
- **No compartir `output_folder`** por red entre máquinas (el lock del manifiesto no cruza máquinas).
- **No usar `overwrite`** al retomar (re-subiría todo).

> Si en el futuro se necesita un pool dinámico donde cualquier máquina jale de una cola compartida sin particionar a mano, se puede añadir un "claim" atómico por objeto en el bucket (lock con `if_generation_match=0`). La partición por worker cubre el caso de una migración planificada sin esa complejidad.
