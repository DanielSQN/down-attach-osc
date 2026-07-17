# down-attach-osc

API para descargar metadatos y binarios de adjuntos de solicitudes de servicio (Service Requests) de Oracle Service Cloud / Fusion CRM, usando el REST API `crmRestApi` con autenticación **Basic**.

Expone dos métodos:

| Método | Descripción |
|---|---|
| `POST /GetMetadataAttachments` | Lee una carpeta con archivos `ServiceRequest_X_Y_00Z.csv` (columnas `"Service Request ID","Reference Number"`), consulta los adjuntos de cada `Reference Number` (con paginación por `offset` mientras `hasMore` sea `true`) y genera **un CSV de metadatos por cada archivo de entrada**, con todas las columnas de metadatos más el `href` del enclosure `FileContents`. |
| `POST /GetAttachmentBinary` | Lee un CSV de metadatos generado por el método anterior y, por cada fila, descarga el binario desde el `href` de `FileContents`, guardándolo con el nombre del `FileName` (en una subcarpeta por SR para evitar colisiones). |

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

Con el servidor levantado, la documentación interactiva (Swagger) queda en <http://127.0.0.1:8000/docs> — desde ahí se pueden probar los dos métodos.

## Uso

### 1. GetMetadataAttachments

Lee todos los `.csv` de la carpeta de entrada y genera en la carpeta de salida un archivo `<nombre>_attachments.csv` por cada uno.

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/GetMetadataAttachments `
  -ContentType "application/json" `
  -Body '{
    "input_folder":  "C:\\Users\\daniel\\Downloads\\ServiceRequest20260710_2038\\ServiceRequest",
    "output_folder": "C:\\Users\\daniel\\Downloads\\metadatos"
  }'
```

Columnas del CSV generado: `Service Request ID`, `Reference Number`, `AttachedDocumentId`, `DatatypeCode`, `FileName`, `DmDocumentId`, `UploadedFileContentType`, `UploadedFileLength`, `Title`, `CreationDate`, `CreatedBy`, `FileContentsHref`.

### 2. GetAttachmentBinary

Lee un CSV de metadatos y descarga los binarios en `output_folder\<Reference Number>\<FileName>`. Los archivos ya descargados se omiten (reanudable); usar `"overwrite": true` para volver a bajarlos.

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/GetAttachmentBinary `
  -ContentType "application/json" `
  -Body '{
    "metadata_csv":  "C:\\Users\\daniel\\Downloads\\metadatos\\ServiceRequest_1_1_001_attachments.csv",
    "output_folder": "C:\\Users\\daniel\\Downloads\\adjuntos"
  }'
```

Ambas respuestas devuelven un resumen JSON con totales y la lista de errores (los SR o adjuntos que fallen no detienen el proceso).

> Nota: con carpetas grandes el procesamiento puede tardar varios minutos; el avance se ve en la consola donde corre `uvicorn`.
