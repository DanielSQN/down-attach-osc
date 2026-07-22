# Conexión a GCP con cuenta de servicio (destino `gcp` de GetAttachmentBinary)

Guía completa para configurar la subida de adjuntos a un bucket de Google Cloud Storage (GCS) usando una **cuenta de servicio**. Cubre qué es, cómo crearla, la estructura del JSON, dónde ubicarlo, cómo lo usa la app y cómo probar la conexión.

---

## 1. ¿Qué es una cuenta de servicio y por qué?

Una **cuenta de servicio** (service account) es una "identidad" de GCP para programas (no personas). En vez de que un usuario se loguee, la app se autentica con una **llave** (un archivo JSON) que representa a esa identidad. A esa cuenta se le dan **permisos** sobre el bucket, y la app puede subir objetos en su nombre.

El flujo es: `archivo JSON` → la app se autentica ante Google → obtiene un token → sube los binarios al bucket.

---

## 2. Crear la cuenta de servicio y la llave JSON

En la **consola de GCP** (https://console.cloud.google.com), con un proyecto ya seleccionado:

1. **IAM y administración → Cuentas de servicio → + Crear cuenta de servicio**.
2. Nombre, p. ej. `osc-adjuntos-writer`. Crear y continuar.
3. (El rol se puede asignar acá a nivel proyecto, pero **es mejor asignarlo a nivel del bucket** — ver punto 3.) Finalizar.
4. Entrar a la cuenta creada → pestaña **Claves (Keys) → Agregar clave → Crear clave nueva → JSON → Crear**.
5. Se descarga un archivo `.json`. **Ese es el archivo que usará la app.** Guardalo bien: no se puede volver a descargar (si se pierde, se crea otra llave).

> Alternativa por línea de comandos (gcloud):
> ```bash
> gcloud iam service-accounts create osc-adjuntos-writer --display-name "OSC adjuntos writer"
> gcloud iam service-accounts keys create sa.json \
>   --iam-account osc-adjuntos-writer@TU_PROYECTO.iam.gserviceaccount.com
> ```

---

## 3. Permisos: dar acceso al bucket

La cuenta necesita **crear objetos** (subir) y **listar objetos** (para la reanudación, que lista el prefijo y omite lo ya subido). El rol que cubre ambos es **`Storage Object Admin`** (`roles/storage.objectAdmin`). Si se quiere el mínimo, sirven **`Storage Object Creator`** + **`Storage Object Viewer`** juntos.

Lo recomendado es asignarlo **solo en el bucket** (menos privilegio que a nivel proyecto):

1. **Cloud Storage → Buckets →** tu bucket **→ pestaña Permisos (Permissions) → Otorgar acceso (Grant access)**.
2. En "Principales nuevas" pegar el **email de la cuenta de servicio** (tiene forma `osc-adjuntos-writer@TU_PROYECTO.iam.gserviceaccount.com`; también está en el campo `client_email` del JSON).
3. Rol: **Storage Object Admin**. Guardar.

---

## 4. Estructura del archivo JSON

El archivo descargado tiene esta forma (los valores reales son largos; aquí van como marcadores). **No hay que editarlo**: se usa tal cual se descargó.

```json
{
  "type": "service_account",
  "project_id": "tu-proyecto-123456",
  "private_key_id": "0a1b2c3d4e5f...",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkq...\n-----END PRIVATE KEY-----\n",
  "client_email": "osc-adjuntos-writer@tu-proyecto-123456.iam.gserviceaccount.com",
  "client_id": "1234567890123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/osc-adjuntos-writer%40tu-proyecto-123456.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
```

Los dos campos que importan conceptualmente: `client_email` (la identidad a la que diste permiso en el bucket) y `private_key` (el secreto con el que se firma la autenticación). El resto son endpoints estándar de Google.

---

## 5. Dónde ubicar el archivo y cómo referenciarlo

- **Ubicación**: cualquier carpeta de la máquina Windows donde corre el API, **fuera del repositorio** para no subirlo por error a git. Por ejemplo `C:\secrets\osc-gcp-sa.json`.
- **Referencia**: en el archivo `.env` del proyecto, con las barras invertidas escapadas:

```ini
GCP_SERVICE_ACCOUNT_FILE=C:\\secrets\\osc-gcp-sa.json
```

- Si se deja **vacío**, la app usa las *credenciales por defecto* de GCP (la variable de entorno estándar `GOOGLE_APPLICATION_CREDENTIALS` o `gcloud auth application-default login`). Para tu caso, lo simple es apuntar `GCP_SERVICE_ACCOUNT_FILE` al JSON.

> **Seguridad**: el JSON es un secreto (equivale a una contraseña del bucket). No lo subas a git (ya está cubierto por `.gitignore` si lo dejás fuera del repo), no lo compartas por chat/correo, y si se filtra, borrá esa llave en la consola (pestaña Claves) y generá otra. Conviene rotarlo periódicamente.

---

## 6. Cómo lo usa la app (flujo de conexión)

Cuando enviás `GetAttachmentBinary` con `destination: "gcp"`:

1. La app lee `GCP_SERVICE_ACCOUNT_FILE` del `.env`.
2. Crea el cliente autenticado: `storage.Client.from_service_account_json(<ruta>)`.
3. Toma el bucket indicado en `gcp_bucket`.
4. **Reanudación**: lista una vez los objetos bajo `gcp_prefix` (`list_blobs`) para saber qué ya está subido y omitirlo.
5. Por cada adjunto, transmite el binario **directo de Oracle a GCS** (`upload_from_file`), sin escribirlo en disco local. El objeto queda en `gs://<bucket>/<gcp_prefix>/<Reference Number>/<FileName>`.

Todo esto ocurre por **HTTPS saliente** hacia Google. La validación de credenciales/bucket se hace al recibir la petición: si algo está mal, responde `500` de inmediato (antes de encolar el job).

---

## 7. Red y firewall (importante en redes corporativas)

Las subidas salen a Internet hacia Google. La máquina necesita **HTTPS (443) saliente** hacia:

- `oauth2.googleapis.com` (obtener el token de autenticación)
- `storage.googleapis.com` (subir/listar objetos)

Si estás detrás de un **proxy corporativo**, configurá las variables de entorno antes de lanzar uvicorn (la librería de Google las respeta):

```powershell
$env:HTTPS_PROXY = "http://usuario:clave@proxy.tuempresa.com:8080"
```

Si el firewall bloquea Google, el job fallará con errores de conexión (que además reintenta y, si persisten, abren el circuit breaker). Coordiná con tu área de redes la salida a esos dos dominios.

---

## 8. Probar la conexión ANTES de un job grande

### Opción A (recomendada): endpoint `GET /health/gcp`

Con el API corriendo, desde el navegador/Swagger o PowerShell (reemplazá el bucket):

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/health/gcp?bucket=mi-bucket&prefix=adjuntos"
```

Prueba autenticación + listar y, por defecto, sube y borra un objeto de prueba (`write_test=false` para omitir la escritura). Respuesta:

| Campo | Descripción |
|---|---|
| `gcp_ok` | `true` si la cuenta puede listar y subir (lo que el job necesita). |
| `credentials` | `service_account_file` (usó el JSON) o `default` (ADC). |
| `checks` | `{ auth_list, write, delete }` — qué verificaciones pasaron (`delete` puede ser `false` sin invalidar: el job no borra). |
| `errors` | Detalle del error donde se detuvo (p. ej. `403 storage.objects.create denied` → falta rol en el bucket). |
| `elapsed_ms` | Duración. |

Ejemplo OK: `{ "gcp_ok": true, "checks": { "auth_list": true, "write": true, "delete": true }, "errors": {} }`.

### Opción B: script directo

Con el entorno virtual activado (reemplazá la ruta y el bucket):

```powershell
.\venv\Scripts\Activate.ps1
python -c "from google.cloud import storage; c = storage.Client.from_service_account_json(r'C:\secrets\osc-gcp-sa.json'); b = c.bucket('mi-bucket'); b.blob('prueba/conexion.txt').upload_from_string('ok'); print('Subida OK'); print('Listar:', [x.name for x in c.list_blobs('mi-bucket', prefix='prueba', max_results=5)])"
```

Si cualquiera de las dos falla, mirá el punto 9.

---

## 9. Errores comunes

| Síntoma | Causa probable |
|---|---|
| `500 ... Falta la dependencia google-cloud-storage` | No se instaló `requirements.txt` en el venv (`pip install -r requirements.txt`). |
| `500 ... No se pudo inicializar GCP: File ... not found` | La ruta de `GCP_SERVICE_ACCOUNT_FILE` está mal (revisar barras `\\` y que el archivo exista). |
| `403 ... does not have storage.objects.create access` | La cuenta de servicio no tiene el rol en el bucket (punto 3). |
| `403 ... storage.objects.list` al reanudar | Falta el permiso de listar (usar `Storage Object Admin` o agregar `Storage Object Viewer`). |
| `404 ... bucket does not exist` | El nombre en `gcp_bucket` está mal o el bucket está en otro proyecto. |
| Errores de conexión / timeouts constantes | Firewall/proxy bloqueando `storage.googleapis.com` (punto 7). |

---

## 10. Resumen para tu setup

1. Crear la cuenta de servicio y descargar el JSON (puntos 2).
2. Darle **Storage Object Admin** sobre el bucket (punto 3).
3. Copiar el JSON a `C:\secrets\osc-gcp-sa.json` (fuera del repo) y ponerlo en el `.env`:
   `GCP_SERVICE_ACCOUNT_FILE=C:\\secrets\\osc-gcp-sa.json`.
4. Probar la conexión (punto 8).
5. Lanzar `GetAttachmentBinary` con `destination: "gcp"`, `gcp_bucket` y `gcp_prefix`.
