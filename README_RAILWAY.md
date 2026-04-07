# Deploy a Railway (GitHub + volumen persistente)

## Qué quedó preparado
- `gunicorn` agregado a `requirements.txt`
- `railway.json` con start command y healthcheck
- soporte de almacenamiento persistente vía `RAILWAY_VOLUME_MOUNT_PATH` o `CRM_DATA_DIR`
- copiado inicial opcional de `crm.db` y `.secret_key` desde el repo al volumen (`CRM_SEED_FROM_REPO=1`)

## Recomendación de persistencia
Este proyecto usa SQLite, así que la vía más simple es montar un **Volume** en Railway y dejar la DB dentro de ese volumen.

## Variables sugeridas
- `CRM_SEED_FROM_REPO=1` solo para el primer deploy si querés clonar al volumen el `crm.db` actual del repo.
- `CHEQ_MONITOR_START_HOUR=8` (o el horario que quieras)
- `CRM_SLACK_WEBHOOK_URL=...` si usás alertas a Slack

## Flujo
1. Subí este repo a GitHub.
2. En Railway: `New Project` → `Deploy from GitHub repo`.
3. Una vez creado el servicio, agregale un **Volume** montado, por ejemplo, en `/data`.
4. Railway inyecta `RAILWAY_VOLUME_MOUNT_PATH` automáticamente; la app lo detecta sola.
5. Generá dominio público y verificá `/api/health`.

## Nota importante sobre datos reales
Si `crm.db` contiene datos sensibles y el repo va a ser remoto, no conviene subir esa DB real a GitHub. En ese caso:
- quitá `crm.db` del repo antes del push, o
- dejá `CRM_SEED_FROM_REPO=0` y cargá la DB por otro medio.
