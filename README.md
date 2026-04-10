# El Buen Samaritano - Bot de Gestión de Miembros para Telegram

![Python Version](https://img.shields.io/badge/python-3.11%2B-blue)
![Telegram Bot API](https://img.shields.io/badge/python--telegram--bot-20.8-green)
![License](https://img.shields.io/badge/license-MIT-blue)

**El Buen Samaritano** es un bot de Telegram diseñado para la gestión automatizada de miembros en grupos privados. Permite a los administradores controlar el acceso mediante un sistema de suscripciones basado en días, expulsando automáticamente a los usuarios cuyos planes han expirado.

## 🌟 Características Principales

- **Gestión de Suscripciones:** Añade, extiende o reduce días de acceso a los miembros del grupo.
- **Flexibilidad de Comandos:** Ejecuta acciones respondiendo a mensajes, mencionando `@usernames` o usando IDs numéricos.
- **Expulsión Automática:** Tarea en segundo plano que verifica cada hora y expulsa a los usuarios con planes vencidos.
- **Notificaciones Preventivas:** Avisa a los usuarios antes de que su plan expire.
- **Gestión Masiva:** Añade o resta días a todos los usuarios activos simultáneamente.
- **Limpieza de Inactivos:** Detecta y expulsa a usuarios que permanecen en el grupo pero no tienen un plan activo en la base de datos.
- **Observabilidad:** Comando `/scan` para ver el estado general del sistema, próximos vencimientos y últimos errores.

## 🚀 Instalación y Configuración Local

### Requisitos Previos
- Python 3.11 o superior
- Un token de bot de Telegram (obtenido a través de [@BotFather](https://t.me/BotFather))
- El ID del grupo donde operará el bot
- Tu ID de usuario de Telegram (para permisos de administrador)

### Pasos de Instalación

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/tu-usuario/el-buen-samaritano.git
   cd el-buen-samaritano
   ```

2. **Crear un entorno virtual (recomendado):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variables de entorno:**
   Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:
   ```env
   TELEGRAM_BOT_TOKEN=tu_token_aqui
   GROUP_CHAT_ID=-1001234567890
   ADMIN_IDS=123456789,987654321
   TZ=America/Mexico_City
   PERSISTENT_STORAGE_PATH=./data
   ```

5. **Ejecutar el bot:**
   ```bash
   python Samaritan_fixed.py
   ```

## 🛠️ Comandos de Administrador

Todos los comandos de gestión de usuarios soportan tres formatos:
- Respondiendo a un mensaje del usuario: `/plan 30`
- Mencionando al usuario: `/plan @username 30`
- Usando el ID numérico: `/plan 123456789 30`

| Comando | Descripción |
|---------|-------------|
| `/plan <usuario> <días>` | Activa un nuevo plan para el usuario. |
| `/extender <usuario> <días>` | Añade días a un plan existente. |
| `/menos <usuario> <días>` | Resta días a un plan existente. |
| `/expulsar <usuario>` | Expulsa manualmente a un usuario del grupo. |
| `/aceptar <usuario>` | Permite que un usuario previamente expulsado vuelva a unirse. |
| `/todosmas <días>` | Añade días a todos los usuarios activos. |
| `/todosmenos <días>` | Resta días a todos los usuarios activos. |
| `/limpieza` | Inicia el proceso para expulsar usuarios inactivos que siguen en el grupo. |
| `/scan` | Muestra un informe detallado del estado del sistema. |
| `/estado` | Muestra la lista paginada de todos los usuarios y sus fechas de vencimiento. |
| `/mensaje` | Inicia un flujo para enviar un mensaje oficial al grupo como el bot. |

## ☁️ Despliegue en Railway.app

Este proyecto está optimizado para ser desplegado en [Railway.app](https://railway.app/).

1. Crea un nuevo proyecto en Railway y selecciona "Deploy from GitHub repo".
2. Selecciona tu repositorio.
3. Ve a la pestaña **Variables** y añade las siguientes:
   - `TELEGRAM_BOT_TOKEN`
   - `GROUP_CHAT_ID`
   - `ADMIN_IDS`
   - `TZ` (ej. `America/Mexico_City`)
4. Ve a la pestaña **Settings** > **Volumes** y añade un volumen montado en `/app/data`.
5. En la pestaña **Variables**, añade `PERSISTENT_STORAGE_PATH=/app/data` para asegurar que la base de datos SQLite no se pierda entre reinicios.
6. Railway detectará automáticamente el archivo `Procfile` y ejecutará el bot como un proceso `worker`.

## 🛡️ Seguridad y Arquitectura

- **Seguridad por Diseño:** Las credenciales nunca se exponen en el código, utilizando variables de entorno.
- **Validación de Inputs:** Todos los comandos que reciben parámetros numéricos están protegidos contra inyecciones y errores de tipo.
- **Persistencia:** Utiliza SQLite con manejo seguro de conexiones mediante context managers (`with sqlite3.connect(...)`).
- **Resiliencia:** Manejador de errores global que captura excepciones no controladas, las registra en la base de datos y evita que el bot se detenga.

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.
