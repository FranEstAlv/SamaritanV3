# Arquitectura y API de Comandos: El Buen Samaritano

**Autor:** Manus AI
**Fecha:** 09 de Abril de 2026

Este documento describe la arquitectura interna del bot "El Buen Samaritano" (`Samaritan_fixed.py`), el flujo de datos, el esquema de la base de datos y la API de comandos disponibles para los administradores.

## 1. Arquitectura del Sistema

El bot está construido sobre la librería `python-telegram-bot` (v20+), utilizando su arquitectura asíncrona basada en `Application.builder()`. 

### 1.1. Componentes Principales

- **Manejador de Eventos (Event Loop):** Utiliza `asyncio` para procesar múltiples actualizaciones de Telegram de forma concurrente sin bloquear el hilo principal.
- **JobQueue:** Gestiona tareas programadas en segundo plano:
  - `check_expirations_and_notify`: Se ejecuta diariamente a las 12:00 para avisar a los usuarios cuyos planes están por expirar.
  - `send_random_daily_message_job`: Envía un mensaje motivacional aleatorio al grupo una vez al día.
  - `auto_expel_expired_users`: Se ejecuta cada hora para expulsar automáticamente a los usuarios cuyos planes han vencido.
- **Capa de Persistencia:** Utiliza SQLite (`sqlite3`) para almacenar el estado de los usuarios, registros de expulsión y errores del sistema.
- **Gestor de Conversaciones (`ConversationHandler`):** Maneja flujos de múltiples pasos, como el envío de mensajes oficiales (`/mensaje`) o la limpieza masiva de inactivos (`/limpieza`).

### 1.2. Flujo de Resolución de Usuarios (`resolve_user_target`)

Una de las características más robustas del bot es su capacidad para identificar al usuario objetivo de un comando de tres maneras diferentes. El flujo es el siguiente:

1. **Verificación de Reply:** Si el comando es una respuesta a un mensaje, extrae el ID del autor de ese mensaje.
2. **Verificación de Argumentos:** Si no es un reply, busca argumentos en el comando.
3. **Resolución de Username:** Si el argumento empieza con `@`, busca el username en la base de datos SQLite y extrae su ID numérico.
4. **Resolución de ID Directo:** Si el argumento es un número, lo asume como el ID del usuario.
5. **Fallback:** Si ninguna de las anteriores funciona, retorna `None` y el comando solicita el formato correcto.

## 2. Esquema de la Base de Datos (SQLite)

La base de datos (`buen_samaritano.db`) consta de cuatro tablas principales:

### 2.1. Tabla `users`
Almacena el estado y los planes de los miembros del grupo.
- `tg_id` (INTEGER PRIMARY KEY): ID único de Telegram del usuario.
- `username` (TEXT): Nombre de usuario (sin el @).
- `start_date` (TEXT): Fecha ISO de inicio del plan actual.
- `end_date` (TEXT): Fecha ISO de vencimiento del plan actual.
- `active` (INTEGER): Estado del plan (1 = Activo, 0 = Inactivo).
- `activated_by_admin_id` (INTEGER): ID del administrador que activó el plan.
- `initial_days` (INTEGER): Días iniciales otorgados en el plan.
- `last_notification_date` (TEXT): Fecha de la última notificación de expiración enviada.

### 2.2. Tabla `expulsion_log`
Registro de auditoría de las acciones de expulsión y readmisión.
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `user_id` (INTEGER): ID del usuario afectado.
- `admin_id` (INTEGER): ID del administrador que ejecutó la acción.
- `action` (TEXT): Tipo de acción (`expel`, `accept`, `limpieza_masiva`).
- `timestamp` (DATETIME): Fecha y hora de la acción.

### 2.3. Tabla `bot_events`
Rastrea la ejecución de tareas programadas para evitar duplicidades.
- `event_name` (TEXT PRIMARY KEY): Nombre del evento (ej. `daily_message`).
- `last_run` (TEXT): Fecha ISO de la última ejecución exitosa.

### 2.4. Tabla `runtime_errors`
Almacena excepciones no controladas para depuración mediante el comando `/scan`.
- `id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `timestamp` (TEXT): Fecha ISO del error.
- `error_message` (TEXT): Mensaje corto del error.
- `traceback` (TEXT): Traza completa de la excepción.

## 3. API de Comandos (Referencia para Administradores)

Todos los comandos de gestión de usuarios (`/plan`, `/extender`, `/menos`) soportan el flujo de resolución de usuarios descrito en la sección 1.2.

### 3.1. Gestión Individual
- **`/plan <días>`** (Respondiendo a un mensaje) o **`/plan <ID/@username> <días>`**
  - *Descripción:* Activa un nuevo plan para el usuario, sobrescribiendo cualquier plan anterior.
  - *Ejemplo:* `/plan @juanperez 30`
- **`/extender <días>`** (Respondiendo a un mensaje) o **`/extender <ID/@username> <días>`**
  - *Descripción:* Suma días a la fecha de vencimiento actual del usuario. Falla si el usuario no tiene un plan activo.
  - *Ejemplo:* `/extender 123456789 15`
- **`/menos <días>`** (Respondiendo a un mensaje) o **`/menos <ID/@username> <días>`**
  - *Descripción:* Resta días a la fecha de vencimiento actual. Falla si el resultado es una fecha en el pasado.
  - *Ejemplo:* `/menos @juanperez 5`

### 3.2. Gestión Masiva
- **`/todosmas <días>`**
  - *Descripción:* Inicia un flujo de confirmación para añadir días a *todos* los usuarios con planes activos.
  - *Requiere:* Confirmación con `/aceptar_todos` en menos de 60 segundos.
- **`/todosmenos <días>`**
  - *Descripción:* Inicia un flujo de confirmación para restar días a *todos* los usuarios con planes activos.
  - *Requiere:* Confirmación con `/aceptar_todos` en menos de 60 segundos.

### 3.3. Moderación y Limpieza
- **`/expulsar <ID>`**
  - *Descripción:* Expulsa inmediatamente a un usuario del grupo y marca su plan como inactivo (`active = 0`).
- **`/aceptar <ID>`**
  - *Descripción:* Levanta el ban de un usuario previamente expulsado, permitiéndole volver a unirse mediante un enlace de invitación.
- **`/limpieza`**
  - *Descripción:* Escanea el grupo en busca de usuarios que están físicamente presentes pero cuyo plan en la base de datos está inactivo (`active = 0`).
  - *Requiere:* Confirmación con `/limpiezatotal` en menos de 60 segundos para expulsarlos a todos.

### 3.4. Observabilidad y Utilidades
- **`/scan`**
  - *Descripción:* Genera un reporte del sistema (Uptime, próxima expulsión, estado del mensaje diario, estadísticas de usuarios, última expulsión y último error registrado).
- **`/estado`**
  - *Descripción:* Muestra una lista paginada (inline keyboard) de todos los usuarios registrados y sus fechas de vencimiento.
- **`/mensaje`**
  - *Descripción:* Inicia un flujo conversacional en el chat privado del bot para redactar y enviar un mensaje oficial (con formato HTML) al grupo.
- **`/getchatid`**
  - *Descripción:* Retorna el ID numérico del chat actual (útil para configurar `GROUP_CHAT_ID`).
