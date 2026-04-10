# Guía de Despliegue en Railway.app

**Autor:** Manus AI
**Fecha:** 09 de Abril de 2026

Esta guía detalla los pasos exactos para desplegar el bot "El Buen Samaritano" en [Railway.app](https://railway.app/), asegurando la persistencia de la base de datos SQLite y la correcta configuración del entorno.

## 1. Preparación del Repositorio

Asegúrate de que tu repositorio en GitHub contenga los siguientes archivos en la raíz:
- `Samaritan_fixed.py` (El script principal del bot)
- `requirements.txt` (Dependencias del proyecto)
- `Procfile` (Instrucciones de ejecución para Railway)
- `.gitignore` (Para evitar subir archivos locales innecesarios)

## 2. Creación del Proyecto en Railway

1. Inicia sesión en tu cuenta de [Railway.app](https://railway.app/).
2. Haz clic en el botón **"New Project"** (Nuevo Proyecto).
3. Selecciona **"Deploy from GitHub repo"** (Desplegar desde repositorio de GitHub).
4. Busca y selecciona el repositorio que contiene el código del bot.
5. Haz clic en **"Deploy Now"** (Desplegar Ahora).

*Nota: El primer despliegue probablemente fallará porque aún no hemos configurado las variables de entorno ni el volumen persistente. Esto es normal.*

## 3. Configuración de Variables de Entorno

Como mencionaste que ya tienes las variables configuradas, asegúrate de que los nombres coincidan exactamente con los que espera el script:

1. Ve a tu proyecto en Railway y selecciona el servicio del bot.
2. Navega a la pestaña **"Variables"**.
3. Verifica que existan las siguientes variables:
   - `TELEGRAM_BOT_TOKEN`: El token proporcionado por BotFather.
   - `GROUP_CHAT_ID`: El ID numérico del grupo (ej. `-1001234567890`).
   - `ADMIN_IDS`: Una lista de IDs de administradores separados por comas (ej. `123456789,987654321`).
   - `TZ`: La zona horaria para las tareas programadas (ej. `America/Mexico_City` o `Europe/Madrid`).

## 4. Configuración de Persistencia (Volumen)

**¡CRÍTICO!** Railway utiliza contenedores efímeros. Si no configuras un volumen, la base de datos SQLite (`buen_samaritano.db`) se borrará cada vez que el bot se reinicie o se actualice.

1. En el panel de tu servicio en Railway, ve a la pestaña **"Settings"** (Configuración).
2. Desplázate hacia abajo hasta la sección **"Volumes"** (Volúmenes).
3. Haz clic en **"Add Volume"** (Añadir Volumen).
4. En el campo **"Mount Path"** (Ruta de Montaje), escribe exactamente: `/app/data`
5. Haz clic en **"Create"** (Crear).

## 5. Conectar el Volumen con el Script

Ahora debemos decirle al script de Python que guarde la base de datos dentro de ese volumen que acabamos de crear.

1. Vuelve a la pestaña **"Variables"**.
2. Añade una nueva variable llamada `PERSISTENT_STORAGE_PATH`.
3. Establece su valor como `/app/data`.
4. Haz clic en **"Add"** (Añadir).

*El script `Samaritan_fixed.py` está programado para buscar esta variable y guardar `buen_samaritano.db` en esa ruta.*

## 6. Verificación Final

1. Al añadir la última variable, Railway debería iniciar un nuevo despliegue automáticamente.
2. Ve a la pestaña **"Deployments"** (Despliegues) y espera a que el proceso termine.
3. Una vez que esté en verde ("Success"), ve a la pestaña **"View Logs"** (Ver Registros).
4. Deberías ver mensajes similares a estos:
   ```
   Base de datos inicializada o verificada en: /app/data/buen_samaritano.db
   El Buen Samaritano (v32) está listo y en funcionamiento.
   ```

## 7. Mantenimiento y Actualizaciones

- **Actualizar el código:** Simplemente haz un `git push` a la rama principal (main/master) de tu repositorio en GitHub. Railway detectará el cambio y redesplegará el bot automáticamente.
- **Reiniciar el bot:** Si necesitas reiniciar el bot manualmente, puedes hacerlo desde la pestaña "Deployments" haciendo clic en los tres puntos del último despliegue exitoso y seleccionando "Restart".
- **Respaldo de la Base de Datos:** Actualmente, Railway no ofrece una forma directa de descargar archivos de un volumen a través de la interfaz web. Si necesitas respaldar la base de datos, considera implementar un comando en el bot que envíe el archivo `.db` al chat privado del administrador.
