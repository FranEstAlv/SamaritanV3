# el_buen_samaritano_v31.py

import logging
import sqlite3
import os
import random
import asyncio
import traceback
import html
from datetime import datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember, ChatMemberUpdated
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    JobQueue,
    CallbackQueryHandler,
    ChatMemberHandler,
)
from telegram.constants import ParseMode, ChatType
from telegram.error import TelegramError, BadRequest

# Logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Credenciales
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0"))
ADMIN_GROUP_CHAT_ID = int(os.getenv("ADMIN_GROUP_CHAT_ID", "0"))  # Chat ID del grupo de administradores
REFERENCES_CHANNEL_ID = int(os.getenv("REFERENCES_CHANNEL_ID", "0"))  # Canal para publicar referencias
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = {int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(",") if admin_id.strip()}
PERSISTENT_STORAGE_PATH = os.getenv("PERSISTENT_STORAGE_PATH", ".").strip()
BOT_START_TIME = datetime.now(timezone.utc)

# Crear directorio de almacenamiento si no existe
os.makedirs(PERSISTENT_STORAGE_PATH, exist_ok=True)

try:
    BOT_TIMEZONE = ZoneInfo(os.getenv("TZ", "UTC"))
except Exception:
    BOT_TIMEZONE = ZoneInfo("UTC")
    logger.warning("Variable de entorno TZ no encontrada o inválida. Usando UTC.")

DATA_DIR = PERSISTENT_STORAGE_PATH
DB_PATH = os.path.join(DATA_DIR, "buen_samaritano.db")
ESTADO_PAGE_SIZE = 3

MOTIVATIONAL_MESSAGES = [
    "¡Suerte y mucho éxito en sus compras! Recuerden que son los mejores, que son OLIMPO todos."
]

# Mssj
ENVIAR_MENSAJE, CONFIRM_PURGE, CONFIRM_TODOS, ENVIAR_MENSAJE_ADMIN, ENVIAR_IMAGEN_ADMIN, BAN_USER_ID, BAN_REASON, BAN_IMAGE = range(8)

# DB
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    tg_id INTEGER PRIMARY KEY, username TEXT, start_date TEXT,
                    end_date TEXT, active INTEGER DEFAULT 0,
                    activated_by_admin_id INTEGER, initial_days INTEGER DEFAULT 0,
                    last_notification_date TEXT, references_count INTEGER DEFAULT 0
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS expulsion_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
                    admin_id INTEGER, action TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS bot_events (
                    event_name TEXT PRIMARY KEY, last_run TEXT
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS runtime_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    error_message TEXT,
                    traceback TEXT
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS pending_new_members (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    join_time TEXT,
                    approved INTEGER DEFAULT 0
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS blacklist (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    reason TEXT,
                    image_file_id TEXT,
                    banned_by_admin_id INTEGER,
                    ban_date TEXT,
                    ban_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS user_references (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    image_file_id TEXT,
                    reference_date TEXT,
                    posted_to_channel INTEGER DEFAULT 0,
                    channel_message_id INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(tg_id)
                )
            """)
            conn.commit()
            logger.info(f"Base de datos inicializada o verificada en: {DB_PATH}")
    except sqlite3.Error as e:
        logger.critical(f"Error crítico al inicializar la base de datos: {e}")
        raise

# Comandos
def make_aware(dt_str: str | None) -> datetime | None:
    if not dt_str: return None
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
    return dt

async def check_admin_permissions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if user_id in ADMIN_IDS: return True
    await update.message.reply_text("❌ Este comando solo puede ser usado por administradores autorizados.")

async def is_admin_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.callback_query.from_user.id if update.callback_query else update.effective_user.id
    return user_id in ADMIN_IDS

def register_user(tg_id: int, username: str):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id FROM users WHERE tg_id = ?", (tg_id,))
        if c.fetchone() is None:
            c.execute("INSERT INTO users (tg_id, username, active) VALUES (?, ?, 0)", (tg_id, username))
            logger.info(f"Nuevo usuario registrado: {username} ({tg_id})")
        else:
            c.execute("UPDATE users SET username = ? WHERE tg_id = ?", (username, tg_id))
            logger.info(f"Username actualizado para usuario existente: {username} ({tg_id})")
        conn.commit()

async def get_or_register_user(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id FROM users WHERE tg_id = ?", (user_id,))
        if c.fetchone() is not None: return True
    try:
        chat_member = await context.bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
        user = chat_member.user
        register_user(user.id, user.username or f"user_{user.id}")
        logger.info(f"Usuario {user_id} no estaba en la DB. Registrado sobre la marcha.")
        return True
    except TelegramError:
        logger.warning(f"Se intentó registrar al usuario {user_id} sobre la marcha, pero no se encontró en el grupo.")
        return False
        
def format_timedelta(td: timedelta) -> str:
    days, remainder = divmod(td.total_seconds(), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days > 0: parts.append(f"{int(days)}d")
    if hours > 0: parts.append(f"{int(hours)}h")
    if minutes > 0: parts.append(f"{int(minutes)}m")
    return ", ".join(parts) if parts else "menos de un minuto"

# Admi
async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    report_parts = ["- - - 🩺 <b>Informe de Sistema</b> 🩺 - - -"]
    now = datetime.now(timezone.utc)
    
    uptime = format_timedelta(now - BOT_START_TIME)
    report_parts.append(f"<b>En línea desde hace:</b> {uptime}")
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT username, end_date FROM users WHERE active = 1 AND end_date > ? ORDER BY end_date ASC LIMIT 1", (now.isoformat(),))
        next_to_expire = c.fetchone()
        if next_to_expire and next_to_expire[1]:
            username, end_date_str = next_to_expire
            time_left = make_aware(end_date_str) - now
            report_parts.append(f"🔴 <b>Próxima Expulsión:</b> @{username or 'N/A'} en {format_timedelta(time_left)}")
        else:
            report_parts.append("🔴 <b>Próxima Expulsión:</b> Ninguna programada.")

        c.execute("SELECT last_run FROM bot_events WHERE event_name = 'daily_message'")
        last_run_row = c.fetchone()
        if last_run_row and last_run_row[0]:
            last_run_dt = make_aware(last_run_row[0])
            if last_run_dt and last_run_dt.astimezone(BOT_TIMEZONE).date() == datetime.now(BOT_TIMEZONE).date():
                report_parts.append(f"✉️ <b>Mensaje Diario:</b> Ya enviado hoy a las {last_run_dt.astimezone(BOT_TIMEZONE).strftime('%H:%M:%S')}")
            else:
                report_parts.append("✉️ <b>Mensaje Diario:</b> Pendiente para hoy.")
        else:
            report_parts.append("✉️ <b>Mensaje Diario:</b> Pendiente para hoy.")
            
        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE active = 1")
        active_users = c.fetchone()[0]
        report_parts.append(f"👥 <b>Miembros:</b> {active_users} activos de {total_users} registrados.")

        c.execute("SELECT username, end_date FROM users WHERE active = 1 ORDER BY end_date DESC LIMIT 1")
        top_user = c.fetchone()
        if top_user and top_user[1]:
            username, end_date_str = top_user
            time_left = make_aware(end_date_str) - now
            report_parts.append(f"🏆 <b>Usuario Top:</b> @{username or 'N/A'} con {time_left.days} días restantes.")
        else:
            report_parts.append("🏆 <b>Usuario Top:</b> No hay usuarios activos.")
            
        c.execute("SELECT user_id, timestamp FROM expulsion_log ORDER BY timestamp DESC LIMIT 1")
        last_expelled_log = c.fetchone()
        if last_expelled_log and last_expelled_log[1]:
            user_id, timestamp_str = last_expelled_log
            c.execute("SELECT username FROM users WHERE tg_id = ?", (user_id,))
            user_data = c.fetchone()
            username = user_data[0] if user_data else f"ID {user_id}"
            expel_time = make_aware(timestamp_str)
            report_parts.append(f"👢 <b>Última Expulsión:</b> @{username} hace {format_timedelta(now - expel_time)}")
        else:
            report_parts.append("👢 <b>Última Expulsión:</b> Ninguna registrada.")

        c.execute("SELECT timestamp, error_message FROM runtime_errors ORDER BY id DESC LIMIT 1")
        last_error = c.fetchone()
        if last_error and last_error[0]:
            timestamp_str, error_message = last_error
            error_time = make_aware(timestamp_str)
            short_error = (error_message[:75] + '...') if len(error_message) > 75 else error_message
            report_parts.append(f"🔥 <b>Último Error:</b> Hace {format_timedelta(now - error_time)} - <code>{html.escape(short_error)}</code>")
        else:
            report_parts.append("🔥 <b>Último Error:</b> Ninguno registrado.")

    keyboard = [[InlineKeyboardButton("Cerrar Informe 🗑️", callback_data="estado_close")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("\n".join(report_parts), parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def resolve_user_target(update: Update, context: ContextTypes.DEFAULT_TYPE, arg_index: int = 0) -> int | None:
    """Resuelve el ID del usuario a partir de un reply, una mención o un ID directo."""
    # 1. Si es un reply a un mensaje
    if update.message.reply_to_message:
        return update.message.reply_to_message.from_user.id
    
    # 2. Si se proporcionan argumentos
    if context.args and len(context.args) > arg_index:
        target = context.args[arg_index]
        
        # Si es una mención (@username)
        if target.startswith('@'):
            username = target[1:]
            with sqlite3.connect(DB_PATH) as conn:
                c = conn.cursor()
                c.execute("SELECT tg_id FROM users WHERE username = ?", (username,))
                result = c.fetchone()
                if result:
                    return result[0]
            return None # Username no encontrado en DB
            
        # Si es un ID numérico
        try:
            return int(target)
        except ValueError:
            return None
            
    return None

async def plan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    
    user_id = await resolve_user_target(update, context, 0)
    
    # Determinar dónde están los días dependiendo de si se usó reply o no
    days_arg_index = 0 if update.message.reply_to_message else 1
    
    if not user_id or len(context.args) <= days_arg_index:
        await update.message.reply_text("Uso: Responde a un usuario con <code>/plan &lt;días&gt;</code> o usa <code>/plan &lt;ID/@username&gt; &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return
        
    try:
        days = int(context.args[days_arg_index])
        if not (1 <= days <= 999): raise ValueError("Los días deben estar entre 1 y 999.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return

    user_exists = await get_or_register_user(user_id, context)
    if not user_exists:
        await update.message.reply_text(f"❌ Usuario con ID {user_id} no encontrado ni en la base de datos ni en el grupo.")
        return
        
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        start_date = datetime.now(timezone.utc)
        end_date = start_date + timedelta(days=days)
        admin_id = update.effective_user.id
        
        c.execute("UPDATE users SET start_date = ?, end_date = ?, active = 1, activated_by_admin_id = ?, initial_days = ?, last_notification_date = NULL WHERE tg_id = ?",
                  (start_date.isoformat(), end_date.isoformat(), admin_id, days, user_id))
        conn.commit()
    await update.message.reply_text(f"✅ Plan de {days} días activado para el usuario {user_id}.")

async def extender_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    
    user_id = await resolve_user_target(update, context, 0)
    days_arg_index = 0 if update.message.reply_to_message else 1
    
    if not user_id or len(context.args) <= days_arg_index:
        await update.message.reply_text("Uso: Responde a un usuario con <code>/extender &lt;días&gt;</code> o usa <code>/extender &lt;ID/@username&gt; &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return
        
    try:
        days_to_add = int(context.args[days_arg_index])
        if not (1 <= days_to_add <= 999): raise ValueError("Los días deben estar entre 1 y 999.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return
        
    user_exists = await get_or_register_user(user_id, context)
    if not user_exists:
        await update.message.reply_text(f"❌ Usuario con ID {user_id} no encontrado ni en la base de datos ni en el grupo.")
        return
        
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT end_date, active FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()
        if not result or not result[1] or not result[0]:
            await update.message.reply_text(f"❌ El usuario {user_id} no tiene un plan activo para extender.")
            return
        current_end_date = make_aware(result[0])
        base_date = max(current_end_date, datetime.now(timezone.utc))
        new_end_date = base_date + timedelta(days=days_to_add)
        c.execute("UPDATE users SET end_date = ?, last_notification_date = NULL WHERE tg_id = ?", (new_end_date.isoformat(), user_id))
        conn.commit()
    await update.message.reply_text(f"✅ Suscripción extendida por {days_to_add} días. Nueva fecha de vencimiento: {new_end_date.strftime('%Y-%m-%d')}")

async def menos_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    
    user_id = await resolve_user_target(update, context, 0)
    days_arg_index = 0 if update.message.reply_to_message else 1
    
    if not user_id or len(context.args) <= days_arg_index:
        await update.message.reply_text("Uso: Responde a un usuario con <code>/menos &lt;días&gt;</code> o usa <code>/menos &lt;ID/@username&gt; &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return
        
    try:
        days_to_subtract = int(context.args[days_arg_index])
        if days_to_subtract <= 0: raise ValueError("El número de días a restar debe ser positivo.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return
        
    user_exists = await get_or_register_user(user_id, context)
    if not user_exists:
        await update.message.reply_text(f"❌ Usuario con ID {user_id} no encontrado ni en la base de datos ni en el grupo.")
        return
        
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT end_date, active FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()
        if not result or not result[1] or not result[0]:
            await update.message.reply_text(f"❌ El usuario {user_id} no tiene un plan activo para modificar.")
            return
        current_end_date = make_aware(result[0])
        new_end_date = current_end_date - timedelta(days=days_to_subtract)
        if new_end_date < datetime.now(timezone.utc):
            await update.message.reply_text(f"❌ La operación resultaría en una fecha de expiración pasada. No se aplicaron cambios.")
            return
        c.execute("UPDATE users SET end_date = ?, last_notification_date = NULL WHERE tg_id = ?", (new_end_date.isoformat(), user_id))
        conn.commit()
    await update.message.reply_text(f"✅ Se restaron {days_to_subtract} días al plan del usuario {user_id}.\nNueva fecha de vencimiento: {new_end_date.strftime('%Y-%m-%d')}")

async def expulsar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    if GROUP_CHAT_ID == 0:
        await update.message.reply_text("❌ El <code>GROUP_CHAT_ID</code> no está configurado.", parse_mode=ParseMode.HTML)
        return
    if len(context.args) != 1:
        await update.message.reply_text("Uso: <code>/expulsar &lt;ID_del_usuario&gt;</code>", parse_mode=ParseMode.HTML)
        return
    try: user_id_to_expel = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID de usuario inválido.")
        return
    try:
        await context.bot.ban_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id_to_expel)
        admin_id = update.effective_user.id
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET active = 0 WHERE tg_id = ?", (user_id_to_expel,))
            c.execute("INSERT INTO expulsion_log (user_id, admin_id, action) VALUES (?, ?, 'expel')", (user_id_to_expel, admin_id))
            conn.commit()
        await update.message.reply_text(f"✅ Usuario {user_id_to_expel} ha sido expulsado manualmente.")
    except TelegramError as e: await update.message.reply_text(f"❌ Error al expulsar: {e}")

async def aceptar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    if GROUP_CHAT_ID == 0:
        await update.message.reply_text("❌ El <code>GROUP_CHAT_ID</code> no está configurado.", parse_mode=ParseMode.HTML)
        return
    if len(context.args) != 1:
        await update.message.reply_text("Uso: <code>/aceptar &lt;ID_del_usuario&gt;</code>", parse_mode=ParseMode.HTML)
        return
    try: user_id_to_accept = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID de usuario inválido.")
        return
    try:
        await context.bot.unban_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id_to_accept, only_if_banned=True)
        admin_id = update.effective_user.id
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO expulsion_log (user_id, admin_id, action) VALUES (?, ?, 'accept')", (user_id_to_accept, admin_id))
            conn.commit()
        await update.message.reply_text(f"✅ Usuario {user_id_to_accept} ahora puede volver a unirse.")
    except TelegramError as e: await update.message.reply_text(f"❌ Error al aceptar: {e}")

async def estado_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM users ORDER BY username")
        all_users = c.fetchall()
    if not all_users:
        await update.message.reply_text("No hay usuarios registrados.")
        return
    text, reply_markup = await build_estado_page(all_users, 0, context)
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def limpieza_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await check_admin_permissions(update, context): return ConversationHandler.END
    await update.message.reply_text("🔎 Buscando miembros inactivos en el grupo... Esto puede tardar un momento.")
    inactive_users_in_db = []
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id, username FROM users WHERE active = 0")
        inactive_users_in_db = c.fetchall()
    if not inactive_users_in_db:
        await update.message.reply_text("✅ No se encontraron usuarios marcados como inactivos en la base de datos.")
        return ConversationHandler.END
    users_to_expel = []
    for user_id, username in inactive_users_in_db:
        try:
            chat_member = await context.bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
            if chat_member.status in [ChatMember.MEMBER, ChatMember.RESTRICTED]:
                users_to_expel.append({'id': user_id, 'username': username})
                await asyncio.sleep(0.1)
        except TelegramError:
            continue
    if not users_to_expel:
        await update.message.reply_text("✅ Todos los usuarios inactivos ya están fuera del grupo.")
        return ConversationHandler.END
    context.user_data['users_to_expel'] = users_to_expel
    user_list_text = "\n".join([f"- @{user['username'] or user['id']}" for user in users_to_expel[:15]])
    if len(users_to_expel) > 15:
        user_list_text += f"\n- ... y {len(users_to_expel) - 15} más."
    message = (
        "⚠️ <b>ADVERTENCIA: ACCIÓN IRREVERSIBLE</b> ⚠️\n\n"
        f"Se ha identificado a <b>{len(users_to_expel)}</b> miembro(s) con planes inactivos que aún permanecen en el grupo:\n\n"
        f"<code>{user_list_text}</code>\n\n"
        "Esta acción los expulsará a todos permanentemente.\n"
        "Para confirmar, escribe <code>/limpiezatotal</code> en los próximos 60 segundos.\n"
        "Para cancelar, escribe <code>/cancelar</code> o no hagas nada."
    )
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    return CONFIRM_PURGE

async def limpiezatotal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    users_to_expel = context.user_data.get('users_to_expel')
    if not users_to_expel:
        await update.message.reply_text("❌ No hay ninguna operación de limpieza pendiente o ha expirado. Usa /limpieza para iniciar una nueva.")
        return ConversationHandler.END
    await update.message.reply_text(f"✅ Confirmado. Iniciando expulsión de {len(users_to_expel)} miembros. Esto puede tardar...")
    expelled_count = 0
    for user in users_to_expel:
        try:
            await context.bot.ban_chat_member(chat_id=GROUP_CHAT_ID, user_id=user['id'])
            with sqlite3.connect(DB_PATH) as conn:
                c = conn.cursor()
                c.execute("INSERT INTO expulsion_log (user_id, admin_id, action) VALUES (?, ?, 'limpieza_masiva')",
                          (user['id'], update.effective_user.id))
                conn.commit()
            expelled_count += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error al expulsar a {user['id']} durante la limpieza: {e}")
    await update.message.reply_text(f"✅ Limpieza completada. Se han expulsado a {expelled_count} de {len(users_to_expel)} miembros.")
    context.user_data.pop('users_to_expel', None)
    return ConversationHandler.END

async def cancel_limpieza(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop('users_to_expel', None)
    await update.message.reply_text("ℹ️ Operación de limpieza cancelada.")
    return ConversationHandler.END

# Prole
async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_user_id = None
    if update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
    elif context.args:
        query = context.args[0]
        if query.isdigit():
            target_user_id = int(query)
        else:
            with sqlite3.connect(DB_PATH) as conn:
                c = conn.cursor()
                c.execute("SELECT tg_id FROM users WHERE username = ?", (query.lstrip('@'),))
                result = c.fetchone()
                if result:
                    target_user_id = result[0]
                else:
                    await update.message.reply_text(f"❌ No se encontró al usuario <code>{query}</code>.", parse_mode=ParseMode.HTML)
                    return
    else:
        target_user_id = update.effective_user.id

    if not target_user_id:
        await update.message.reply_text("No se pudo determinar el usuario.")
        return
    
    user_exists = await get_or_register_user(target_user_id, context)
    if not user_exists:
        await update.message.reply_text(f"ℹ️ El usuario con ID <code>{target_user_id}</code> no está en el grupo o el ID es incorrecto.", parse_mode=ParseMode.HTML)
        return

    is_caller_admin = await is_admin_check(update, context)

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id, username, start_date, end_date, active, activated_by_admin_id, initial_days, references_count FROM users WHERE tg_id = ?", (target_user_id,))
        target_user_data = c.fetchone()
        expulsion_count = 0
        if target_user_data:
            c.execute("SELECT COUNT(*) FROM expulsion_log WHERE user_id = ? AND action = 'expel'", (target_user_id,))
            expulsion_count = c.fetchone()[0]

    if not target_user_data:
        await update.message.reply_text(f"❌ Ocurrió un error al obtener la información del usuario <code>{target_user_id}</code>.", parse_mode=ParseMode.HTML)
        return

    tg_id, username, start_str, end_str, active, admin_id, initial_days, references_count = target_user_data
    
    if is_caller_admin:
        days_left_str = "N/A"
        if active and end_str:
            days_left = (make_aware(end_str) - datetime.now(timezone.utc)).days
            days_left_str = f"{max(0, days_left)} días"
        start_date_str = make_aware(start_str).strftime('%Y-%m-%d') if start_str else "N/A"
        message = (
            f"👤 <b>Detalles de {username or 'Sin Username'}</b> (<code>{tg_id}</code>)\n"
            f"   - <b>Estado:</b> {'Activo ✅' if active else 'Inactivo ❌'}\n"
            f"   - <b>Fecha Ingreso:</b> {start_date_str}\n"
            f"   - <b>Días Asignados:</b> {f'{initial_days} días' if initial_days else 'N/A'} {f'por {admin_id}' if admin_id else 'N/A'}\n"
            f"   - <b>Días Restantes:</b> {days_left_str}\n"
            f"   - <b>Expulsiones:</b> {expulsion_count}\n"
            f"   - <b>Referencias Enviadas:</b> {references_count}"
        )
    else:
        days_left_str = "Inactivo o sin plan"
        if active and end_str:
            days_left = (make_aware(end_str) - datetime.now(timezone.utc)).days
            days_left_str = f"{days_left} días restantes" if days_left >= 0 else "Expirado"
        message = (f"👤 <b>Información del Usuario</b>\n"
                   f"  - <b>Usuario:</b> @{username or 'N/A'}\n"
                   f"  - <b>ID:</b> <code>{tg_id}</code>\n"
                   f"  - <b>Estado:</b> {days_left_str}\n"
                   f"  - <b>Referencias Enviadas:</b> {references_count}")
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def web_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Envía el enlace de la web."""
    await update.message.reply_text("https://chk.leviatan-chk.com/")

async def bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Envía el enlace del propio bot."""
    await update.message.reply_text("@LevitanChk_bot")

# Mensajes programados
async def build_estado_page(users_list: list, page: int, context: ContextTypes.DEFAULT_TYPE) -> tuple[str, InlineKeyboardMarkup | None]:
    start_index = page * ESTADO_PAGE_SIZE
    end_index = start_index + ESTADO_PAGE_SIZE
    users_on_page = users_list[start_index:end_index]

    if not users_on_page:
        return "No hay usuarios en esta página.", None

    report_parts = [f"📊 <b>Reporte de Usuarios (Página {page + 1}/{ -(-len(users_list) // ESTADO_PAGE_SIZE) })</b>\n\n"]
    today = datetime.now(timezone.utc)
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in c.fetchall()]
        
        for user_row in users_on_page:
            user_dict = dict(zip(columns, user_row))
            user_id = user_dict['tg_id']
            
            membership_status_str = "Desconocido"
            try:
                chat_member = await context.bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
                if chat_member.status in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.CREATOR]:
                    membership_status_str = "En el grupo ✅"
                else:
                    membership_status_str = "Fuera del grupo ❌"
            except TelegramError as e:
                logger.warning(f"Error de API al verificar membresía de {user_id}: {e}")
                membership_status_str = "Fuera del grupo ❌"
            except Exception as e:
                logger.error(f"Error inesperado al verificar membresía de {user_id}: {e}")
                membership_status_str = "Error al verificar"

            c.execute("SELECT COUNT(*) FROM expulsion_log WHERE user_id = ? AND action = 'expel'", (user_id,))
            expulsion_count = c.fetchone()[0]

            days_left_str = "N/A"
            if user_dict.get('active') and user_dict.get('end_date'):
                days_left = (make_aware(user_dict['end_date']) - today).days
                days_left_str = f"{max(0, days_left)} días"
            
            start_date_str = make_aware(user_dict['start_date']).strftime('%Y-%m-%d') if user_dict.get('start_date') else "N/A"
            
            user_report = (
                f"👤 <b>{user_dict.get('username') or 'Sin Username'}</b> (<code>{user_id}</code>)\n"
                f"   - <b>Presencia:</b> {membership_status_str}\n"
                f"   - <b>Estado del Plan:</b> {'Activo✅' if user_dict.get('active') else 'Inactivo❌'}\n"
                f"   - <b>Días Restantes:</b> {days_left_str}\n"
                f"   - <b>Fecha Ingreso:</b> {start_date_str}\n"
                f"   - <b>Plan Asignado por:</b> {user_dict.get('activated_by_admin_id') or 'N/A'}\n"
                f"   - <b>Expulsiones:</b> {expulsion_count}\n"
                "---------------------------------\n"
            )
            report_parts.append(user_report)

    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Atrás", callback_data=f"estado_page_{page - 1}"))
    
    pagination_buttons.append(InlineKeyboardButton("Fin 🗑️", callback_data="estado_close"))

    if end_index < len(users_list):
        pagination_buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"estado_page_{page + 1}"))

    reply_markup = InlineKeyboardMarkup([pagination_buttons])
    
    return "".join(report_parts), reply_markup

async def estado_pagination_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[2])
    except (IndexError, ValueError):
        await query.edit_message_text("Error al procesar la página.")
        return
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM users ORDER BY username")
        all_users = c.fetchall()
    if not all_users:
        await query.edit_message_text("La lista de usuarios está vacía.")
        return
    text, reply_markup = await build_estado_page(all_users, page, context)
    if query.message.text != text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def estado_close_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"No se pudo borrar el mensaje de estado (quizás ya fue borrado): {e}")

async def activate_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    try:
        _, days_str, target_user_id_str = query.data.split("_")
        days, target_user_id = int(days_str), int(target_user_id_str)
    except (ValueError, IndexError):
        await query.answer("Error en los datos del botón.", show_alert=True)
        return
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT active FROM users WHERE tg_id = ?", (target_user_id,))
        result = c.fetchone()
        if result and result[0] == 1:
            await query.answer("Este usuario ya tiene un plan activo.", show_alert=True)
            return
        start_date, end_date, admin_id = datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(days=days), query.from_user.id
        c.execute("UPDATE users SET start_date = ?, end_date = ?, active = 1, activated_by_admin_id = ?, initial_days = ?, last_notification_date = NULL WHERE tg_id = ?",
                  (start_date.isoformat(), end_date.isoformat(), admin_id, days, target_user_id))
        c.execute("UPDATE pending_new_members SET approved = 1 WHERE user_id = ?", (target_user_id,))
        conn.commit()
    await query.answer(f"¡Plan de {days} días activado para el usuario {target_user_id}! Aprobado.", show_alert=True)
    
    # Cancelar la tarea de expulsión automática si existe
    try:
        jobs = context.job_queue.get_jobs_by_name(f"expel_unapproved_{target_user_id}")
        for job in jobs:
            job.schedule_removal()
        logger.info(f"Tarea de expulsion cancelada para {target_user_id}.")
    except Exception as e:
        logger.debug(f"No se pudo cancelar expulsion para {target_user_id}: {e}")
    
    try:
        await query.message.delete()
    except Exception as e:
        logger.error(f"No se pudo borrar el mensaje de bienvenida tras la activacion: {e}")

async def notify_user_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    today_str = datetime.now(BOT_TIMEZONE).date().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT end_date, active, last_notification_date FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()
    if not result: return
    end_date_str, active, last_notification_date = result
    if not active or not end_date_str or last_notification_date == today_str: return
    time_left = make_aware(end_date_str) - datetime.now(timezone.utc)
    if timedelta(days=0) < time_left <= timedelta(days=1):
        reminder_message = f"⏳ @{update.effective_user.username}, tu acceso a este grupo está a punto de expirar. Contacta a un administrador para renovarlo."
        await update.message.reply_text(reminder_message)
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET last_notification_date = ? WHERE tg_id = ?", (today_str, user_id))
            conn.commit()

async def check_expirations_and_notify(context: ContextTypes.DEFAULT_TYPE):
    now, one_day_from_now = datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(days=1)
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id, username FROM users WHERE active = 1 AND end_date BETWEEN ? AND ?", (now.isoformat(), one_day_from_now.isoformat()))
        expiring_users = c.fetchall()
    if not expiring_users:
        logger.info("Tarea de expiración ejecutada: No hay usuarios por expirar.")
        return
    logger.info(f"Tarea de expiración: {len(expiring_users)} usuario(s) por expirar.")
    for user_id, username in expiring_users:
        message_to_admin = f"⚠️ Alerta de Expiración: El plan del usuario @{username or 'N/A'} (ID: {user_id}) está a punto de expirar."
        for admin_id in ADMIN_IDS:
            try: await context.bot.send_message(chat_id=admin_id, text=message_to_admin)
            except TelegramError as e: logger.error(f"No se pudo notificar al admin {admin_id} sobre la expiración de {user_id}: {e}")

async def auto_expel_expired_users(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Ejecutando tarea de expulsión automática...")
    now_utc_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id, username FROM users WHERE active = 1 AND end_date < ?", (now_utc_iso,))
        expired_users = c.fetchall()
    if not expired_users:
        logger.info("No se encontraron usuarios expirados para expulsar.")
        return
    logger.info(f"Se encontraron {len(expired_users)} usuario(s) expirado(s). Procediendo a expulsar.")
    for user_id, username in expired_users:
        try:
            await context.bot.ban_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
            logger.info(f"Usuario {username} (ID: {user_id}) expulsado automáticamente.")
            notification = f"🤖 Expulsión Automática: El usuario @{username or 'N/A'} (ID: {user_id}) ha sido expulsado por tener una suscripción vencida."
            for admin_id in ADMIN_IDS:
                try: await context.bot.send_message(chat_id=admin_id, text=notification)
                except Exception as e: logger.error(f"No se pudo notificar al admin {admin_id} sobre la expulsión de {user_id}: {e}")
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET active = 0 WHERE tg_id = ?", (user_id,))
                conn.commit()
        except BadRequest as e:
            if "user not found" in str(e) or "user is not a member" in str(e):
                logger.warning(f"No se pudo expulsar al usuario {user_id} porque ya no es miembro. Marcando como inactivo.")
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("UPDATE users SET active = 0 WHERE tg_id = ?", (user_id,))
                    conn.commit()
            else: logger.error(f"Error de API al intentar expulsar al usuario {user_id}: {e}")
        except Exception as e: logger.error(f"Error inesperado al procesar la expulsión del usuario {user_id}: {e}")

async def send_random_daily_message_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        message = random.choice(MOTIVATIONAL_MESSAGES)
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=message)
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO bot_events (event_name, last_run) VALUES (?, ?)", 
                      ('daily_message', datetime.now(timezone.utc).isoformat()))
            conn.commit()
        logger.info("Mensaje diario aleatorio enviado y evento registrado.")
    except Exception as e: logger.error(f"No se pudo enviar el mensaje diario: {e}")

async def test_random_message_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    await update.message.reply_text("Forzando el envío del mensaje diario de prueba...")
    await send_random_daily_message_job(context)

async def test_admin_notification_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    message_to_admin = "⚠️ <b>Prueba de Alerta de Expiración</b>\nEl plan del usuario @usuario_ficticio (ID: <code>12345</code>) está a punto de expirar."
    notified_admins = 0
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=message_to_admin, parse_mode=ParseMode.HTML)
            notified_admins += 1
        except Exception as e: logger.error(f"No se pudo enviar la notificación de prueba al admin {admin_id}: {e}")
    await update.message.reply_text(f"✅ Notificación de prueba enviada a {notified_admins} administrador(es). Revisa tus mensajes privados.")

async def force_user_warning_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    reminder_message = "⏳ @usuario_de_prueba, tu acceso a este grupo está a punto de expirar. Contacta a un administrador para renovarlo. (Este es un mensaje de prueba)."
    await update.message.reply_text(reminder_message)

async def get_chat_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    chat_id, title, chat_type = update.effective_chat.id, update.effective_chat.title or "N/A", update.effective_chat.type or "N/A"
    message = (f"<b>Información del Chat:</b>\n"
               f"  - Título: <code>{title}</code>\n"
               f"  - ID: <code>{chat_id}</code>\n"
               f"  - Tipo: <code>{chat_type}</code>")
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

async def mensaje_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # Permitir en chat privado O en grupo de administradores
    is_private = update.effective_chat.type == ChatType.PRIVATE
    is_admin_group = update.effective_chat.id == ADMIN_GROUP_CHAT_ID
    
    if not (is_private or is_admin_group):
        await update.message.reply_text("❌ Este comando solo funciona en el chat privado del bot o en el grupo de administradores.")
        return ConversationHandler.END
    
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ No tienes permiso.")
        return ConversationHandler.END
    if GROUP_CHAT_ID == 0:
        await update.message.reply_text("❌ <code>GROUP_CHAT_ID</code> no configurado.", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    await update.message.reply_text("📝 Escribe el mensaje a enviar al grupo, o envía una imagen con pie de foto. Envía /cancelar para abortar.")
    return ENVIAR_MENSAJE

async def handle_mensaje_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        # Si es una imagen
        if update.message.photo:
            photo_file_id = update.message.photo[-1].file_id
            caption = f"<b>Mensaje del OLIMPO</b>\n\n{update.message.caption or ''}" if update.message.caption else "<b>Mensaje del OLIMPO</b>"
            await context.bot.send_photo(chat_id=GROUP_CHAT_ID, photo=photo_file_id, caption=caption, parse_mode=ParseMode.HTML)
            await update.message.reply_text("✅ Imagen enviada.")
        # Si es texto
        elif update.message.text:
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=f"<b>Mensaje del OLIMPO</b>\n\n{update.message.text}", parse_mode=ParseMode.HTML)
            await update.message.reply_text("✅ Mensaje enviado.")
        else:
            await update.message.reply_text("❌ Por favor envía texto o una imagen.")
            return ENVIAR_MENSAJE
    except TelegramError as e: await update.message.reply_text(f"❌ Error al enviar: {e}")
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Operación cancelada.")
    return ConversationHandler.END

def extract_status_change(chat_member_update: ChatMemberUpdated) -> tuple[bool, bool] | None:
    status_change = chat_member_update.difference().get("status")
    if status_change is None: return None
    old_is_member, new_is_member = chat_member_update.old_chat_member.is_member, chat_member_update.new_chat_member.is_member
    return old_is_member, new_is_member

async def send_welcome_message(new_member: Update.effective_user, update: Update, context: ContextTypes.DEFAULT_TYPE):
    if new_member.is_bot: return
    username = new_member.username or f"user_{new_member.id}"
    register_user(new_member.id, username)
    
    # Registrar como miembro pendiente de aprobación
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO pending_new_members (user_id, username, join_time, approved) VALUES (?, ?, ?, 0)",
                  (new_member.id, username, datetime.now(timezone.utc).isoformat()))
        conn.commit()
    
    welcome_message = (f"¡Bienvenido, @{username} (ID: <code>{new_member.id}</code>)!\n\n"
                     f"Soy <b>El Buen Samaritano</b>, el guardián de este lugar. "
                     f"Aquí valoramos el respeto, el compañerismo y la buena convivencia. "
                     f"Te invito a leer las reglas del grupo para mantener la armonía.\n\n"
                     f"⏳ <i>Tienes 1 minuto para ser aprobado por un administrador, de lo contrario serás expulsado automáticamente.</i>")
    keyboard = [[InlineKeyboardButton("✅ Activar 30 días", callback_data=f"activate_30_{new_member.id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=welcome_message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )
    
    # Programar expulsión automática en 1 minuto si no es aprobado
    context.job_queue.run_once(
        auto_expel_unapproved_member,
        when=timedelta(minutes=1),
        data={"user_id": new_member.id, "username": username, "chat_id": update.effective_chat.id},
        name=f"expel_unapproved_{new_member.id}"
    )

async def auto_expel_unapproved_member(context: ContextTypes.DEFAULT_TYPE):
    """Expulsa automáticamente a un miembro nuevo si no fue aprobado en 1 minuto."""
    job = context.job
    user_id = job.data["user_id"]
    username = job.data["username"]
    chat_id = job.data["chat_id"]
    
    # Verificar si el miembro fue aprobado
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT approved FROM pending_new_members WHERE user_id = ?", (user_id,))
        result = c.fetchone()
    
    if result and result[0] == 0:  # No fue aprobado
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            logger.info(f"Usuario {username} (ID: {user_id}) expulsado automáticamente por no ser aprobado en 1 minuto.")
            
            # Notificar a los admins
            notification = f"🤖 <b>Expulsión Automática:</b> El usuario @{username} (ID: {user_id}) ha sido expulsado por no ser aprobado en el tiempo límite."
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=notification, parse_mode=ParseMode.HTML)
                except Exception as e:
                    logger.error(f"No se pudo notificar al admin {admin_id}: {e}")
            
            # Marcar como expulsado en la BD
            with sqlite3.connect(DB_PATH) as conn:
                c = conn.cursor()
                c.execute("DELETE FROM pending_new_members WHERE user_id = ?", (user_id,))
                c.execute("INSERT INTO expulsion_log (user_id, admin_id, action) VALUES (?, ?, 'auto_expel_unapproved')",
                          (user_id, 0))
                conn.commit()
        except TelegramError as e:
            logger.error(f"Error al expulsar automáticamente al usuario {user_id}: {e}")

async def track_member_changes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = extract_status_change(update.chat_member)
    if result is None: return
    was_member, is_member = result
    if not was_member and is_member:
        new_member = update.chat_member.new_chat_member.user
        await send_welcome_message(new_member, update, context)

async def welcome_new_member_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for new_member in update.message.new_chat_members:
        await send_welcome_message(new_member, update, context)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Excepción al manejar una actualización:", exc_info=context.error)
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    error_message = f"Error: {context.error}"
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("INSERT INTO runtime_errors (timestamp, error_message, traceback) VALUES (?, ?, ?)",
                  (datetime.now(timezone.utc).isoformat(), error_message, tb_string))
        conn.commit()

# Comandos másivos 
async def todosmas_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await check_admin_permissions(update, context): return ConversationHandler.END
    if len(context.args) != 1:
        await update.message.reply_text("Uso: <code>/todosmas &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    try:
        days_to_add = int(context.args[0])
        if not (1 <= days_to_add <= 999): raise ValueError("Los días deben estar entre 1 y 999.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return ConversationHandler.END

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id, username FROM users WHERE active = 1")
        active_users = c.fetchall()

    if not active_users:
        await update.message.reply_text("No hay usuarios activos en la base de datos para modificar.")
        return ConversationHandler.END

    context.user_data["action_type"] = "add"
    context.user_data["days_to_modify"] = days_to_add
    context.user_data["affected_users"] = active_users

    user_list_text = "\n".join([f"- @{user[1] or user[0]}" for user in active_users[:15]])
    if len(active_users) > 15:
        user_list_text += f"\n- ... y {len(active_users) - 15} más."

    message = (
        "⚠️ <b>ADVERTENCIA: ACCIÓN MASIVA</b> ⚠️\n\n"
        f"Se añadirán <b>{days_to_add} días</b> a <b>{len(active_users)}</b> usuario(s) activo(s):\n\n"
        f"<code>{user_list_text}</code>\n\n"
        "Para confirmar, escribe <code>/aceptar_todos</code> en los próximos 60 segundos.\n"
        "Para cancelar, escribe <code>/cancelar_todos</code> o no hagas nada."
    )
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    return CONFIRM_TODOS

async def todosmenos_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await check_admin_permissions(update, context): return ConversationHandler.END
    if len(context.args) != 1:
        await update.message.reply_text("Uso: <code>/todosmenos &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    try:
        days_to_subtract = int(context.args[0])
        if not (1 <= days_to_subtract <= 999): raise ValueError("Los días deben estar entre 1 y 999.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return ConversationHandler.END

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT tg_id, username FROM users WHERE active = 1")
        active_users = c.fetchall()

    if not active_users:
        await update.message.reply_text("No hay usuarios activos en la base de datos para modificar.")
        return ConversationHandler.END

    context.user_data["action_type"] = "subtract"
    context.user_data["days_to_modify"] = days_to_subtract
    context.user_data["affected_users"] = active_users

    user_list_text = "\n".join([f"- @{user[1] or user[0]}" for user in active_users[:15]])
    if len(active_users) > 15:
        user_list_text += f"\n- ... y {len(active_users) - 15} más."

    message = (
        "⚠️ <b>ADVERTENCIA: ACCIÓN MASIVA</b> ⚠️\n\n"
        f"Se restarán <b>{days_to_subtract} días</b> a <b>{len(active_users)}</b> usuario(s) activo(s):\n\n"
        f"<code>{user_list_text}</code>\n\n"
        "Para confirmar, escribe <code>/aceptar_todos</code> en los próximos 60 segundos.\n"
        "Para cancelar, escribe <code>/cancelar_todos</code> o no hagas nada."
    )
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    return CONFIRM_TODOS

async def aceptar_todos_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await check_admin_permissions(update, context): return ConversationHandler.END

    action_type = context.user_data.get("action_type")
    days_to_modify = context.user_data.get("days_to_modify")
    affected_users = context.user_data.get("affected_users")

    if not all([action_type, days_to_modify, affected_users]):
        await update.message.reply_text("❌ No hay ninguna operación masiva pendiente o ha expirado. Usa /todosmas o /todosmenos para iniciar una nueva.")
        return ConversationHandler.END

    await update.message.reply_text(f"✅ Confirmado. Iniciando {'adición' if action_type == 'add' else 'resta'} de días para {len(affected_users)} miembros. Esto puede tardar...")

    modified_count = 0
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        for user_id, username in affected_users:
            try:
                c.execute("SELECT end_date, active FROM users WHERE tg_id = ?", (user_id,))
                result = c.fetchone()
                if not result or not result[1] or not result[0]:
                    logger.warning(f"Usuario {user_id} no activo o sin fecha de fin, saltando.")
                    continue

                current_end_date = make_aware(result[0])
                
                if action_type == "add":
                    new_end_date = current_end_date + timedelta(days=days_to_modify)
                else: # subtract
                    new_end_date = current_end_date - timedelta(days=days_to_modify)
                    if new_end_date < datetime.now(timezone.utc):
                        logger.warning(f"La resta de días para el usuario {user_id} resultaría en una fecha pasada. Saltando.")
                        continue
                
                c.execute("UPDATE users SET end_date = ?, last_notification_date = NULL WHERE tg_id = ?", (new_end_date.isoformat(), user_id))
                modified_count += 1
            except Exception as e:
                logger.error(f"Error al modificar días para el usuario {user_id}: {e}")
        conn.commit()

    await update.message.reply_text(f"✅ Operación masiva completada. Se {'añadieron' if action_type == 'add' else 'restaron'} días a {modified_count} de {len(affected_users)} miembros.")
    context.user_data.clear()
    return ConversationHandler.END

async def cancelar_todos_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("ℹ️ Operación masiva cancelada.")
    return ConversationHandler.END

# File id videos
async def get_video_file_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.video:
        file_id = update.message.video.file_id
        await update.message.reply_text(f"El file_id de este video es: <code>{file_id}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Por favor, envíame un video para obtener su file_id.")

# Función para extraer file_id de fotos en privado
async def get_photo_file_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extrae el file_id de una foto enviada en privado."""
    if update.message.photo:
        file_id = update.message.photo[-1].file_id
        await update.message.reply_text(f"El file_id de esta foto es: <code>{file_id}</code>", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Por favor, envíame una foto para obtener su file_id.")

# Videos de ayuda
async def pc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    video_file_id = "BAACAgEAAxkBAAPHaHwpgYkxhFkQV7DiVLugYFoXVUwAAucFAALgruhHoiyIqynNbpg2BA"
    await update.message.reply_video(video=video_file_id, caption="Tutorial de cómo obtener cookies en PC.")

async def android_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    video_file_id = "BAACAgEAAxkBAAPFaHwpY3s4cYpxi_VjfySv6wX5nHMAAuYFAALgruhHD-X0BmB2iOM2BA"
    await update.message.reply_video(video=video_file_id, caption="Tutorial de cómo obtener cookies en Android.")

async def manzana_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    video_file_id = "BAACAgEAAxkBAAPDaHwpGehxbZ8jb-c1fz-tytOSIqYAAuUFAALgruhHsGcJxYdonVM2BA"
    await update.message.reply_video(video=video_file_id, caption="Tutorial de cómo obtener cookies en iOS.")

# Menú de ayuda
MENU_COMMANDS = [
    {"command": "/android", "description": "Tutorial de cómo obtener cookies en Android"},
    {"command": "/pc", "description": "Tutorial de cómo obtener cookies en PC"},
    {"command": "/manzana", "description": "Tutorial de cómo obtener cookies en iOS"},
    {"command": "/info", "description": "Muestra información de tu plan"},
    {"command": "/web", "description": "Enlace a la página web"},
    {"command": "/bot", "description": "Enlace al bot"},
]
MENU_PAGE_SIZE = 3

async def build_menu_page(commands_list: list, page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    start_index = page * MENU_PAGE_SIZE
    end_index = start_index + MENU_PAGE_SIZE
    commands_on_page = commands_list[start_index:end_index]

    if not commands_on_page:
        return "No hay comandos disponibles en esta página.", None

    menu_parts = [f"📚 <b>Menú de Comandos (Página {page + 1}/{ -(-len(commands_list) // MENU_PAGE_SIZE) })</b>\n\n"]
    for cmd_data in commands_on_page:
        menu_parts.append(f"<b>{cmd_data['command']}</b>: {cmd_data['description']}\n")

    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Atrás", callback_data=f"menu_page_{page - 1}"))
    
    pagination_buttons.append(InlineKeyboardButton("Fin 🗑️", callback_data="menu_close"))

    if end_index < len(commands_list):
        pagination_buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"menu_page_{page + 1}"))

    reply_markup = InlineKeyboardMarkup([pagination_buttons])
    
    return "".join(menu_parts), reply_markup

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text, reply_markup = await build_menu_page(MENU_COMMANDS, 0)
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def menu_pagination_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[2])
    except (IndexError, ValueError):
        await query.edit_message_text("Error al procesar la página del menú.")
        return
    text, reply_markup = await build_menu_page(MENU_COMMANDS, page)
    if query.message.text != text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def menu_close_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"No se pudo borrar el mensaje del menú (quizás ya fue borrado): {e}")

# Menú Administrativo Exclusivo
ADMIN_MENU_COMMANDS = [
    {"command": "/plan", "description": "Activar plan para un usuario"},
    {"command": "/extender", "description": "Extender días a un usuario"},
    {"command": "/menos", "description": "Restar días a un usuario"},
    {"command": "/expulsar", "description": "Expulsar usuario del grupo"},
    {"command": "/aceptar", "description": "Readmitir usuario expulsado"},
    {"command": "/estado", "description": "Ver estado de todos los usuarios"},
    {"command": "/limpieza", "description": "Limpiar usuarios inactivos"},
    {"command": "/mensaje", "description": "Enviar mensaje/imagen al grupo"},
    {"command": "/ban", "description": "Agregar usuario a lista negra"},
    {"command": "/consulta", "description": "Consultar usuario en lista negra"},
    {"command": "/refe", "description": "Publicar referencia en canal"},
    {"command": "/todosmas", "description": "Agregar días a todos"},
    {"command": "/todosmenos", "description": "Restar días a todos"},
    {"command": "/scan", "description": "Escanear miembros del grupo"},
]
ADMIN_MENU_PAGE_SIZE = 4

async def build_admin_menu_page(commands_list: list, page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    start_index = page * ADMIN_MENU_PAGE_SIZE
    end_index = start_index + ADMIN_MENU_PAGE_SIZE
    commands_on_page = commands_list[start_index:end_index]

    if not commands_on_page:
        return "No hay comandos disponibles en esta página.", None

    menu_parts = [f"🔐 <b>Menú Administrativo (Página {page + 1}/{ -(-len(commands_list) // ADMIN_MENU_PAGE_SIZE) })</b>\n\n"]
    for cmd_data in commands_on_page:
        menu_parts.append(f"<b>{cmd_data['command']}</b>: {cmd_data['description']}\n")

    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton("⬅️ Atrás", callback_data=f"admin_menu_page_{page - 1}"))
    
    pagination_buttons.append(InlineKeyboardButton("Cerrar 🗑️", callback_data="admin_menu_close"))

    if end_index < len(commands_list):
        pagination_buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"admin_menu_page_{page + 1}"))

    reply_markup = InlineKeyboardMarkup([pagination_buttons])
    
    return "".join(menu_parts), reply_markup

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    text, reply_markup = await build_admin_menu_page(ADMIN_MENU_COMMANDS, 0)
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def admin_menu_pagination_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        page = int(query.data.split("_")[3])
    except (IndexError, ValueError):
        await query.edit_message_text("Error al procesar la página del menú administrativo.")
        return
    text, reply_markup = await build_admin_menu_page(ADMIN_MENU_COMMANDS, page)
    if query.message.text != text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def admin_menu_close_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"No se pudo borrar el mensaje del menú administrativo (quizás ya fue borrado): {e}")

# Funciones de Lista Negra (/ban y /consulta)
async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia el flujo para agregar un usuario a la lista negra."""
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END
    
    await update.message.reply_text(
        "🚫 <b>Agregar a Lista Negra</b>\n\n"
        "Proporciona el ID o username del usuario a banear (o responde a un mensaje):",
        parse_mode="HTML"
    )
    return BAN_USER_ID

async def ban_user_id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captura el ID o username del usuario a banear."""
    # Validar que sea admin
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END
    
    user_input = update.message.text.strip()
    
    if user_input.startswith("@"):
        context.user_data["ban_username"] = user_input[1:]
        context.user_data["ban_user_id"] = None
    elif user_input.isdigit():
        context.user_data["ban_user_id"] = int(user_input)
        context.user_data["ban_username"] = None
    elif update.message.reply_to_message:
        context.user_data["ban_user_id"] = update.message.reply_to_message.from_user.id
        context.user_data["ban_username"] = update.message.reply_to_message.from_user.username
    else:
        await update.message.reply_text("❌ Formato inválido. Usa: ID, @username o responde a un mensaje.")
        return BAN_USER_ID
    
    await update.message.reply_text("📝 Ahora envía el motivo del baneo:")
    return BAN_REASON

async def ban_reason_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captura el motivo del baneo."""
    # Validar que sea admin
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END
    
    context.user_data["ban_reason"] = update.message.text
    await update.message.reply_text("🖼️ Ahora envía la imagen de prueba/evidencia del usuario:")
    return BAN_IMAGE

async def ban_image_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captura la imagen y guarda el baneo en la BD."""
    # Validar que sea admin
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END
    
    if not update.message.photo:
        await update.message.reply_text("❌ Por favor envía una imagen.")
        return BAN_IMAGE
    
    photo_file_id = update.message.photo[-1].file_id
    user_id = context.user_data.get("ban_user_id")
    username = context.user_data.get("ban_username", "desconocido")
    reason = context.user_data.get("ban_reason")
    admin_id = update.effective_user.id
    ban_date = datetime.now(timezone.utc).isoformat()
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO blacklist (user_id, username, reason, image_file_id, banned_by_admin_id, ban_date) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, username, reason, photo_file_id, admin_id, ban_date)
            )
            conn.commit()
        
        await update.message.reply_text(
            f"✅ Usuario {username or user_id} añadido a la lista negra.",
            parse_mode="HTML"
        )
        logger.info(f"Usuario {username or user_id} (ID: {user_id}) añadido a blacklist por admin {admin_id}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al guardar: {e}")
        logger.error(f"Error al guardar baneo: {e}")
    
    return ConversationHandler.END

async def consulta_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Consulta un usuario en la lista negra."""
    if not await check_admin_permissions(update, context): return
    
    if not context.args:
        await update.message.reply_text("❌ Uso: /consulta <ID o @username>")
        return
    
    user_input = context.args[0].strip()
    if user_input.startswith("@"):
        user_input = user_input[1:]
        query_field = "username"
    else:
        try:
            user_input = int(user_input)
            query_field = "user_id"
        except ValueError:
            await update.message.reply_text("❌ Formato inválido. Usa: /consulta <ID> o /consulta @username")
            return
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute(f"SELECT user_id, username, reason, image_file_id, banned_by_admin_id, ban_date FROM blacklist WHERE {query_field} = ?", (user_input,))
            result = c.fetchone()
        
        if result:
            user_id, username, reason, image_file_id, admin_id, ban_date = result
            caption = (f"🚫 <b>No aceptar a este usuario</b>\n\n"
                      f"<b>Usuario:</b> @{username or user_id}\n"
                      f"<b>ID:</b> <code>{user_id}</code>\n"
                      f"<b>Motivo:</b> {reason}\n"
                      f"<b>Baneo por Admin ID:</b> <code>{admin_id}</code>\n"
                      f"<b>Fecha del Baneo:</b> {ban_date}")
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=image_file_id, caption=caption, parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("✅ Usuario no encontrado en la lista negra.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al consultar: {e}")
        logger.error(f"Error al consultar blacklist: {e}")

# Comando /refe - Publicar referencias en canal
async def refe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Publica una referencia de una imagen en el canal de referencias."""
    # Solo funciona en el grupo principal
    if update.effective_chat.id != GROUP_CHAT_ID:
        await update.message.reply_text("❌ Este comando solo funciona en el grupo principal.")
        return
    
    # Debe ser una respuesta a un mensaje con imagen
    if not update.message.reply_to_message or not update.message.reply_to_message.photo:
        await update.message.reply_text("❌ Debes responder a un mensaje que contenga una imagen.")
        return
    
    if REFERENCES_CHANNEL_ID == 0:
        await update.message.reply_text("❌ El canal de referencias no está configurado.")
        return
    
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    
    # Registrar usuario si no existe
    await get_or_register_user(user_id, context)
    
    # Obtener la imagen
    photo_file_id = update.message.reply_to_message.photo[-1].file_id
    
    # Obtener hora y fecha actual
    now = datetime.now(BOT_TIMEZONE)
    hora = now.strftime("%H:%M")
    fecha = now.strftime("%d/%m/%Y")
    
    # Obtener número de referencias del usuario
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT references_count FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()
        references_count = result[0] if result else 0
        references_count += 1
        
        # Actualizar contador
        c.execute("UPDATE users SET references_count = ? WHERE tg_id = ?", (references_count, user_id))
        
        # Guardar referencia en BD
        reference_date = now.isoformat()
        c.execute(
            "INSERT INTO user_references (user_id, username, image_file_id, reference_date) VALUES (?, ?, ?, ?)",
            (user_id, username, photo_file_id, reference_date)
        )
        conn.commit()
    
    # Crear mensaje para el canal
    caption = (
        f"Hora: {hora}\n"
        f"Fecha: {fecha}\n"
        f"Referencias: {references_count}\n"
        f"Informes del grupo:"
    )
    
    try:
        # Enviar imagen al canal
        await context.bot.send_photo(
            chat_id=REFERENCES_CHANNEL_ID,
            photo=photo_file_id,
            caption=caption,
            parse_mode=ParseMode.HTML
        )
        
        await update.message.reply_text(
            f"✅ Referencia publicada exitosamente.\n"
            f"Total de referencias: {references_count}",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Referencia publicada por {username} (ID: {user_id}). Total: {references_count}")
    except TelegramError as e:
        await update.message.reply_text(f"❌ Error al publicar la referencia: {e}")
        logger.error(f"Error al publicar referencia: {e}")

# Comando /topreferencias - Mostrar top 5 usuarios con más referencias
async def topreferencias_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra un marcador con los 5 usuarios que más referencias han enviado."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT tg_id, username, references_count FROM users WHERE references_count > 0 ORDER BY references_count DESC LIMIT 5"
            )
            top_users = c.fetchall()
        
        if not top_users:
            await update.message.reply_text("📊 Aún no hay referencias registradas.")
            return
        
        message_parts = ["🏆 <b>Top 5 Usuarios con Más Referencias</b>\n"]
        for idx, (user_id, username, count) in enumerate(top_users, 1):
            medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else "4️⃣" if idx == 4 else "5️⃣"
            message_parts.append(f"{medal} <b>@{username or f'user_{user_id}'}</b>: {count} referencias\n")
        
        await update.message.reply_text("".join(message_parts), parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Error al obtener el ranking: {e}")
        logger.error(f"Error en topreferencias: {e}")

# Main
def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN no definido. El bot no puede iniciar.")
        return
    init_db()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_error_handler(error_handler)
    job_queue = app.job_queue
    job_queue.run_daily(check_expirations_and_notify, time(12, 0, tzinfo=BOT_TIMEZONE), name="check_expirations")
    job_queue.run_daily(send_random_daily_message_job, time(hour=random.randint(8, 21), minute=random.randint(0, 59), tzinfo=BOT_TIMEZONE), name="daily_message")
    job_queue.run_repeating(auto_expel_expired_users, interval=timedelta(hours=1), first=10)
    
    # Filtro que permite chat privado O grupo de administradores
    admin_chat_filter = filters.ChatType.PRIVATE | filters.Chat(chat_id=ADMIN_GROUP_CHAT_ID)
    
    limpieza_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("limpieza", limpieza_command)],
        states={ CONFIRM_PURGE: [CommandHandler("limpiezatotal", limpiezatotal_command)] },
        fallbacks=[CommandHandler("cancelar", cancel_limpieza)],
        conversation_timeout=60,
        per_user=True,
        per_chat=True
    )

    mensaje_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("mensaje", mensaje_start_command)],
        states={ENVIAR_MENSAJE: [MessageHandler(((filters.TEXT | filters.PHOTO) & ~filters.COMMAND) & admin_chat_filter, handle_mensaje_input)]},
        fallbacks=[CommandHandler("cancelar", cancel_conversation)],
        per_user=True,
        per_chat=True
    )
    
    todos_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("todosmas", todosmas_command), CommandHandler("todosmenos", todosmenos_command)],
        states={ CONFIRM_TODOS: [CommandHandler("aceptar_todos", aceptar_todos_command)] },
        fallbacks=[CommandHandler("cancelar_todos", cancelar_todos_command)],
        conversation_timeout=60,
        per_user=True,
        per_chat=True
    )

    # Lista Negra (/ban y /consulta) - ConversationHandler (funciona en privado y grupo admin)
    # Registrar con maxima prioridad (group=-1)
    ban_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("ban", ban_command)],
        states={
            BAN_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_chat_filter, ban_user_id_handler)],
            BAN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_chat_filter, ban_reason_handler)],
            BAN_IMAGE: [MessageHandler(filters.PHOTO & admin_chat_filter, ban_image_handler)]
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation)],
        conversation_timeout=300,
        per_user=True,
        per_chat=True
    )
    
    # Registrar ConversationHandlers con maxima prioridad (group=-1)
    app.add_handler(ban_conv_handler, group=-1)
    app.add_handler(mensaje_conv_handler, group=-1)
    app.add_handler(todos_conv_handler, group=-1)
    app.add_handler(limpieza_conv_handler, group=-1)
    
    # Comandos administrativos simples (grupo 0, prioridad normal)
    app.add_handler(CommandHandler("plan", plan_command))
    app.add_handler(CommandHandler("consulta", consulta_command))
    app.add_handler(CommandHandler("refe", refe_command))
    app.add_handler(CommandHandler("topreferencias", topreferencias_command))
    app.add_handler(CommandHandler("extender", extender_command))
    app.add_handler(CommandHandler("menos", menos_command))
    app.add_handler(CommandHandler("expulsar", expulsar_command))
    app.add_handler(CommandHandler("aceptar", aceptar_command))
    app.add_handler(CommandHandler("estado", estado_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("getchatid", get_chat_id_command))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("web", web_command))
    app.add_handler(CommandHandler("bot", bot_command))
    app.add_handler(CommandHandler("prueba1", test_random_message_command))
    app.add_handler(CommandHandler("prueba2", test_admin_notification_command))
    app.add_handler(CommandHandler("prueba3", force_user_warning_command))
    app.add_handler(CallbackQueryHandler(activate_button_handler, pattern=r"^activate_"))
    app.add_handler(CallbackQueryHandler(estado_pagination_handler, pattern=r"^estado_page_"))
    app.add_handler(CallbackQueryHandler(estado_close_handler, pattern=r"^estado_close$"))
    app.add_handler(ChatMemberHandler(track_member_changes, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, welcome_new_member_fallback))
    # MessageHandler generico con baja prioridad (grupo 1) para no interceptar ConversationHandlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Chat(chat_id=GROUP_CHAT_ID), notify_user_on_message), group=1)
    
    # Video
    app.add_handler(CommandHandler("pc", pc_command))
    app.add_handler(CommandHandler("android", android_command))
    app.add_handler(CommandHandler("manzana", manzana_command))

    # File id
    app.add_handler(MessageHandler(filters.VIDEO & filters.ChatType.PRIVATE, get_video_file_id_command))
    app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, get_photo_file_id_command))

    # Menú
    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(CallbackQueryHandler(menu_pagination_handler, pattern=r"^menu_page_"))
    app.add_handler(CallbackQueryHandler(menu_close_handler, pattern=r"^menu_close$"))
    
    # Menú Administrativo
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CallbackQueryHandler(admin_menu_pagination_handler, pattern=r"^admin_menu_page_"))
    app.add_handler(CallbackQueryHandler(admin_menu_close_handler, pattern=r"^admin_menu_close$"))
    

    logger.info("El Buen Samaritano (v32) está listo y en funcionamiento.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()












