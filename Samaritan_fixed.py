# el_buen_samaritano_v31.py

import logging
import sqlite3
import os
import sys
import re
import random
import asyncio
import traceback
import html
import csv
import io
from datetime import datetime, timedelta, time, timezone
from contextlib import contextmanager
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember, ChatMemberUpdated, ChatPermissions, User
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
    ChatMemberHandler,
)
from telegram.constants import ParseMode, ChatType
from telegram.error import TelegramError, BadRequest

# Logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Credenciales
def get_env_int(name: str, default: int = 0) -> int:
    raw_value = os.getenv(name, str(default)).strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        logger.warning("Variable de entorno %s inválida: %r. Usando %s.", name, raw_value, default)
        return default

def parse_admin_ids(raw_value: str) -> set[int]:
    admin_ids: set[int] = set()
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            admin_ids.add(int(item))
        except ValueError:
            logger.warning("ADMIN_IDS contiene un valor inválido: %r. Se ignorará.", item)
    return admin_ids

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GROUP_CHAT_ID = get_env_int("GROUP_CHAT_ID")
ADMIN_GROUP_CHAT_ID = get_env_int("ADMIN_GROUP_CHAT_ID")  # Chat ID del grupo de administradores
REFERENCES_CHANNEL_ID = get_env_int("REFERENCES_CHANNEL_ID")  # Canal para publicar referencias
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = parse_admin_ids(ADMIN_IDS_STR)
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
VER_PAGE_SIZE = 5
VER_MAX_LIMIT = 50
SETPLAN_PAGE_SIZE = 5
MUTE_DURATION_RE = re.compile(r"^(\d+)([mhd])$", re.IGNORECASE)

@contextmanager
def get_db_connection():
    """Abre SQLite con cierre garantizado, WAL y espera anti-lock para alta concurrencia."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def ensure_schema_migrations(cursor: sqlite3.Cursor) -> None:
    """Aplica migraciones idempotentes para bases creadas con versiones anteriores."""
    required_columns: dict[str, dict[str, str]] = {
        "users": {
            "username": "TEXT",
            "start_date": "TEXT",
            "end_date": "TEXT",
            "active": "INTEGER DEFAULT 0",
            "activated_by_admin_id": "INTEGER",
            "initial_days": "INTEGER DEFAULT 0",
            "last_notification_date": "TEXT",
            "references_count": "INTEGER DEFAULT 0",
        },
        "bot_requests": {
            "chat_type": "TEXT",
        },
        "blacklist": {
            "username": "TEXT",
            "reason": "TEXT",
            "image_file_id": "TEXT",
            "banned_by_admin_id": "INTEGER",
            "ban_date": "TEXT",
            "ban_timestamp": "TEXT",
        },
        "membership_audit": {
            "user_id": "INTEGER",
            "username": "TEXT",
            "action": "TEXT",
            "days": "INTEGER",
            "admin_id": "INTEGER",
            "admin_username": "TEXT",
            "timestamp": "TEXT",
        },
        "moderation_audit": {
            "user_id": "INTEGER",
            "username": "TEXT",
            "action": "TEXT",
            "duration_seconds": "INTEGER",
            "until_date": "TEXT",
            "admin_id": "INTEGER",
            "admin_username": "TEXT",
            "timestamp": "TEXT",
        },
        "active_mutes": {
            "username": "TEXT",
            "muted_at": "TEXT",
            "until_date": "TEXT",
            "duration_seconds": "INTEGER",
            "admin_id": "INTEGER",
            "admin_username": "TEXT",
            "active": "INTEGER DEFAULT 1",
            "unmuted_at": "TEXT",
            "unmuted_by_admin_id": "INTEGER",
        },
    }
    for table_name, columns in required_columns.items():
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        for column_name, ddl in columns.items():
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")

MOTIVATIONAL_MESSAGES = [
    "¡Suerte y mucho éxito en sus compras! Recuerden que son los mejores, que son OLIMPO todos."
]

# Mssj
ENVIAR_MENSAJE, CONFIRM_PURGE, CONFIRM_TODOS, ENVIAR_MENSAJE_ADMIN, ENVIAR_IMAGEN_ADMIN, BAN_USER_ID, BAN_REASON, BAN_IMAGE = range(8)

# DB
def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("PRAGMA journal_mode = WAL")
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
            c.execute("""
                CREATE TABLE IF NOT EXISTS bot_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    command TEXT,
                    request_date TEXT,
                    request_time TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    chat_type TEXT
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS user_roles (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    role TEXT DEFAULT 'member',
                    assigned_by_admin_id INTEGER,
                    assigned_date TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS membership_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    action TEXT,
                    days INTEGER,
                    admin_id INTEGER,
                    admin_username TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS moderation_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    username TEXT,
                    action TEXT,
                    duration_seconds INTEGER,
                    until_date TEXT,
                    admin_id INTEGER,
                    admin_username TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS active_mutes (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    muted_at TEXT,
                    until_date TEXT,
                    duration_seconds INTEGER,
                    admin_id INTEGER,
                    admin_username TEXT,
                    active INTEGER DEFAULT 1,
                    unmuted_at TEXT,
                    unmuted_by_admin_id INTEGER
                )
            """)
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_moderation_audit_user_timestamp
                ON moderation_audit (user_id, timestamp)
            """)
            c.execute("""
                CREATE INDEX IF NOT EXISTS idx_active_mutes_due
                ON active_mutes (active, until_date)
            """)
            ensure_schema_migrations(c)
            conn.commit()
            logger.info(f"Base de datos inicializada o verificada en: {DB_PATH}")
    except sqlite3.Error as e:
        logger.critical(f"Error crítico al inicializar la base de datos: {e}")
        raise

# Comandos
def make_aware(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(str(dt_str))
    except (TypeError, ValueError) as exc:
        logger.warning("Fecha inválida en base de datos: %r (%s)", dt_str, exc)
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

async def check_admin_permissions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user = update.effective_user
    if user and user.id in ADMIN_IDS:
        return True
    message = update.effective_message
    if message:
        await message.reply_text("❌ Este comando solo puede ser usado por administradores autorizados.")
    return False

async def is_admin_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.callback_query.from_user.id if update.callback_query else update.effective_user.id
    return user_id in ADMIN_IDS

async def log_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registra cada petición (comando) hecha al bot sin secuestrar el handler real."""
    try:
        message = update.effective_message
        if not message or not message.text or not update.effective_user or not update.effective_chat:
            return
        user_id = update.effective_user.id
        username = update.effective_user.username or "sin_username"
        command = message.text.split()[0]
        chat_type = update.effective_chat.type
        now = datetime.now(BOT_TIMEZONE)
        request_date = now.strftime("%Y-%m-%d")
        request_time = now.strftime("%H:%M:%S")
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO bot_requests (user_id, username, command, request_date, request_time, chat_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (user_id, username, command, request_date, request_time, chat_type))
            conn.commit()
    except Exception as e:
        logger.error(f"Error al registrar petición: {e}")

def register_user(tg_id: int, username: str):
    with get_db_connection() as conn:
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
    with get_db_connection() as conn:
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
    
    with get_db_connection() as conn:
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

async def get_username(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Obtiene el username de un usuario desde la BD o retorna el ID como string."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT username FROM users WHERE tg_id = ?", (user_id,))
            result = c.fetchone()
            if result and result[0]:
                return result[0]
    except Exception as e:
        logger.debug(f"Error al obtener username para {user_id}: {e}")
    return f"user_{user_id}"

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
            with get_db_connection() as conn:
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
        
    with get_db_connection() as conn:
        c = conn.cursor()
        start_date = datetime.now(timezone.utc)
        end_date = start_date + timedelta(days=days)
        admin_id = update.effective_user.id
        
        c.execute("UPDATE users SET start_date = ?, end_date = ?, active = 1, activated_by_admin_id = ?, initial_days = ?, last_notification_date = NULL WHERE tg_id = ?",
                  (start_date.isoformat(), end_date.isoformat(), admin_id, days, user_id))
        conn.commit()
    await log_membership_audit(
        user_id,
        await get_username(user_id, context),
        "plan",
        days,
        admin_id,
        update.effective_user.username or f"admin_{admin_id}"
    )
    await update.message.reply_text(f"✅ Plan de {days} días activado para el usuario {user_id}.")

async def extender_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context):
        return

    user_id = await resolve_user_target(update, context, 0)
    days_arg_index = 0 if update.message.reply_to_message else 1

    if not user_id or len(context.args) <= days_arg_index:
        await update.message.reply_text("Uso: Responde a un usuario con <code>/extender &lt;días&gt;</code> o usa <code>/extender &lt;ID/@username&gt; &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return

    try:
        days_to_add = int(context.args[days_arg_index])
        if not (1 <= days_to_add <= 999):
            raise ValueError("Los días deben estar entre 1 y 999.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return

    user_exists = await get_or_register_user(user_id, context)
    if not user_exists:
        await update.message.reply_text(f"❌ Usuario con ID {user_id} no encontrado ni en la base de datos ni en el grupo.")
        return

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT end_date, active FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()

    if not result or not result[1] or not result[0]:
        await update.message.reply_text(f"❌ El usuario {user_id} no tiene un plan activo para extender.")
        return

    current_end_date = make_aware(result[0])
    if not current_end_date:
        await update.message.reply_text(f"❌ La fecha de expiración del usuario {user_id} es inválida. Corrige el registro antes de extender.")
        return

    base_date = max(current_end_date, datetime.now(timezone.utc))
    new_end_date = base_date + timedelta(days=days_to_add)

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET end_date = ?, last_notification_date = NULL WHERE tg_id = ?", (new_end_date.isoformat(), user_id))
        conn.commit()

    admin_id = update.effective_user.id
    await log_membership_audit(
        user_id,
        await get_username(user_id, context),
        "extender",
        days_to_add,
        admin_id,
        update.effective_user.username or f"admin_{admin_id}"
    )
    await update.message.reply_text(f"✅ Suscripción extendida por {days_to_add} días. Nueva fecha de vencimiento: {new_end_date.strftime('%Y-%m-%d')}")

async def menos_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context):
        return

    user_id = await resolve_user_target(update, context, 0)
    days_arg_index = 0 if update.message.reply_to_message else 1

    if not user_id or len(context.args) <= days_arg_index:
        await update.message.reply_text("Uso: Responde a un usuario con <code>/menos &lt;días&gt;</code> o usa <code>/menos &lt;ID/@username&gt; &lt;días&gt;</code>", parse_mode=ParseMode.HTML)
        return

    try:
        days_to_subtract = int(context.args[days_arg_index])
        if days_to_subtract <= 0:
            raise ValueError("El número de días a restar debe ser positivo.")
    except ValueError as e:
        await update.message.reply_text(f"❌ Error en los argumentos.\nDetalle: {e}")
        return

    user_exists = await get_or_register_user(user_id, context)
    if not user_exists:
        await update.message.reply_text(f"❌ Usuario con ID {user_id} no encontrado ni en la base de datos ni en el grupo.")
        return

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT end_date, active FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()

    if not result or not result[1] or not result[0]:
        await update.message.reply_text(f"❌ El usuario {user_id} no tiene un plan activo para modificar.")
        return

    current_end_date = make_aware(result[0])
    if not current_end_date:
        await update.message.reply_text(f"❌ La fecha de expiración del usuario {user_id} es inválida. Corrige el registro antes de restar días.")
        return

    new_end_date = current_end_date - timedelta(days=days_to_subtract)
    if new_end_date < datetime.now(timezone.utc):
        await update.message.reply_text("❌ La operación resultaría en una fecha de expiración pasada. No se aplicaron cambios.")
        return

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET end_date = ?, last_notification_date = NULL WHERE tg_id = ?", (new_end_date.isoformat(), user_id))
        conn.commit()

    admin_id = update.effective_user.id
    await log_membership_audit(
        user_id,
        await get_username(user_id, context),
        "menos",
        days_to_subtract,
        admin_id,
        update.effective_user.username or f"admin_{admin_id}"
    )
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
        with get_db_connection() as conn:
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
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO expulsion_log (user_id, admin_id, action) VALUES (?, ?, 'accept')", (user_id_to_accept, admin_id))
            conn.commit()
        await update.message.reply_text(f"✅ Usuario {user_id_to_accept} ahora puede volver a unirse.")
    except TelegramError as e: await update.message.reply_text(f"❌ Error al aceptar: {e}")

async def estado_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    with get_db_connection() as conn:
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
    with get_db_connection() as conn:
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
            with get_db_connection() as conn:
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


# Moderación: /mute y /unmute
class MuteDurationError(ValueError):
    """Error de validación para duraciones de /mute."""


def get_moderation_chat_id(update: Update) -> int:
    """Devuelve el chat donde se aplicará la moderación."""
    if GROUP_CHAT_ID:
        return GROUP_CHAT_ID
    if update.effective_chat and update.effective_chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return update.effective_chat.id
    return 0


def parse_mute_duration(raw_duration: str | None) -> tuple[datetime | None, int | None, str]:
    """Convierte 10m/2h/7d a until_date UTC, segundos y etiqueta humana."""
    if raw_duration is None:
        return None, None, "indeterminado"

    duration_text = raw_duration.strip().strip('"').strip("'").lower()
    match = MUTE_DURATION_RE.fullmatch(duration_text)
    if not match:
        raise MuteDurationError(
            "Formato inválido. Usa 1m-59m, 1h-23h o 1d-999d. Ejemplo: <code>/mute 15m</code>."
        )

    value = int(match.group(1))
    unit = match.group(2).lower()
    if unit == "m":
        if not 1 <= value <= 59:
            raise MuteDurationError("Los minutos permitidos van de 1m a 59m.")
        delta = timedelta(minutes=value)
        label = f"{value} minuto{'s' if value != 1 else ''}"
    elif unit == "h":
        if not 1 <= value <= 23:
            raise MuteDurationError("Las horas permitidas van de 1h a 23h.")
        delta = timedelta(hours=value)
        label = f"{value} hora{'s' if value != 1 else ''}"
    elif unit == "d":
        if not 1 <= value <= 999:
            raise MuteDurationError("Los días permitidos van de 1d a 999d.")
        delta = timedelta(days=value)
        label = f"{value} día{'s' if value != 1 else ''}"
    else:
        raise MuteDurationError("Unidad inválida. Usa m, h o d.")

    return datetime.now(timezone.utc) + delta, int(delta.total_seconds()), label


def get_mute_usage_text() -> str:
    return (
        "Uso de moderación:\n"
        "• Responde a un usuario con <code>/mute</code> para silenciarlo indefinidamente.\n"
        "• Responde a un usuario con <code>/mute 15m</code>, <code>/mute 2h</code> o <code>/mute 7d</code>.\n"
        "• También puedes usar <code>/mute &lt;ID/@username&gt; [1m|1h|1d]</code>.\n"
        "• Para revertir: responde con <code>/unmute</code> o usa <code>/unmute &lt;ID/@username&gt;</code>.\n\n"
        "Rangos válidos: <code>1m</code>-<code>59m</code>, <code>1h</code>-<code>23h</code>, <code>1d</code>-<code>999d</code>."
    )


def resolve_mute_duration_argument(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str | None:
    """Determina qué argumento corresponde a la duración según reply o target explícito."""
    if update.effective_message and update.effective_message.reply_to_message:
        return context.args[0] if context.args else None
    return context.args[1] if len(context.args) >= 2 else None


def has_extra_mute_arguments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Detecta argumentos sobrantes para evitar comandos ambiguos."""
    if update.effective_message and update.effective_message.reply_to_message:
        return len(context.args) > 1
    return len(context.args) > 2


def log_moderation_audit(
    user_id: int,
    username: str,
    action: str,
    duration_seconds: int | None,
    until_date: datetime | None,
    admin_id: int,
    admin_username: str,
) -> None:
    """Registra acciones de moderación sin depender de estado global mutable."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO moderation_audit
                (user_id, username, action, duration_seconds, until_date, admin_id, admin_username)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                username,
                action,
                duration_seconds,
                until_date.isoformat() if until_date else None,
                admin_id,
                admin_username,
            ),
        )
        conn.commit()


def record_active_mute(
    user_id: int,
    username: str,
    until_date: datetime | None,
    duration_seconds: int | None,
    admin_id: int,
    admin_username: str,
) -> None:
    """Guarda el estado actual del silencio para restauraciones persistentes."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO active_mutes
                (user_id, username, muted_at, until_date, duration_seconds, admin_id, admin_username, active, unmuted_at, unmuted_by_admin_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, NULL, NULL)
            """,
            (
                user_id,
                username,
                datetime.now(timezone.utc).isoformat(),
                until_date.isoformat() if until_date else None,
                duration_seconds,
                admin_id,
                admin_username,
            ),
        )
        conn.commit()


def mark_active_mute_inactive(user_id: int, unmuted_by_admin_id: int | None) -> None:
    """Marca un silencio como terminado sin borrar auditoría operacional."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            UPDATE active_mutes
            SET active = 0, unmuted_at = ?, unmuted_by_admin_id = ?
            WHERE user_id = ?
            """,
            (datetime.now(timezone.utc).isoformat(), unmuted_by_admin_id, user_id),
        )
        conn.commit()


async def validate_moderation_target(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    target_user_id: int,
    chat_id: int,
) -> User | None:
    """Evita restringir administradores, owners o usuarios fuera del grupo objetivo."""
    if target_user_id in ADMIN_IDS:
        await update.effective_message.reply_text("❌ No puedo silenciar a un administrador autorizado del bot.")
        return None

    try:
        chat_member = await context.bot.get_chat_member(chat_id=chat_id, user_id=target_user_id)
    except TelegramError as exc:
        await update.effective_message.reply_text(
            f"❌ No pude verificar al usuario <code>{target_user_id}</code> en el grupo objetivo: {html.escape(str(exc))}",
            parse_mode=ParseMode.HTML,
        )
        return None

    if chat_member.status in {ChatMember.ADMINISTRATOR, ChatMember.OWNER}:
        await update.effective_message.reply_text("❌ No puedo silenciar administradores ni propietarios del grupo.")
        return None

    if chat_member.status in {ChatMember.LEFT, ChatMember.BANNED}:
        await update.effective_message.reply_text("❌ El usuario no está activo en el grupo objetivo.")
        return None

    return chat_member.user


async def mute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Silencia a un miembro por duración asignada o indefinidamente."""
    if not await check_admin_permissions(update, context):
        return

    message = update.effective_message
    if not message:
        return

    chat_id = get_moderation_chat_id(update)
    if not chat_id:
        await message.reply_text("❌ GROUP_CHAT_ID no está configurado y el comando no se ejecutó desde un grupo.")
        return

    if has_extra_mute_arguments(update, context):
        await message.reply_text(get_mute_usage_text(), parse_mode=ParseMode.HTML)
        return

    target_user_id = await resolve_user_target(update, context, 0)
    if not target_user_id:
        await message.reply_text(get_mute_usage_text(), parse_mode=ParseMode.HTML)
        return

    duration_argument = resolve_mute_duration_argument(update, context)
    try:
        until_date, duration_seconds, duration_label = parse_mute_duration(duration_argument)
    except MuteDurationError as exc:
        await message.reply_text(f"❌ {exc}", parse_mode=ParseMode.HTML)
        return

    target_user = await validate_moderation_target(update, context, target_user_id, chat_id)
    if not target_user:
        return

    target_username = target_user.username or await get_username(target_user_id, context)
    admin_id = update.effective_user.id
    admin_username = update.effective_user.username or f"admin_{admin_id}"
    register_user(target_user_id, target_username)
    record_active_mute(target_user_id, target_username, until_date, duration_seconds, admin_id, admin_username)

    try:
        await context.bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=target_user_id,
            permissions=ChatPermissions.no_permissions(),
            until_date=until_date,
            use_independent_chat_permissions=True,
        )
        log_moderation_audit(
            target_user_id,
            target_username,
            "mute",
            duration_seconds,
            until_date,
            admin_id,
            admin_username,
        )
    except TelegramError as exc:
        mark_active_mute_inactive(target_user_id, admin_id)
        await message.reply_text(f"❌ Error al silenciar: {html.escape(str(exc))}", parse_mode=ParseMode.HTML)
        logger.error("Error en /mute para %s: %s", target_user_id, exc)
        return

    escaped_username = html.escape(target_username or f"user_{target_user_id}")
    until_text = "" if until_date is None else f"\n⏳ Hasta: <code>{until_date.astimezone(BOT_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')}</code>"
    await message.reply_text(
        f"🔇 Usuario @{escaped_username} (<code>{target_user_id}</code>) silenciado por tiempo {html.escape(duration_label)}.{until_text}",
        parse_mode=ParseMode.HTML,
    )
    logger.info("Admin %s silenció a %s por %s", update.effective_user.id, target_user_id, duration_label)


async def unmute_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Restaura permisos de envío a un miembro silenciado."""
    if not await check_admin_permissions(update, context):
        return

    message = update.effective_message
    if not message:
        return

    chat_id = get_moderation_chat_id(update)
    if not chat_id:
        await message.reply_text("❌ GROUP_CHAT_ID no está configurado y el comando no se ejecutó desde un grupo.")
        return

    if (message.reply_to_message and context.args) or (not message.reply_to_message and len(context.args) != 1):
        await message.reply_text(get_mute_usage_text(), parse_mode=ParseMode.HTML)
        return

    target_user_id = await resolve_user_target(update, context, 0)
    if not target_user_id:
        await message.reply_text(get_mute_usage_text(), parse_mode=ParseMode.HTML)
        return

    target_user = await validate_moderation_target(update, context, target_user_id, chat_id)
    if not target_user:
        return

    target_username = target_user.username or await get_username(target_user_id, context)
    admin_id = update.effective_user.id
    admin_username = update.effective_user.username or f"admin_{admin_id}"
    register_user(target_user_id, target_username)

    try:
        await context.bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=target_user_id,
            permissions=ChatPermissions.all_permissions(),
            use_independent_chat_permissions=True,
        )
        log_moderation_audit(
            target_user_id,
            target_username,
            "unmute",
            None,
            None,
            admin_id,
            admin_username,
        )
        mark_active_mute_inactive(target_user_id, admin_id)
    except TelegramError as exc:
        await message.reply_text(f"❌ Error al quitar silencio: {html.escape(str(exc))}", parse_mode=ParseMode.HTML)
        logger.error("Error en /unmute para %s: %s", target_user_id, exc)
        return

    escaped_username = html.escape(target_username or f"user_{target_user_id}")
    await message.reply_text(
        f"🔊 Usuario @{escaped_username} (<code>{target_user_id}</code>) ya puede enviar mensajes nuevamente.",
        parse_mode=ParseMode.HTML,
    )
    logger.info("Admin %s quitó silencio a %s", update.effective_user.id, target_user_id)



async def auto_unmute_expired_mutes(context: ContextTypes.DEFAULT_TYPE):
    """Restaura silencios temporales vencidos, incluidos plazos mayores a 366 días."""
    if GROUP_CHAT_ID == 0:
        logger.warning("No se puede ejecutar auto_unmute_expired_mutes: GROUP_CHAT_ID no configurado.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT user_id, username, duration_seconds, until_date
            FROM active_mutes
            WHERE active = 1 AND until_date IS NOT NULL AND until_date <= ?
            """,
            (now_iso,),
        )
        due_mutes = c.fetchall()

    if not due_mutes:
        return

    for user_id, username, duration_seconds, until_date_str in due_mutes:
        try:
            await context.bot.restrict_chat_member(
                chat_id=GROUP_CHAT_ID,
                user_id=user_id,
                permissions=ChatPermissions.all_permissions(),
                use_independent_chat_permissions=True,
            )
            mark_active_mute_inactive(user_id, 0)
            log_moderation_audit(
                user_id,
                username or f"user_{user_id}",
                "auto_unmute",
                duration_seconds,
                make_aware(until_date_str),
                0,
                "system",
            )
            notification = (
                f"🔊 Auto-unmute aplicado a @{html.escape(username or f'user_{user_id}')} "
                f"(<code>{user_id}</code>) por vencimiento del silencio."
            )
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=notification, parse_mode=ParseMode.HTML)
                except TelegramError as exc:
                    logger.error("No se pudo notificar auto-unmute al admin %s: %s", admin_id, exc)
            logger.info("Auto-unmute aplicado a %s", user_id)
        except BadRequest as exc:
            error_text = str(exc).lower()
            if "user not found" in error_text or "user is not a member" in error_text:
                mark_active_mute_inactive(user_id, 0)
                log_moderation_audit(
                    user_id,
                    username or f"user_{user_id}",
                    "auto_unmute_skipped_not_member",
                    duration_seconds,
                    make_aware(until_date_str),
                    0,
                    "system",
                )
                logger.warning("Auto-unmute omitido para %s: el usuario ya no está en el grupo.", user_id)
            else:
                logger.error("Error de API en auto-unmute para %s: %s", user_id, exc)
        except TelegramError as exc:
            logger.error("Error en auto-unmute para %s: %s", user_id, exc)

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
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("SELECT tg_id FROM users WHERE username = ?", (query.lstrip('@'),))
                result = c.fetchone()
            if result:
                target_user_id = result[0]
            else:
                await update.message.reply_text(f"❌ No se encontró al usuario <code>{html.escape(query)}</code>.", parse_mode=ParseMode.HTML)
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

    with get_db_connection() as conn:
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
    safe_username = html.escape(username or 'N/A')

    if is_caller_admin:
        days_left_str = "N/A"
        if active and end_str:
            end_date = make_aware(end_str)
            if end_date:
                days_left = (end_date - datetime.now(timezone.utc)).days
                days_left_str = f"{max(0, days_left)} días"
        start_date = make_aware(start_str) if start_str else None
        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else "N/A"
        message = (
            f"👤 <b>Detalles de {safe_username}</b> (<code>{tg_id}</code>)\n"
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
            end_date = make_aware(end_str)
            if end_date:
                days_left = (end_date - datetime.now(timezone.utc)).days
                days_left_str = f"{days_left} días restantes" if days_left >= 0 else "Expirado"
        message = (f"👤 <b>Información del Usuario</b>\n"
                   f"  - <b>Usuario:</b> @{safe_username}\n"
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

    total_pages = -(-len(users_list) // ESTADO_PAGE_SIZE)
    report_parts = [f"📊 <b>Reporte de Usuarios (Página {page + 1}/{total_pages})</b>\n\n"]
    today = datetime.now(timezone.utc)

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in c.fetchall()]
        c.execute("SELECT user_id, COUNT(*) FROM expulsion_log WHERE action = 'expel' GROUP BY user_id")
        expulsion_counts = dict(c.fetchall())

    for user_row in users_on_page:
        user_dict = dict(zip(columns, user_row))
        user_id = user_dict['tg_id']

        membership_status_str = "Desconocido"
        try:
            chat_member = await context.bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=user_id)
            if chat_member.status in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
                membership_status_str = "En el grupo ✅"
            else:
                membership_status_str = "Fuera del grupo ❌"
        except TelegramError as e:
            logger.warning(f"Error de API al verificar membresía de {user_id}: {e}")
            membership_status_str = "Fuera del grupo ❌"
        except Exception as e:
            logger.error(f"Error inesperado al verificar membresía de {user_id}: {e}")
            membership_status_str = "Error al verificar"

        expulsion_count = expulsion_counts.get(user_id, 0)

        days_left_str = "N/A"
        if user_dict.get('active') and user_dict.get('end_date'):
            end_date = make_aware(user_dict['end_date'])
            if end_date:
                days_left = (end_date - today).days
                days_left_str = f"{max(0, days_left)} días"

        start_date = make_aware(user_dict.get('start_date')) if user_dict.get('start_date') else None
        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else "N/A"
        safe_username = html.escape(user_dict.get('username') or 'Sin Username')

        user_report = (
            f"👤 <b>{safe_username}</b> (<code>{user_id}</code>)\n"
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
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    await query.answer()
    try:
        page = int(query.data.split("_")[2])
    except (IndexError, ValueError):
        await query.edit_message_text("Error al procesar la página.")
        return
    with get_db_connection() as conn:
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
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
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

    source_chat_id = getattr(query.message, "chat_id", None)
    if source_chat_id is None and getattr(query.message, "chat", None):
        source_chat_id = query.message.chat.id
    if source_chat_id != GROUP_CHAT_ID:
        await query.answer("Este botón solo puede activar planes desde el grupo principal.", show_alert=True)
        logger.warning(
            "Intento de activar plan desde chat no autorizado: %s para callback %s",
            source_chat_id,
            query.data,
        )
        return

    try:
        _, days_str, target_user_id_str = query.data.split("_")
        days, target_user_id = int(days_str), int(target_user_id_str)
    except (ValueError, IndexError):
        await query.answer("Error en los datos del botón.", show_alert=True)
        return

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT username, active FROM users WHERE tg_id = ?", (target_user_id,))
        result = c.fetchone()

    if result and result[1] == 1:
        await query.answer("Este usuario ya tiene un plan activo.", show_alert=True)
        return

    username = result[0] if result and result[0] else f"user_{target_user_id}"
    start_date = datetime.now(timezone.utc)
    end_date = start_date + timedelta(days=days)
    admin_id = query.from_user.id

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET start_date = ?, end_date = ?, active = 1, activated_by_admin_id = ?, initial_days = ?, last_notification_date = NULL WHERE tg_id = ?",
                  (start_date.isoformat(), end_date.isoformat(), admin_id, days, target_user_id))
        c.execute("UPDATE pending_new_members SET approved = 1 WHERE user_id = ?", (target_user_id,))
        conn.commit()

    await log_membership_audit(
        target_user_id,
        username,
        "activate_button",
        days,
        admin_id,
        query.from_user.username or f"admin_{admin_id}"
    )
    await query.answer(f"¡Plan de {days} días activado para el usuario {target_user_id}! Aprobado.", show_alert=True)

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
    with get_db_connection() as conn:
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
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET last_notification_date = ? WHERE tg_id = ?", (today_str, user_id))
            conn.commit()

async def check_expirations_and_notify(context: ContextTypes.DEFAULT_TYPE):
    now, one_day_from_now = datetime.now(timezone.utc), datetime.now(timezone.utc) + timedelta(days=1)
    with get_db_connection() as conn:
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
    with get_db_connection() as conn:
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
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET active = 0 WHERE tg_id = ?", (user_id,))
                conn.commit()
        except BadRequest as e:
            if "user not found" in str(e) or "user is not a member" in str(e):
                logger.warning(f"No se pudo expulsar al usuario {user_id} porque ya no es miembro. Marcando como inactivo.")
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("UPDATE users SET active = 0 WHERE tg_id = ?", (user_id,))
                    conn.commit()
            else: logger.error(f"Error de API al intentar expulsar al usuario {user_id}: {e}")
        except Exception as e: logger.error(f"Error inesperado al procesar la expulsión del usuario {user_id}: {e}")

async def send_random_daily_message_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        message = random.choice(MOTIVATIONAL_MESSAGES)
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=message)
        with get_db_connection() as conn:
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
        if update.message.photo:
            photo_file_id = update.message.photo[-1].file_id
            raw_caption = update.message.caption or ""
            caption = f"<b>Mensaje del OLIMPO</b>\n\n{html.escape(raw_caption)}" if raw_caption else "<b>Mensaje del OLIMPO</b>"
            await context.bot.send_photo(chat_id=GROUP_CHAT_ID, photo=photo_file_id, caption=caption, parse_mode=ParseMode.HTML)
            context.user_data["_skip_private_file_id_once"] = True
            await update.message.reply_text("✅ Imagen enviada.")
        elif update.message.text:
            safe_text = html.escape(update.message.text)
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=f"<b>Mensaje del OLIMPO</b>\n\n{safe_text}", parse_mode=ParseMode.HTML)
            await update.message.reply_text("✅ Mensaje enviado.")
        else:
            await update.message.reply_text("❌ Por favor envía texto o una imagen.")
            return ENVIAR_MENSAJE
    except TelegramError as e:
        await update.message.reply_text(f"❌ Error al enviar: {e}")
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Operación cancelada.")
    return ConversationHandler.END

def extract_status_change(chat_member_update: ChatMemberUpdated) -> Optional[Tuple[bool, bool]]:
    """Extrae cambios de membresía usando el patrón oficial de PTB v20+."""
    status_change = chat_member_update.difference().get("status")
    old_is_member, new_is_member = chat_member_update.difference().get("is_member", (None, None))

    if status_change is None:
        return None

    old_status, new_status = status_change
    was_member = old_status in [ChatMember.MEMBER, ChatMember.OWNER, ChatMember.ADMINISTRATOR] or (
        old_status == ChatMember.RESTRICTED and old_is_member is True
    )
    is_member = new_status in [ChatMember.MEMBER, ChatMember.OWNER, ChatMember.ADMINISTRATOR] or (
        new_status == ChatMember.RESTRICTED and new_is_member is True
    )
    return was_member, is_member

async def send_welcome_message(new_member: User, update: Update, context: ContextTypes.DEFAULT_TYPE):
    if new_member.is_bot:
        return

    source_chat_id = update.effective_chat.id if update.effective_chat else None
    if source_chat_id != GROUP_CHAT_ID:
        logger.info(
            "Evento de bienvenida ignorado fuera del grupo principal: chat_id=%s user_id=%s",
            source_chat_id,
            new_member.id,
        )
        return

    username = new_member.username or f"user_{new_member.id}"
    register_user(new_member.id, username)

    # Registrar como miembro pendiente de aprobación únicamente en el grupo principal.
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO pending_new_members (user_id, username, join_time, approved)
            VALUES (?, ?, ?, 0)
            """,
            (new_member.id, username, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

    welcome_message = (
        f"¡Bienvenido, @{username} (ID: <code>{new_member.id}</code>)!\n\n"
        f"Soy <b>El Buen Samaritano</b>, el guardián de este lugar. "
        f"Aquí valoramos el respeto, el compañerismo y la buena convivencia. "
        f"Te invito a leer las reglas del grupo para mantener la armonía.\n\n"
        f"⏳ <i>Tienes 1 minuto para ser aprobado por un administrador, "
        f"de lo contrario serás expulsado automáticamente.</i>"
    )
    keyboard = [[InlineKeyboardButton("✅ Activar 30 días", callback_data=f"activate_30_{new_member.id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(
        chat_id=GROUP_CHAT_ID,
        text=welcome_message,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )

    # Programar expulsión automática solo contra el grupo principal.
    context.job_queue.run_once(
        auto_expel_unapproved_member,
        when=timedelta(minutes=1),
        data={"user_id": new_member.id, "username": username, "chat_id": GROUP_CHAT_ID},
        name=f"expel_unapproved_{new_member.id}",
    )

async def auto_expel_unapproved_member(context: ContextTypes.DEFAULT_TYPE):
    """Expulsa automáticamente a un miembro nuevo si no fue aprobado en 1 minuto."""
    job = context.job
    user_id = job.data["user_id"]
    username = job.data["username"]
    chat_id = job.data["chat_id"]
    
    # Verificar si el miembro fue aprobado
    with get_db_connection() as conn:
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
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("DELETE FROM pending_new_members WHERE user_id = ?", (user_id,))
                c.execute("INSERT INTO expulsion_log (user_id, admin_id, action) VALUES (?, ?, 'auto_expel_unapproved')",
                          (user_id, 0))
                conn.commit()
        except TelegramError as e:
            logger.error(f"Error al expulsar automáticamente al usuario {user_id}: {e}")

async def track_member_changes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.chat_member or update.chat_member.chat.id != GROUP_CHAT_ID:
        return

    result = extract_status_change(update.chat_member)
    if result is None:
        return

    was_member, is_member = result
    if not was_member and is_member:
        new_member = update.chat_member.new_chat_member.user
        await send_welcome_message(new_member, update, context)

async def welcome_new_member_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or update.effective_chat.id != GROUP_CHAT_ID:
        return

    for new_member in update.message.new_chat_members:
        await send_welcome_message(new_member, update, context)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Excepción al manejar una actualización:", exc_info=context.error)
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    error_message = f"Error: {context.error}"
    with get_db_connection() as conn:
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

    with get_db_connection() as conn:
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

    with get_db_connection() as conn:
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
    with get_db_connection() as conn:
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
    """Extrae el file_id de una foto enviada en privado, salvo si pertenece a otro flujo."""
    if context.user_data.pop("_skip_private_file_id_once", False):
        return
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
    {"command": "/mute", "description": "Silenciar usuario: indefinido, 1m-59m, 1h-23h o 1d-999d"},
    {"command": "/unmute", "description": "Quitar silencio a un usuario"},
    {"command": "/estado", "description": "Ver estado de todos los usuarios"},
    {"command": "/limpieza", "description": "Limpiar usuarios inactivos"},
    {"command": "/mensaje", "description": "Enviar mensaje/imagen al grupo"},
    {"command": "/ban", "description": "Agregar usuario a lista negra"},
    {"command": "/consulta", "description": "Consultar usuario en lista negra"},
    {"command": "/refe", "description": "Publicar referencia en canal"},
    {"command": "/todosmas", "description": "Agregar días a todos"},
    {"command": "/todosmenos", "description": "Restar días a todos"},
    {"command": "/setplan", "description": "Ver auditoría paginada de cambios de membresía"},
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
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
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
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"No se pudo borrar el mensaje del menú administrativo (quizás ya fue borrado): {e}")

# Funciones de Lista Negra (/ban y /consulta)
async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Inicia el flujo para agregar un usuario a la lista de no permitidos."""
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END

    target_user_id = await resolve_user_target(update, context, 0)
    if target_user_id:
        context.user_data["ban_user_id"] = target_user_id
        context.user_data["ban_username"] = await get_username(target_user_id, context)
        await update.message.reply_text("📝 Usuario detectado. Ahora envía el motivo del baneo:")
        return BAN_REASON

    await update.message.reply_text(
        "📋 <b>Agregar a Lista de No Permitidos</b>\n\n"
        "Proporciona el ID del usuario, su @username registrado en la BD, o responde a un mensaje:",
        parse_mode="HTML"
    )
    return BAN_USER_ID

async def list_user_id_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captura el ID o username del usuario a registrar en lista de no permitidos."""
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END

    user_id = await resolve_user_target(update, context, 0)
    if not user_id:
        await update.message.reply_text("❌ Usuario no encontrado. Usa un ID, un @username ya registrado en la BD o responde a un mensaje del usuario.")
        return BAN_USER_ID

    context.user_data["ban_user_id"] = user_id
    context.user_data["ban_username"] = await get_username(user_id, context)

    await update.message.reply_text("📝 Ahora envía el motivo del baneo:")
    return BAN_REASON

async def list_reason_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captura el motivo del registro en lista de no permitidos."""
    # Validar que sea admin
    if not await check_admin_permissions(update, context):
        return ConversationHandler.END
    
    context.user_data["ban_reason"] = update.message.text
    await update.message.reply_text("🖼️ Ahora envía la imagen de prueba/evidencia del usuario:")
    return BAN_IMAGE

async def list_image_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Captura la imagen y guarda el registro en la BD."""
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

    if not user_id:
        await update.message.reply_text("❌ Falta el ID del usuario. Reinicia el flujo con /ban.")
        return ConversationHandler.END

    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO blacklist (user_id, username, reason, image_file_id, banned_by_admin_id, ban_date) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, username, reason, photo_file_id, admin_id, ban_date)
            )
            conn.commit()

        await update.message.reply_text(
            f"✅ Usuario {html.escape(username or str(user_id))} registrado en lista de no permitidos.",
            parse_mode="HTML"
        )
        logger.info(f"Usuario {username or user_id} (ID: {user_id}) registrado en lista de no permitidos por admin {admin_id}")
        context.user_data["_skip_private_file_id_once"] = True
    except Exception as e:
        await update.message.reply_text(f"❌ Error al guardar: {e}")
        logger.error(f"Error al guardar baneo: {e}")
    finally:
        for key in ("ban_user_id", "ban_username", "ban_reason"):
            context.user_data.pop(key, None)

    return ConversationHandler.END

async def consulta_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Consulta un usuario en la lista negra."""
    if not await check_admin_permissions(update, context):
        return

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
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(f"SELECT user_id, username, reason, image_file_id, banned_by_admin_id, ban_date FROM blacklist WHERE {query_field} = ?", (user_input,))
            result = c.fetchone()

        if result:
            user_id, username, reason, image_file_id, admin_id, ban_date = result
            safe_username = html.escape(username or str(user_id))
            safe_reason = html.escape(reason or "Sin especificar")
            caption = (f"🚫 <b>No aceptar a este usuario</b>\n\n"
                      f"<b>Usuario:</b> @{safe_username}\n"
                      f"<b>ID:</b> <code>{user_id}</code>\n"
                      f"<b>Motivo:</b> {safe_reason}\n"
                      f"<b>Baneo por Admin ID:</b> <code>{admin_id}</code>\n"
                      f"<b>Fecha del Baneo:</b> {html.escape(str(ban_date))}")
            await context.bot.send_photo(chat_id=update.effective_chat.id, photo=image_file_id, caption=caption, parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("✅ Usuario no encontrado en la lista negra.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error al consultar: {e}")
        logger.error(f"Error al consultar blacklist: {e}")

# Comando /refe - Publicar referencias en canal
async def refe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Publica una referencia de una imagen en el canal de referencias."""
    if update.effective_chat.id != GROUP_CHAT_ID:
        await update.message.reply_text("❌ Este comando solo funciona en el grupo principal.")
        return

    if not update.message.reply_to_message or not update.message.reply_to_message.photo:
        await update.message.reply_text("❌ Debes responder a un mensaje que contenga una imagen.")
        return

    if REFERENCES_CHANNEL_ID == 0:
        await update.message.reply_text("❌ El canal de referencias no está configurado.")
        return

    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"

    await get_or_register_user(user_id, context)
    photo_file_id = update.message.reply_to_message.photo[-1].file_id

    now = datetime.now(BOT_TIMEZONE)
    hora = now.strftime("%H:%M")
    fecha = now.strftime("%d/%m/%Y")

    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE users SET references_count = COALESCE(references_count, 0) + 1 WHERE tg_id = ?", (user_id,))
        if c.rowcount == 0:
            c.execute("INSERT INTO users (tg_id, username, active, references_count) VALUES (?, ?, 0, 1)", (user_id, username))
        c.execute("SELECT references_count FROM users WHERE tg_id = ?", (user_id,))
        result = c.fetchone()
        references_count = result[0] if result else 1
        reference_date = now.isoformat()
        c.execute(
            "INSERT INTO user_references (user_id, username, image_file_id, reference_date) VALUES (?, ?, ?, ?)",
            (user_id, username, photo_file_id, reference_date)
        )
        conn.commit()

    caption = (
        f"🏛️ OLIMPO BINS 🏛️\n\n"
        f"<blockquote><b>Hora: {hora}\n"
        f"Fecha: {fecha}\n"
        f"Referencias: {references_count}\n"
        f"Informes del grupo: <a href='https://t.me/olimpobinsrefes/4237'>Aquí</a></b></blockquote>"
    )

    try:
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
        with get_db_connection() as conn:
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
            safe_username = html.escape(username or f'user_{user_id}')
            message_parts.append(f"{medal} <b>@{safe_username}</b>: {count} referencias\n")

        await update.message.reply_text("".join(message_parts), parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Error al obtener el ranking: {e}")
        logger.error(f"Error en topreferencias: {e}")

# Comando para Ver Últimas Peticiones
async def build_ver_page(limit: int, page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    """Construye una página compacta de las últimas peticiones registradas."""
    limit = max(1, min(limit, VER_MAX_LIMIT))
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM bot_requests")
        total_available = c.fetchone()[0]
        effective_total = min(total_available, limit)
        total_pages = max(1, -(-effective_total // VER_PAGE_SIZE))
        page = max(0, min(page, total_pages - 1))
        offset = page * VER_PAGE_SIZE
        rows_to_fetch = min(VER_PAGE_SIZE, max(0, effective_total - offset))
        c.execute("""
            SELECT user_id, username, command, request_date, request_time, chat_type
            FROM bot_requests
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """, (rows_to_fetch, offset))
        requests = c.fetchall()

    if not requests:
        keyboard = [[InlineKeyboardButton("Cerrar 🗑️", callback_data="ver_close")]]
        return "📭 No hay peticiones registradas.", InlineKeyboardMarkup(keyboard)

    message_parts = [f"📋 <b>Últimas peticiones al Bot</b>\nPágina {page + 1}/{total_pages} · Mostrando {effective_total} de {total_available}\n\n"]
    for idx, (user_id, username, command, req_date, req_time, chat_type) in enumerate(requests, offset + 1):
        safe_username = html.escape(username or "sin_username")
        safe_command = html.escape(command or "N/A")
        safe_chat_type = html.escape(chat_type or "N/A")
        message_parts.append(
            f"<b>{idx}.</b> <code>{safe_command}</code> · {safe_chat_type}\n"
            f"👤 @{safe_username} (<code>{user_id}</code>)\n"
            f"📅 {html.escape(str(req_date))} ⏰ {html.escape(str(req_time))}\n\n"
        )

    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Atrás", callback_data=f"ver_page_{page - 1}_{limit}"))
    buttons.append(InlineKeyboardButton("Cerrar 🗑️", callback_data="ver_close"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"ver_page_{page + 1}_{limit}"))

    return "".join(message_parts), InlineKeyboardMarkup([buttons])

async def ver_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra las últimas peticiones hechas al bot en panel paginado."""
    if not await check_admin_permissions(update, context):
        return

    try:
        n = VER_MAX_LIMIT
        if context.args and len(context.args) > 0:
            try:
                n = int(context.args[0])
                if n < 1:
                    await update.message.reply_text("❌ El número debe ser mayor a 0.")
                    return
                if n > VER_MAX_LIMIT:
                    await update.message.reply_text(f"❌ El máximo es {VER_MAX_LIMIT} peticiones.")
                    return
            except ValueError:
                await update.message.reply_text(f"❌ Debes proporcionar un número válido.\nUso: /ver [n] (1-{VER_MAX_LIMIT})")
                return

        text, reply_markup = await build_ver_page(n, 0)
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        logger.info(f"Admin {update.effective_user.id} consultó las últimas {n} peticiones")

    except Exception as e:
        await update.message.reply_text(f"❌ Error al obtener peticiones: {e}")
        logger.error(f"Error en comando /ver: {e}")

async def ver_pagination_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    await query.answer()
    try:
        _, _, page_str, limit_str = query.data.split("_")
        page = int(page_str)
        limit = int(limit_str)
    except (ValueError, IndexError):
        await query.edit_message_text("Error al procesar la página de peticiones.")
        return

    text, reply_markup = await build_ver_page(limit, page)
    if query.message.text != text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def ver_close_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"No se pudo borrar el panel de peticiones: {e}")

# Comando para Importar CSV
async def importar_csv_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Importa un archivo CSV a la base de datos."""
    if not await check_admin_permissions(update, context):
        return

    try:
        document = update.message.document
        if not document and update.message.reply_to_message:
            document = update.message.reply_to_message.document

        if not document:
            await update.message.reply_text(
                "❌ Debes adjuntar un archivo CSV o responder con /importar a un mensaje que contenga el CSV."
            )
            return

        file_name = document.file_name or "archivo.csv"
        if not file_name.lower().endswith('.csv'):
            await update.message.reply_text(
                "❌ El archivo debe ser de tipo CSV (.csv).\n\n"
                f"Archivo recibido: {html.escape(file_name)}"
            )
            return

        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()

        try:
            csv_content = file_content.decode('utf-8')
        except UnicodeDecodeError:
            csv_content = file_content.decode('latin-1')

        csv_reader = csv.DictReader(io.StringIO(csv_content))

        if not csv_reader.fieldnames:
            await update.message.reply_text("❌ El archivo CSV está vacío o no es válido.")
            return

        fieldnames = set(csv_reader.fieldnames)
        csv_reader = csv.DictReader(io.StringIO(csv_content))

        imported_count = 0
        error_count = 0
        table_name = "desconocida"

        if 'user_id' in fieldnames and 'username' in fieldnames and 'reason' in fieldnames:
            table_name = "blacklist"
            imported_count, error_count = await import_blacklist(csv_reader)
        elif 'tg_id' in fieldnames and 'username' in fieldnames and 'start_date' in fieldnames:
            table_name = "users"
            imported_count, error_count = await import_users(csv_reader)
        elif 'user_id' in fieldnames and 'command' in fieldnames and 'request_date' in fieldnames:
            table_name = "bot_requests"
            imported_count, error_count = await import_bot_requests(csv_reader)
        else:
            await update.message.reply_text(
                "❌ No se pudo identificar el tipo de datos en el CSV.\n\n"
                f"Columnas encontradas: {html.escape(', '.join(fieldnames))}"
            )
            return

        message = (
            f"✅ Importación completada\n\n"
            f"📊 Tabla: <b>{html.escape(table_name)}</b>\n"
            f"✅ Registros importados: {imported_count}\n"
            f"❌ Errores: {error_count}\n"
            f"📁 Archivo: {html.escape(file_name)}"
        )

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        logger.info(f"Admin {update.effective_user.id} importó {imported_count} registros a {table_name}")

    except Exception as e:
        await update.message.reply_text(f"❌ Error al importar CSV: {e}")
        logger.error(f"Error en comando /importar: {e}")

async def import_blacklist(csv_reader):
    """Importa datos a la tabla blacklist."""
    imported = 0
    errors = 0
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            for row in csv_reader:
                try:
                    user_id = int(row.get('user_id', 0))
                    username = row.get('username', 'desconocido').strip()
                    reason = row.get('reason', 'Sin especificar').strip()
                    image_file_id = row.get('image_file_id', '').strip()
                    banned_by_admin_id = int(row.get('banned_by_admin_id', 0))
                    ban_date = row.get('ban_date', datetime.now().strftime("%Y-%m-%d")).strip()
                    c.execute("""
                        INSERT OR REPLACE INTO blacklist 
                        (user_id, username, reason, image_file_id, banned_by_admin_id, ban_date)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (user_id, username, reason, image_file_id, banned_by_admin_id, ban_date))
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importando fila: {e}")
                    errors += 1
            conn.commit()
    except Exception as e:
        logger.error(f"Error en import_blacklist: {e}")
    return imported, errors

async def import_users(csv_reader):
    """Importa datos a la tabla users."""
    imported = 0
    errors = 0
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            for row in csv_reader:
                try:
                    tg_id = int(row.get('tg_id', 0))
                    username = row.get('username', 'desconocido').strip()
                    start_date = row.get('start_date', datetime.now().strftime("%Y-%m-%d")).strip()
                    end_date = row.get('end_date', '').strip()
                    active = int(row.get('active', 0))
                    activated_by_admin_id = int(row.get('activated_by_admin_id', 0))
                    initial_days = int(row.get('initial_days', 0))
                    references_count = int(row.get('references_count', 0))
                    c.execute("""
                        INSERT OR REPLACE INTO users 
                        (tg_id, username, start_date, end_date, active, activated_by_admin_id, initial_days, references_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (tg_id, username, start_date, end_date, active, activated_by_admin_id, initial_days, references_count))
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importando fila: {e}")
                    errors += 1
            conn.commit()
    except Exception as e:
        logger.error(f"Error en import_users: {e}")
    return imported, errors

async def import_bot_requests(csv_reader):
    """Importa datos a la tabla bot_requests."""
    imported = 0
    errors = 0
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            for row in csv_reader:
                try:
                    user_id = int(row.get('user_id', 0))
                    username = row.get('username', 'desconocido').strip()
                    command = row.get('command', '').strip()
                    request_date = row.get('request_date', datetime.now().strftime("%Y-%m-%d")).strip()
                    request_time = row.get('request_time', datetime.now().strftime("%H:%M:%S")).strip()
                    chat_type = row.get('chat_type', 'private').strip()
                    c.execute("""
                        INSERT INTO bot_requests 
                        (user_id, username, command, request_date, request_time, chat_type)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (user_id, username, command, request_date, request_time, chat_type))
                    imported += 1
                except Exception as e:
                    logger.error(f"Error importando fila: {e}")
                    errors += 1
            conn.commit()
    except Exception as e:
        logger.error(f"Error en import_bot_requests: {e}")
    return imported, errors

# Comando de Exportación de Base de Datos
async def archivo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Exporta todas las tablas de la base de datos a un archivo CSV organizado.
    Comando administrativo oculto (no aparece en menú).
    """
    # Verificar permisos de administrador
    if not await check_admin_permissions(update, context):
        await update.message.reply_text("❌ Solo administradores pueden usar este comando.")
        return
    
    try:
        # Crear archivo CSV en memoria
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Conectar a la base de datos
        with get_db_connection() as conn:
            c = conn.cursor()
            
            # Obtener lista de todas las tablas
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = c.fetchall()
            
            # Exportar cada tabla
            for table_name in tables:
                table = table_name[0]
                
                # Escribir título de la sección
                csv_writer.writerow([])  # Línea en blanco
                csv_writer.writerow([f"═══════════════════════════════════════"])
                csv_writer.writerow([f"TABLA: {table.upper()}"])
                csv_writer.writerow([f"═══════════════════════════════════════"])
                csv_writer.writerow([])  # Línea en blanco
                
                # Obtener estructura de la tabla
                c.execute(f"PRAGMA table_info({table})")
                columns = c.fetchall()
                column_names = [col[1] for col in columns]
                
                # Escribir encabezados
                csv_writer.writerow(column_names)
                
                # Obtener datos de la tabla
                c.execute(f"SELECT * FROM {table}")
                rows = c.fetchall()
                
                # Escribir datos con tag de sección
                for row in rows:
                    # Agregar tag de tabla al inicio de cada fila
                    tagged_row = [f"[{table.upper()}]"] + list(row)
                    csv_writer.writerow(tagged_row)
                
                csv_writer.writerow([])  # Línea en blanco
        
        # Obtener contenido del CSV
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()
        
        # Crear archivo temporal
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"samaritan_backup_{timestamp}.csv"
        
        # Guardar archivo
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        # Enviar archivo al usuario
        with open(filename, 'rb') as f:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename=filename,
                caption=f"📊 <b>Backup de Base de Datos</b>\n\n"
                        f"<b>Fecha:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"<b>Archivo:</b> <code>{filename}</code>\n"
                        f"<b>Formato:</b> CSV",
                parse_mode=ParseMode.HTML
            )
        
        # Eliminar archivo temporal
        os.remove(filename)
        
        logger.info(f"Base de datos exportada por admin {update.effective_user.id}")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error al exportar: {e}")
        logger.error(f"Error al exportar base de datos: {e}", exc_info=True)

# Main












# Comando /seller - Asignar/remover sellers
async def seller_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context):
        return

    seller_id = await resolve_user_target(update, context, 0)
    if not seller_id:
        await update.message.reply_text(
            "Uso: responde a un usuario con <code>/seller</code> o usa <code>/seller &lt;ID/@username&gt;</code>",
            parse_mode=ParseMode.HTML
        )
        return

    user_exists = await get_or_register_user(seller_id, context)
    if not user_exists:
        await update.message.reply_text(f"❌ Usuario con ID {seller_id} no encontrado ni en la base de datos ni en el grupo.")
        return

    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT users.username, COALESCE(user_roles.role, 'member')
                FROM users
                LEFT JOIN user_roles ON users.tg_id = user_roles.user_id
                WHERE users.tg_id = ?
            """, (seller_id,))
            result = c.fetchone()

        if not result:
            await update.message.reply_text(f"❌ Usuario {seller_id} no encontrado.")
            return

        username = result[0] or f"user_{seller_id}"
        current_role = result[1] or "member"

        with get_db_connection() as conn:
            c = conn.cursor()
            if current_role == "seller":
                c.execute("DELETE FROM user_roles WHERE user_id = ?", (seller_id,))
                action_message = f"✅ Rol de seller removido a @{html.escape(username)} (ID: {seller_id})"
                audit_action = "remove_seller"
            else:
                c.execute(
                    "INSERT OR REPLACE INTO user_roles (user_id, username, role, assigned_by_admin_id, assigned_date) VALUES (?, ?, 'seller', ?, ?)",
                    (seller_id, username, update.effective_user.id, datetime.now(timezone.utc).isoformat())
                )
                action_message = f"✅ Rol de seller asignado a @{html.escape(username)} (ID: {seller_id})"
                audit_action = "assign_seller"
            conn.commit()

        await log_membership_audit(
            seller_id,
            username,
            audit_action,
            0,
            update.effective_user.id,
            update.effective_user.username or f"admin_{update.effective_user.id}"
        )
        await update.message.reply_text(action_message, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        logger.error(f"Error en /seller: {e}")

# Comando /setadmin - Ver y gestionar roles
async def setadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context): return
    
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, username FROM user_roles WHERE role = 'seller'")
            sellers = c.fetchall()
        
        message = "👑 <b>GESTIÓN DE ROLES</b>\n\n"
        message += "<b>Administradores:</b>\n"
        for admin_id in ADMIN_IDS:
            message += f"• ID: {html.escape(str(admin_id))}\n"
        
        message += "\n<b>Sellers:</b>\n"
        if sellers:
            for seller_id, seller_username in sellers:
                message += f"• @{html.escape(seller_username or f'user_{seller_id}')} (ID: {html.escape(str(seller_id))})\n"
        else:
            message += "• Ninguno\n"
        
        message += "\n<b>Comandos:</b>\n"
        message += "• <code>/seller &lt;ID&gt;</code> - Asignar/remover seller\n"
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        logger.error(f"Error en /setadmin: {e}")

# Comando /setplan - Auditoría de cambios de membresía
async def build_setplan_page(page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    """Construye el panel paginado de auditoría de cambios de membresía."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM membership_audit")
        total_records = c.fetchone()[0]

        if total_records == 0:
            keyboard = [[InlineKeyboardButton("Cerrar 🗑️", callback_data="setplan_close")]]
            return "📊 No hay registros de auditoría.", InlineKeyboardMarkup(keyboard)

        total_pages = max(1, -(-total_records // SETPLAN_PAGE_SIZE))
        page = max(0, min(page, total_pages - 1))
        offset = page * SETPLAN_PAGE_SIZE
        c.execute(
            """
            SELECT user_id, username, action, days, admin_id, admin_username, timestamp
            FROM membership_audit
            ORDER BY timestamp DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (SETPLAN_PAGE_SIZE, offset),
        )
        audits = c.fetchall()

    message_parts = [
        f"📊 <b>AUDITORÍA DE CAMBIOS DE MEMBRESÍA</b>\n"
        f"Página {page + 1}/{total_pages} · {total_records} cambio(s) registrados\n\n"
    ]
    for idx, (user_id, username, action, days, admin_id, admin_username, timestamp) in enumerate(audits, offset + 1):
        safe_username = html.escape(username or f"user_{user_id}")
        safe_action = html.escape(action or "N/A")
        safe_admin_username = html.escape(admin_username or f"admin_{admin_id}")
        message_parts.append(
            f"<b>{idx}.</b> Usuario: @{safe_username} (<code>{html.escape(str(user_id))}</code>)\n"
            f"   Acción: {safe_action} ({days or 0} días)\n"
            f"   Admin: @{safe_admin_username} (<code>{html.escape(str(admin_id))}</code>)\n"
            f"   Fecha: {html.escape(str(timestamp))}\n\n"
        )

    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Atrás", callback_data=f"setplan_page_{page - 1}"))
    buttons.append(InlineKeyboardButton("Cerrar 🗑️", callback_data="setplan_close"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Siguiente ➡️", callback_data=f"setplan_page_{page + 1}"))

    return "".join(message_parts), InlineKeyboardMarkup([buttons])

async def setplan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_admin_permissions(update, context):
        return

    try:
        text, reply_markup = await build_setplan_page(0)
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        logger.error(f"Error en /setplan: {e}")

async def setplan_pagination_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    await query.answer()

    try:
        _, _, page_str = query.data.split("_")
        page = int(page_str)
    except (ValueError, IndexError):
        await query.edit_message_text("Error al procesar la página de auditoría.")
        return

    text, reply_markup = await build_setplan_page(page)
    if query.message.text != text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def setplan_close_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await is_admin_check(update, context):
        await query.answer("Esta acción solo puede ser realizada por un administrador.", show_alert=True)
        return
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"No se pudo borrar el panel de auditoría /setplan: {e}")

# Comando /staff - Lista de staff
async def staff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        message = "👑 <b>STAFF DEL OLIMPO</b>\n\n"
        message += "<b>Administradores</b>\n"
        message += "• <a href=\"https://t.me/MrMxyzptlk04\">@MrMxyzptlk04</a> - El Men\n"
        message += "• <a href=\"https://t.me/Fuzzcas2\">@Fuzzcas2</a> - Fuzz\n"
        message += "• <a href=\"https://t.me/XnaxAK\">@XnaxAK</a> - Alva\n"
        message += "• <a href=\"https://t.me/Elkezo\">@Elkezo</a> - KEZO\n"
        message += "• <a href=\"https://t.me/Hdzleg\">@Hdzleg</a> - CO\n"
        message += "• <a href=\"https://t.me/Carlosrdz19\">@Carlosrdz19</a> - STRIP\n\n"
        message += "<b>Sellers</b>\n"

        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, username FROM user_roles WHERE role = 'seller' ORDER BY username")
            sellers = c.fetchall()

        if sellers:
            for seller_id, seller_username in sellers:
                if seller_username:
                    safe_username = html.escape(seller_username)
                    message += f"• <a href=\"https://t.me/{safe_username}\">@{safe_username}</a>\n"
                else:
                    message += f"• ID: <code>{seller_id}</code>\n"
        else:
            message += "• Ninguno\n"

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        logger.error(f"Error en /staff: {e}")

# Función auxiliar para registrar auditoría
async def log_membership_audit(user_id: int, username: str, action: str, days: int, admin_id: int, admin_username: str):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("INSERT INTO membership_audit (user_id, username, action, days, admin_id, admin_username) VALUES (?, ?, ?, ?, ?, ?)",
                     (user_id, username, action, days, admin_id, admin_username))
            conn.commit()
    except Exception as e:
        logger.error(f"Error al registrar auditoría: {e}")

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
    job_queue.run_repeating(auto_unmute_expired_mutes, interval=timedelta(minutes=1), first=30, name="auto_unmute_expired_mutes")
    
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

    # Registrar auditoría de comandos antes de cualquier handler real; no secuestra otros grupos.
    app.add_handler(MessageHandler(filters.COMMAND, log_request), group=-2)

    # Lista de No Permitidos (/ban, /list y /consulta) - ConversationHandler (funciona en privado y grupo admin)
    # Registrar con maxima prioridad (group=-1)
    list_conv_handler = ConversationHandler(
        entry_points=[CommandHandler(["list", "ban"], list_command)],
        states={
            BAN_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_chat_filter, list_user_id_handler)],
            BAN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_chat_filter, list_reason_handler)],
            BAN_IMAGE: [MessageHandler(filters.PHOTO & admin_chat_filter, list_image_handler)]
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation)],
        conversation_timeout=300,
        per_user=True,
        per_chat=True
    )
    
    # Registrar ConversationHandlers con maxima prioridad (group=-1)
    app.add_handler(list_conv_handler, group=-1)
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
    app.add_handler(CommandHandler("mute", mute_command))
    app.add_handler(CommandHandler("unmute", unmute_command))
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
    
    # Comando de Archivo (oculto, solo admin, no aparece en menú)
    app.add_handler(CommandHandler("archivo", archivo_command))
    
    # Comando para Ver Últimas Peticiones (oculto, solo admin)
    app.add_handler(CommandHandler("ver", ver_command))
    app.add_handler(CallbackQueryHandler(ver_pagination_handler, pattern=r"^ver_page_"))
    app.add_handler(CallbackQueryHandler(ver_close_handler, pattern=r"^ver_close$"))
    
    # Comando para Importar CSV (oculto, solo admin)
    app.add_handler(CommandHandler("importar", importar_csv_command))
    
    # Comandos de gestión de roles
    app.add_handler(CommandHandler("seller", seller_command))
    app.add_handler(CommandHandler("setadmin", setadmin_command))
    app.add_handler(CommandHandler("setplan", setplan_command))
    app.add_handler(CallbackQueryHandler(setplan_pagination_handler, pattern=r"^setplan_page_"))
    app.add_handler(CallbackQueryHandler(setplan_close_handler, pattern=r"^setplan_close$"))
    app.add_handler(CommandHandler("staff", staff_command))
    
    # Menú Administrativo
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CallbackQueryHandler(admin_menu_pagination_handler, pattern=r"^admin_menu_page_"))
    app.add_handler(CallbackQueryHandler(admin_menu_close_handler, pattern=r"^admin_menu_close$"))
    

    logger.info("El Buen Samaritano (v32) está listo y en funcionamiento.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

