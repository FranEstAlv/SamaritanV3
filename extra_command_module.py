# ============================================
# COMANDO /extra - Extracción y Generación de BINs
# ============================================

import re
import random
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram.constants import ParseMode

# ============================================
# FUNCIONES AUXILIARES
# ============================================

def luhn_checksum(card_number):
    """Calcula el dígito verificador usando algoritmo Luhn."""
    def digits_of(n):
        return [int(d) for d in str(n)]
    
    digits = digits_of(card_number)
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    
    checksum = sum(digits_of(d * 2) for d in odd_digits)
    checksum += sum(even_digits)
    return checksum % 10

def luhn_validate(card_number):
    """Valida un número de tarjeta usando algoritmo Luhn."""
    return luhn_checksum(card_number[:-1]) == int(card_number[-1])

def detect_network(bin_number):
    """Detecta la red de la tarjeta basada en el BIN."""
    bin_num = int(bin_number[:6])
    
    # Visa
    if bin_number[0] == '4':
        return 'Visa'
    # Mastercard
    elif 51 <= bin_num <= 55 or 2221 <= bin_num <= 2720:
        return 'Mastercard'
    # American Express
    elif bin_number[:2] in ['34', '37']:
        return 'American Express'
    # Discover
    elif bin_number[:4] in ['6011', '622126', '622925'] or bin_number[:2] == '65':
        return 'Discover'
    # Diners Club
    elif bin_number[:2] in ['36', '38', '39']:
        return 'Diners Club'
    else:
        return 'Desconocida'

def extract_bin_from_card(card_string):
    """Extrae BIN, fecha y CVV de una tarjeta."""
    match = re.match(r'^(\d{12,16})\|(\d{2})\|(\d{4})\|(\d{3})$', card_string.strip())
    if match:
        card_num = match.group(1)
        month = match.group(2)
        year = match.group(3)
        cvv = match.group(4)
        
        bin_12 = card_num[:12]
        bin_6 = card_num[:6]
        
        return {
            'card_full': card_num,
            'bin_12': bin_12,
            'bin_6': bin_6,
            'month': month,
            'year': year,
            'cvv': cvv,
            'account_digits': card_num[12:]
        }
    return None

def generate_bin_variants(bin_base, account_start, account_end, month, year, cvv, count=10):
    """Genera variantes de un BIN."""
    variants = []
    
    account_range = list(range(account_start, min(account_end + 1, account_start + count)))
    
    for account_num in account_range:
        account_str = str(account_num).zfill(4)
        card_without_check = bin_base + account_str
        check_digit = luhn_checksum(card_without_check)
        card_full = card_without_check + str(check_digit)
        
        if luhn_validate(card_full):
            variants.append({
                'card': f"{card_full}|{month}|{year}|{cvv}",
                'bin': bin_base,
                'account': account_str,
                'network': detect_network(bin_base)
            })
    
    return variants

def detect_common_bin(cards_list):
    """Detecta el BIN común en una lista de tarjetas."""
    if not cards_list:
        return None
    
    bins_12 = []
    bins_6 = []
    
    for card_str in cards_list:
        extracted = extract_bin_from_card(card_str)
        if extracted:
            bins_12.append(extracted['bin_12'])
            bins_6.append(extracted['bin_6'])
    
    if len(set(bins_12)) == 1:
        return {
            'bin': bins_12[0],
            'type': 'bin_12',
            'account_digits': [extract_bin_from_card(c)['account_digits'] for c in cards_list]
        }
    elif len(set(bins_6)) == 1:
        return {
            'bin': bins_6[0],
            'type': 'bin_6',
            'account_digits': [extract_bin_from_card(c)['account_digits'] for c in cards_list]
        }
    
    return None

def analyze_pattern(account_digits_list):
    """Analiza patrones en los dígitos de cuenta."""
    if not account_digits_list:
        return None
    
    pattern = {
        'total_cards': len(account_digits_list),
        'fixed_positions': {},
        'variable_positions': {}
    }
    
    for pos in range(len(account_digits_list[0])):
        digits_at_pos = [acc[pos] for acc in account_digits_list]
        
        if len(set(digits_at_pos)) == 1:
            pattern['fixed_positions'][pos] = digits_at_pos[0]
        else:
            pattern['variable_positions'][pos] = set(digits_at_pos)
    
    return pattern

# ============================================
# COMANDOS PRINCIPALES
# ============================================

async def extra_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /extra - Menú principal."""
    keyboard = [
        [InlineKeyboardButton("1️⃣ Extrapolar BIN", callback_data="extra_bin")],
        [InlineKeyboardButton("2️⃣ Extrapolar desde Listas", callback_data="extra_listas")],
        [InlineKeyboardButton("3️⃣ Cargar BINs desde CSV", callback_data="extra_csv")],
        [InlineKeyboardButton("❌ Salir", callback_data="extra_exit")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🔧 <b>GENERADOR DE TARJETAS - v1.0.2</b>\n"
        "Luhn Engine | BIN | Extrapolador\n\n"
        "<b>FLUJO RECOMENDADO:</b>\n"
        "• Tienes listas → Extrapolar desde listas → genera BINs cercanos\n"
        "• Tienes BIN → Extrapolar BIN → genera variantes\n\n"
        "<b>Selecciona una opción:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )
    
    return 1

async def extra_bin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Opción 1: Extrapolar BIN."""
    await update.callback_query.answer()
    
    await update.callback_query.edit_message_text(
        "📝 <b>EXTRAPOLAR BIN</b>\n\n"
        "Proporciona el BIN en formato:\n"
        "<code>416916146995xxxx|11|2029|xxx</code>\n\n"
        "Donde:\n"
        "• <code>416916146995</code> = BIN (12 dígitos)\n"
        "• <code>11</code> = Mes expiración\n"
        "• <code>2029</code> = Año expiración\n"
        "• <code>xxx</code> = CVV\n\n"
        "Responde con el BIN a extrapolar:",
        parse_mode=ParseMode.HTML
    )
    
    context.user_data['extra_state'] = 'waiting_bin'
    return 1

async def extra_listas_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Opción 2: Extrapolar desde Listas."""
    await update.callback_query.answer()
    
    await update.callback_query.edit_message_text(
        "📝 <b>EXTRAPOLAR DESDE LISTAS</b>\n\n"
        "Pega tus listas (tarjetas activas conocidas) en formato:\n"
        "<code>4169161469950587|11|2029|245</code>\n\n"
        "Una tarjeta por línea.\n"
        "Escribe <code>FIN</code> o <code>fin</code> cuando termines.\n\n"
        "El sistema detectará automáticamente el BIN en común\n"
        "y generará variantes cercanas.",
        parse_mode=ParseMode.HTML
    )
    
    context.user_data['extra_state'] = 'waiting_listas'
    context.user_data['extra_listas'] = []
    return 1

async def extra_csv_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Opción 3: Cargar BINs desde CSV."""
    await update.callback_query.answer()
    
    await update.callback_query.edit_message_text(
        "📁 <b>CARGAR BINs DESDE CSV</b>\n\n"
        "Por favor, adjunta un archivo CSV con BINs.\n\n"
        "Formato esperado:\n"
        "<code>bin,month,year,cvv</code>\n"
        "<code>416916146995,11,2029,xxx</code>",
        parse_mode=ParseMode.HTML
    )
    
    context.user_data['extra_state'] = 'waiting_csv'
    return 1

async def extra_exit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Salir del menú."""
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("✅ Operación cancelada.")
    context.user_data.pop('extra_state', None)
    return ConversationHandler.END

async def extra_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja mensajes de texto."""
    state = context.user_data.get('extra_state')
    
    if state == 'waiting_bin':
        await handle_bin_input(update, context)
    elif state == 'waiting_listas':
        await handle_listas_input(update, context)
    
    return 1

async def handle_bin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa la entrada de BIN."""
    bin_input = update.message.text.strip()
    
    extracted = extract_bin_from_card(bin_input)
    if not extracted:
        await update.message.reply_text(
            "❌ Formato inválido.\n"
            "Usa: <code>416916146995xxxx|11|2029|xxx</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    bin_12 = extracted['bin_12']
    month = extracted['month']
    year = extracted['year']
    cvv = extracted['cvv']
    network = detect_network(bin_12)
    
    keyboard = [
        [InlineKeyboardButton("1️⃣ CERCA PRIMERO (Recomendado)", callback_data="extra_cerca")],
        [InlineKeyboardButton("2️⃣ RANGO AMPLIO", callback_data="extra_amplio")],
        [InlineKeyboardButton("❌ Cancelar", callback_data="extra_cancel_bin")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"✅ <b>BIN DETECTADO</b>\n\n"
        f"BIN base: <code>{bin_12}xxxx|{month}|{year}|{cvv}</code>\n"
        f"Red: <b>{network}</b>\n"
        f"Dígitos variados: <code>{extracted['account_digits']}</code>\n\n"
        f"¿Cómo quieres explorar?",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )
    
    context.user_data['extra_bin_data'] = {
        'bin': bin_12,
        'month': month,
        'year': year,
        'cvv': cvv,
        'network': network
    }

async def handle_listas_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Procesa la entrada de listas de tarjetas."""
    text = update.message.text.strip()
    
    if text.lower() in ['fin', 'end']:
        listas = context.user_data.get('extra_listas', [])
        
        if not listas:
            await update.message.reply_text("❌ No se proporcionaron tarjetas.")
            return
        
        common_bin = detect_common_bin(listas)
        
        if not common_bin:
            await update.message.reply_text(
                "❌ No se encontró BIN común en las tarjetas proporcionadas."
            )
            return
        
        pattern = analyze_pattern(common_bin['account_digits'])
        
        message = (
            f"✅ <b>BIN DETECTADO - POSICIONES ANALIZADAS</b>\n\n"
            f"Números analizados: {pattern['total_cards']}\n"
            f"BIN detectado: <code>{common_bin['bin']}</code>\n"
            f"Red identificada: {detect_network(common_bin['bin'])}\n\n"
            f"<b>Posiciones fijas:</b> {len(pattern['fixed_positions'])}\n"
            f"<b>Posiciones variables:</b> {len(pattern['variable_positions'])}\n\n"
            f"Análisis completado."
        )
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        
        context.user_data['extra_bin_data'] = {
            'bin': common_bin['bin'],
            'pattern': pattern,
            'listas': listas
        }
        context.user_data['extra_state'] = None
        
    else:
        context.user_data['extra_listas'].append(text)
        await update.message.reply_text(
            f"✅ Tarjeta agregada. Total: {len(context.user_data['extra_listas'])}\n"
            "Continúa agregando tarjetas o escribe <code>FIN</code> para terminar.",
            parse_mode=ParseMode.HTML
        )

async def extra_cerca_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exploración CERCA PRIMERO."""
    await update.callback_query.answer()
    
    bin_data = context.user_data.get('extra_bin_data', {})
    bin_12 = bin_data.get('bin')
    month = bin_data.get('month')
    year = bin_data.get('year')
    cvv = bin_data.get('cvv')
    
    variants = generate_bin_variants(bin_12, 0, 99, month, year, cvv, count=10)
    
    message = (
        f"🔍 <b>EXTRAPOLANDO EN MODO 'CERCA PRIMERO'</b>\n\n"
        f"BIN fijo: <code>{bin_12}</code>\n"
        f"Red: {bin_data.get('network')}\n"
        f"Dígitos variados: 0000-0099\n\n"
        f"<b>Extrapolados: {len(variants)}</b>\n\n"
    )
    
    for i, variant in enumerate(variants, 1):
        message += f"{i}. <code>{variant['card']}</code>\n"
    
    await update.callback_query.edit_message_text(
        message,
        parse_mode=ParseMode.HTML
    )

async def extra_amplio_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Exploración RANGO AMPLIO."""
    await update.callback_query.answer()
    
    bin_data = context.user_data.get('extra_bin_data', {})
    bin_12 = bin_data.get('bin')
    month = bin_data.get('month')
    year = bin_data.get('year')
    cvv = bin_data.get('cvv')
    
    variants = generate_bin_variants(bin_12, 0, 9999, month, year, cvv, count=50)
    
    message = (
        f"🔍 <b>EXTRAPOLANDO EN MODO 'RANGO AMPLIO'</b>\n\n"
        f"BIN fijo: <code>{bin_12}</code>\n"
        f"Red: {bin_data.get('network')}\n"
        f"Dígitos variados: 0000-9999\n\n"
        f"<b>Extrapolados: {len(variants)}</b>\n\n"
    )
    
    for i, variant in enumerate(variants[:20], 1):
        message += f"{i}. <code>{variant['card']}</code>\n"
    
    if len(variants) > 20:
        message += f"\n... y {len(variants) - 20} más"
    
    await update.callback_query.edit_message_text(
        message,
        parse_mode=ParseMode.HTML
    )

async def extra_cancel_bin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancelar operación de BIN."""
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("✅ Operación cancelada.")
    context.user_data.pop('extra_state', None)
    context.user_data.pop('extra_bin_data', None)
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancelar conversación."""
    await update.message.reply_text("✅ Operación cancelada.")
    return ConversationHandler.END

def setup_extra_handlers(app):
    """Configura los handlers para el comando /extra."""
    
    extra_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("extra", extra_command)],
        states={
            1: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, extra_message_handler),
                CallbackQueryHandler(extra_bin_callback, pattern="^extra_bin$"),
                CallbackQueryHandler(extra_listas_callback, pattern="^extra_listas$"),
                CallbackQueryHandler(extra_csv_callback, pattern="^extra_csv$"),
                CallbackQueryHandler(extra_exit_callback, pattern="^extra_exit$"),
                CallbackQueryHandler(extra_cerca_callback, pattern="^extra_cerca$"),
                CallbackQueryHandler(extra_amplio_callback, pattern="^extra_amplio$"),
                CallbackQueryHandler(extra_cancel_bin_callback, pattern="^extra_cancel_bin$"),
            ]
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation)],
        per_user=True,
        per_chat=True
    )
    
    app.add_handler(extra_conv_handler)
