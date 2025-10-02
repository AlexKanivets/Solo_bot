NEW_USER_TEMPLATE = """🆕 <b>Новый пользователь</b>

👤 <b>ID:</b> <code>{user_id}</code>
{name_info}{username_info}
📍 <b>Источник:</b> {source}
🕐 <b>Время:</b> {time}"""

# Варианты источников для подстановки
SOURCE_DESCRIPTIONS = {
    "direct": "🔗 Прямая ссылка",
    "partner": "🤝 Партнерская программа (ID: {code})",
    "referral": "👥 Реферальная ссылка (ID: {code})",
    "coupon": "🎫 Купон ({code})",
    "gift": "🎁 Подарок ({code})",
    "utm": "📊 UTM-метка ({code})",
}

PAYMENT_SUCCESS_TEMPLATE = """💰 <b>Успешная оплата</b>

💵 <b>Сумма:</b> {amount}₽
🏦 <b>Система:</b> {payment_system}

👤 <b>ID:</b> <code>{user_id}</code>
{name_info}{username_info}
🕐 <b>Время:</b> {time}"""

USER_MESSAGE_TEMPLATE = """💬 <b>Сообщение от пользователя</b>

👤 <b>ID:</b> <code>{user_id}</code>
{name_info}{username_info}
📝 <b>Сообщение:</b> {message}
🕐 <b>Время:</b> {time}"""

# Шаблон для имени пользователя
NAME_INFO_TEMPLATE = "📝 <b>Имя:</b> {name}\n"

# Шаблон для username
USERNAME_INFO_TEMPLATE = "🔗 <b>Username:</b> @{username}\n"

SOURCE_EMOJI = {
    "direct": "🔗",
    "partner": "🤝", 
    "referral": "👥",
    "coupon": "🎫",
    "gift": "🎁",
    "utm": "📊",
}

PAYMENT_SYSTEM_NAMES = {
    # KassaAI
    "KASSAI_CARDS": "KassaAI Карты",
    "KASSAI_SBP": "KassaAI СБП",
    "kassai": "KassaAI",
    "kassai_plus": "KassaAI Plus",
    
    # RoboKassa
    "ROBOKASSA": "RoboKassa",
    "robokassa": "RoboKassa",
    
    # ЮKassa/ЮMoney
    "YOOKASSA": "ЮKassa",
    "yookassa": "ЮKassa",
    "YOOMONEY": "ЮMoney",
    "yoomoney": "ЮMoney",
    
    # FreeKassa
    "FREEKASSA": "FreeKassa",
    "freekassa": "FreeKassa",
    
    # CryptoBot
    "CRYPTOBOT": "CryptoBot",
    "cryptobot": "CryptoBot",
    
    # Heleket
    "HELEKET": "Heleket",
    "heleket": "Heleket",
    
    # Telegram Stars
    "STARS": "Telegram Stars",
    "stars": "Telegram Stars",
    
    # Tribute
    "TRIBUTE": "Tribute",
    "tribute": "Tribute",
    
    # WATA
    "WATA_RU": "WATA РФ",
    "WATA_SBP": "WATA СБП", 
    "WATA_INT": "WATA International",
    "wata_ru": "WATA РФ",
    "wata_sbp": "WATA СБП",
    "wata_int": "WATA International",
}
