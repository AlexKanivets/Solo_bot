from typing import Any

from aiogram.types import InlineKeyboardButton

from hooks.hooks import register_hook
from logger import logger


def register_admin_subscription_toggle_hooks():
    """
    Регистрирует хуки для добавления кнопки управления заморозкой подписки
    в админское меню редактирования ключа.
    """
    register_hook("admin_key_edit", on_admin_key_edit)
    logger.info("[AdminSubscriptionToggle] Хуки модуля зарегистрированы")


async def on_admin_key_edit(**kwargs) -> dict[str, Any] | None:
    """
    Добавляет кнопку заморозки/разморозки подписки в меню редактирования ключа администратором.
    
    Args:
        email: Email подписки
        tg_id: Telegram ID пользователя-владельца подписки
        is_frozen: Статус заморозки подписки
        **kwargs: Дополнительные аргументы
    
    Returns:
        Словарь с кнопкой для вставки в клавиатуру
    """
    try:
        email = kwargs.get("email")
        tg_id = kwargs.get("tg_id")
        is_frozen = kwargs.get("is_frozen", False)
        
        if not email or not tg_id:
            return None
        
        # Импортируем здесь, чтобы избежать циклических зависимостей
        from handlers.admin.users.keyboard import AdminUserEditorCallback
        
        # Определяем текст и действие кнопки в зависимости от статуса заморозки
        if is_frozen:
            button_text = "🔓 Разморозить подписку"
            action = "admin_unfreeze_subscription"
        else:
            button_text = "❄️ Заморозить подписку"
            action = "admin_freeze_subscription"
        
        # Создаем кнопку
        button = InlineKeyboardButton(
            text=button_text,
            callback_data=AdminUserEditorCallback(
                action=action,
                data=email,
                tg_id=tg_id
            ).pack()
        )
        
        # Возвращаем кнопку с инструкцией вставить её после кнопки "♻️ Сбросить трафик"
        return {
            "after": AdminUserEditorCallback(
                action="users_reset_traffic",
                data=email,
                tg_id=tg_id
            ).pack(),
            "button": button
        }
        
    except Exception as e:
        logger.error(f"[AdminSubscriptionToggle Hook] Ошибка в on_admin_key_edit: {e}")
        return None


# Автоматическая регистрация хуков при импорте модуля
register_admin_subscription_toggle_hooks()
