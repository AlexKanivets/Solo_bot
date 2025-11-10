from typing import Any

from aiogram.types import InlineKeyboardButton

from logger import logger


def register_admin_subscription_toggle_hooks():
    """
    Регистрирует хуки и применяет monkey patching для добавления кнопки управления 
    заморозкой подписки в админское меню редактирования ключа.
    
    ВАЖНО: Использует monkey patching для избежания изменения исходных файлов handlers/.
    """
    logger.info("[AdminSubscriptionToggle] Применение monkey patching...")
    
    try:
        # Импортируем необходимые модули
        from handlers.admin.users import keyboard as keyboard_module
        from hooks.hooks import run_hooks
        from hooks.hook_buttons import insert_hook_buttons
        
        # Сохраняем оригинальную функцию
        original_build_key_edit_kb = keyboard_module.build_key_edit_kb
        
        # Создаем обертку с поддержкой хуков
        def patched_build_key_edit_kb(key_details: dict, email: str):
            """
            Обертка для build_key_edit_kb с поддержкой хуков модулей.
            Добавляет кнопки из модулей через систему хуков.
            """
            # Вызываем оригинальную функцию
            builder_markup = original_build_key_edit_kb(key_details, email)
            
            # Получаем builder из markup
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            from aiogram.types import InlineKeyboardMarkup
            builder = InlineKeyboardBuilder.from_markup(builder_markup)
            
            # Синхронно вызываем хуки (создаем event loop если нужно)
            import asyncio
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Получаем кнопки из модулей
            module_buttons = loop.run_until_complete(run_hooks(
                "admin_key_edit",
                email=email,
                tg_id=key_details["tg_id"],
                is_frozen=key_details.get("is_frozen", False)
            ))
            
            # Вставляем кнопки из модулей
            builder = insert_hook_buttons(builder, module_buttons)
            
            return builder.as_markup()
        
        # Заменяем функцию на пропатченную версию
        keyboard_module.build_key_edit_kb = patched_build_key_edit_kb
        
        logger.info("[AdminSubscriptionToggle] ✅ Monkey patching успешно применен")
        logger.info("[AdminSubscriptionToggle] Хуки модуля зарегистрированы")
        
    except Exception as e:
        logger.error(f"[AdminSubscriptionToggle] ❌ Ошибка при применении monkey patching: {e}")
        import traceback
        traceback.print_exc()


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


# Автоматическая регистрация хуков и применение monkey patching при импорте модуля
register_admin_subscription_toggle_hooks()
