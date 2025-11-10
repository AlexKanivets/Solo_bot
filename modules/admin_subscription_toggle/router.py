import time

from typing import Any

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from database import (
    get_key_details,
    get_servers,
    get_tariff_by_id,
    mark_key_as_frozen,
    mark_key_as_unfrozen,
)
from filters.admin import IsAdminFilter
from handlers.admin.users.keyboard import AdminUserEditorCallback, build_editor_kb
from handlers.buttons import APPLY, CANCEL
from handlers.keys.operations import renew_key_in_cluster, toggle_client_on_cluster
from handlers.texts import (
    FREEZE_SUBSCRIPTION_CONFIRM_MSG,
    SUBSCRIPTION_FROZEN_MSG,
    SUBSCRIPTION_UNFROZEN_MSG,
    UNFREEZE_SUBSCRIPTION_CONFIRM_MSG,
)
from logger import logger


router = Router(name="admin_subscription_toggle")


@router.callback_query(
    AdminUserEditorCallback.filter(F.action == "admin_freeze_subscription"),
    IsAdminFilter()
)
async def admin_freeze_subscription(
    callback_query: CallbackQuery,
    callback_data: AdminUserEditorCallback,
    session: Any
):
    """
    Показывает администратору диалог подтверждения заморозки подписки пользователя.
    """
    email = callback_data.data
    tg_id = callback_data.tg_id
    
    confirm_text = (
        f"❄️ <b>Заморозка подписки</b>\n\n"
        f"Вы действительно хотите заморозить подписку <code>{email}</code> "
        f"для пользователя <code>{tg_id}</code>?\n\n"
        f"При заморозке:\n"
        f"• Подписка будет отключена на сервере\n"
        f"• Оставшееся время будет сохранено\n"
        f"• Пользователь не сможет использовать подписку"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=APPLY,
            callback_data=AdminUserEditorCallback(
                action="admin_freeze_subscription_confirm",
                data=email,
                tg_id=tg_id
            ).pack()
        ),
        InlineKeyboardButton(
            text=CANCEL,
            callback_data=AdminUserEditorCallback(
                action="users_key_edit",
                data=email,
                tg_id=tg_id
            ).pack()
        )
    )
    
    await callback_query.message.edit_text(
        text=confirm_text,
        reply_markup=builder.as_markup()
    )


@router.callback_query(
    AdminUserEditorCallback.filter(F.action == "admin_freeze_subscription_confirm"),
    IsAdminFilter()
)
async def admin_freeze_subscription_confirm(
    callback_query: CallbackQuery,
    callback_data: AdminUserEditorCallback,
    session: Any
):
    """
    Выполняет заморозку подписки пользователя администратором.
    """
    email = callback_data.data
    tg_id = callback_data.tg_id
    admin_id = callback_query.from_user.id
    
    try:
        record = await get_key_details(session, email)
        if not record:
            await callback_query.message.edit_text(
                "❌ Ключ не найден.",
                reply_markup=build_editor_kb(tg_id)
            )
            return
        
        client_id = record["client_id"]
        cluster_id = record["server_id"]
        
        # Отключаем клиента на кластере
        result = await toggle_client_on_cluster(
            cluster_id, email, client_id, enable=False, session=session
        )
        
        if result["status"] == "success":
            # Сохраняем оставшееся время
            now_ms = int(time.time() * 1000)
            time_left = record["expiry_time"] - now_ms
            if time_left < 0:
                time_left = 0
            
            # Помечаем ключ как замороженный
            await mark_key_as_frozen(session, record["tg_id"], client_id, time_left)
            await session.commit()
            
            text_ok = (
                f"✅ <b>Подписка успешно заморожена</b>\n\n"
                f"📧 Email: <code>{email}</code>\n"
                f"👤 Пользователь: <code>{tg_id}</code>\n"
                f"⏱ Сохранено времени: {time_left // (1000 * 86400)} дн.\n\n"
                f"Администратор: @{callback_query.from_user.username or admin_id}"
            )
            
            logger.info(
                f"[AdminSubscriptionToggle] Администратор {admin_id} заморозил подписку {email} "
                f"пользователя {tg_id}"
            )
        else:
            text_ok = (
                f"⚠️ <b>Ошибка при заморозке подписки</b>\n\n"
                f"Детали: {result.get('error') or result.get('results')}"
            )
        
        await callback_query.message.edit_text(
            text=text_ok,
            reply_markup=build_editor_kb(tg_id)
        )
        
    except Exception as e:
        error_text = f"❌ Ошибка при заморозке подписки: {e}"
        logger.error(f"[AdminSubscriptionToggle] {error_text}")
        await callback_query.message.edit_text(
            text=error_text,
            reply_markup=build_editor_kb(tg_id)
        )


@router.callback_query(
    AdminUserEditorCallback.filter(F.action == "admin_unfreeze_subscription"),
    IsAdminFilter()
)
async def admin_unfreeze_subscription(
    callback_query: CallbackQuery,
    callback_data: AdminUserEditorCallback,
    session: Any
):
    """
    Показывает администратору диалог подтверждения разморозки подписки пользователя.
    """
    email = callback_data.data
    tg_id = callback_data.tg_id
    
    confirm_text = (
        f"🔓 <b>Разморозка подписки</b>\n\n"
        f"Вы действительно хотите разморозить подписку <code>{email}</code> "
        f"для пользователя <code>{tg_id}</code>?\n\n"
        f"При разморозке:\n"
        f"• Подписка будет включена на сервере\n"
        f"• Оставшееся время будет восстановлено\n"
        f"• Пользователь снова сможет использовать подписку"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=APPLY,
            callback_data=AdminUserEditorCallback(
                action="admin_unfreeze_subscription_confirm",
                data=email,
                tg_id=tg_id
            ).pack()
        ),
        InlineKeyboardButton(
            text=CANCEL,
            callback_data=AdminUserEditorCallback(
                action="users_key_edit",
                data=email,
                tg_id=tg_id
            ).pack()
        )
    )
    
    await callback_query.message.edit_text(
        text=confirm_text,
        reply_markup=builder.as_markup()
    )


@router.callback_query(
    AdminUserEditorCallback.filter(F.action == "admin_unfreeze_subscription_confirm"),
    IsAdminFilter()
)
async def admin_unfreeze_subscription_confirm(
    callback_query: CallbackQuery,
    callback_data: AdminUserEditorCallback,
    session: Any
):
    """
    Выполняет разморозку подписки пользователя администратором.
    """
    email = callback_data.data
    tg_id = callback_data.tg_id
    admin_id = callback_query.from_user.id
    
    try:
        record = await get_key_details(session, email)
        if not record:
            await callback_query.message.edit_text(
                "❌ Ключ не найден.",
                reply_markup=build_editor_kb(tg_id)
            )
            return
        
        client_id = record["client_id"]
        cluster_id = record["server_id"]
        
        # Включаем клиента на кластере
        result = await toggle_client_on_cluster(
            cluster_id, email, client_id, enable=True, session=session
        )
        
        if result["status"] != "success":
            logger.warning(
                f"[AdminSubscriptionToggle] Не удалось включить подписку: "
                f"{result.get('error') or result.get('results')}"
            )
        
        servers = await get_servers(session)
        cluster_servers = servers.get(cluster_id, [])
        
        if not cluster_servers:
            await callback_query.message.edit_text(
                "❌ Сервер не найден.",
                reply_markup=build_editor_kb(tg_id)
            )
            return
        
        # Получаем параметры тарифа
        tariff = await get_tariff_by_id(session, record["tariff_id"]) if record.get("tariff_id") else None
        
        if not tariff:
            logger.info("[AdminSubscriptionToggle] Тариф не найден — применяем дефолтные значения.")
            total_gb = 0
            hwid_limit = 0
        else:
            total_gb = int(tariff.get("traffic_limit") or 0)
            hwid_limit = int(tariff.get("device_limit") or 0)
        
        # Вычисляем новое время истечения
        now_ms = int(time.time() * 1000)
        leftover = record["expiry_time"]
        if leftover < 0:
            leftover = 0
        new_expiry_time = now_ms + leftover
        
        # Помечаем ключ как размороженный
        await mark_key_as_unfrozen(session, record["tg_id"], client_id, new_expiry_time)
        await session.commit()
        
        logger.info(
            f"[AdminSubscriptionToggle] Запуск renew_key_in_cluster с "
            f"expiry={new_expiry_time}, gb={total_gb}, hwid={hwid_limit}"
        )
        
        # Обновляем подписку на сервере
        await renew_key_in_cluster(
            cluster_id=cluster_id,
            email=email,
            client_id=client_id,
            new_expiry_time=new_expiry_time,
            total_gb=total_gb,
            session=session,
            hwid_device_limit=hwid_limit,
            reset_traffic=False,
        )
        
        text_ok = (
            f"✅ <b>Подписка успешно разморожена</b>\n\n"
            f"📧 Email: <code>{email}</code>\n"
            f"👤 Пользователь: <code>{tg_id}</code>\n"
            f"⏱ Восстановлено времени: {leftover // (1000 * 86400)} дн.\n\n"
            f"Администратор: @{callback_query.from_user.username or admin_id}"
        )
        
        logger.info(
            f"[AdminSubscriptionToggle] Администратор {admin_id} разморозил подписку {email} "
            f"пользователя {tg_id}"
        )
        
        await callback_query.message.edit_text(
            text=text_ok,
            reply_markup=build_editor_kb(tg_id)
        )
        
    except Exception as e:
        error_text = f"❌ Ошибка при разморозке подписки: {e}"
        logger.error(f"[AdminSubscriptionToggle] {error_text}")
        await callback_query.message.edit_text(
            text=error_text,
            reply_markup=build_editor_kb(tg_id)
        )
