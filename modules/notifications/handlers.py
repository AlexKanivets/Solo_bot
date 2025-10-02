import asyncio
from datetime import datetime
from typing import Optional
import pytz
import time

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select
from database.models import Admin, Payment
from database.payments import get_last_payments
from logger import logger

from .settings import (
    NOTIFICATIONS_ENABLED,
    NOTIFY_NEW_USERS, 
    NOTIFY_PAYMENT_SUCCESS,
    NOTIFICATION_SEND_MODE,
    NOTIFICATION_CHANNEL_ID,
    NOTIFICATION_TOPIC_NEW_USERS,
    NOTIFICATION_TOPIC_PAYMENTS,
    NOTIFICATION_TOPIC_MESSAGES,
    NOTIFICATION_BOT_TOKEN,
    MIN_PAYMENT_AMOUNT_NOTIFY,
    NOTIFICATION_TIMEZONE,
    NOTIFY_USER_MESSAGES,
    NOTIFICATION_RATE_LIMIT,
)
from .texts import (
    NEW_USER_TEMPLATE,
    PAYMENT_SUCCESS_TEMPLATE,
    USER_MESSAGE_TEMPLATE,
    SOURCE_DESCRIPTIONS,
    SOURCE_EMOJI,
    PAYMENT_SYSTEM_NAMES,
    NAME_INFO_TEMPLATE,
    USERNAME_INFO_TEMPLATE,
)

last_send_time = 0
min_interval = 1.0 / NOTIFICATION_RATE_LIMIT
notification_queue = asyncio.Queue()
queue_worker_running = False


async def send_notification_with_rate_limit(text: str, session: AsyncSession, topic_id: str = None):
    global queue_worker_running

    if not queue_worker_running:
        queue_worker_running = True
        asyncio.create_task(start_notification_worker())
        await asyncio.sleep(0.1)
    
    await notification_queue.put({
        'text': text,
        'session': session,
        'topic_id': topic_id
    })


async def start_notification_worker():
    global last_send_time
    processed_count = 0
    
    while True:
        try:
            notification = await notification_queue.get()

            current_time = time.time()
            time_since_last = current_time - last_send_time
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                await asyncio.sleep(sleep_time)

            try:
                await send_notification_to_admins(
                    notification['text'], 
                    notification['session'], 
                    notification['topic_id']
                )
                
                last_send_time = time.time()
                processed_count += 1

            except TelegramRetryAfter as e:
                retry_after = e.retry_after
                logger.warning(f"[Notifications] Лимит превышен, ждем {retry_after} секунд")
                await asyncio.sleep(retry_after)

                try:
                    await send_notification_to_admins(
                        notification['text'], 
                        notification['session'], 
                        notification['topic_id']
                    )
                    
                    last_send_time = time.time()
                    processed_count += 1

                except Exception as retry_error:
                    logger.error(f"[Notifications] Ошибка повторной отправки: {retry_error}")

            except Exception as e:
                logger.error(f"[Notifications] Ошибка отправки уведомления: {e}")

            notification_queue.task_done()
            
        except Exception as e:
            logger.error(f"[Notifications] Ошибка в воркере очереди: {e}")
            await asyncio.sleep(1)


async def handle_user_registration(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    source_code: str = None,
    source_type: str = "direct",
    session: AsyncSession = None,
    **kwargs
):
    if NOTIFICATIONS_ENABLED != "true" or NOTIFY_NEW_USERS != "true":
        return

    try:
        source_template = SOURCE_DESCRIPTIONS.get(source_type, "❓ Неизвестный источник")
        if "{code}" in source_template and source_code:
            source = source_template.format(code=source_code)
        else:
            source = source_template.replace(" ({code})", "").replace(" (ID: {code})", "")

        name_info = ""
        if first_name or last_name:
            name_parts = []
            if first_name:
                name_parts.append(first_name)
            if last_name:
                name_parts.append(last_name)
            name_info = NAME_INFO_TEMPLATE.format(name=" ".join(name_parts))

        username_info = ""
        if username:
            username_info = USERNAME_INFO_TEMPLATE.format(username=username)

        tz = pytz.timezone(NOTIFICATION_TIMEZONE)
        time = datetime.now(tz).strftime("%H:%M:%S")

        text = NEW_USER_TEMPLATE.format(
            user_id=user_id,
            name_info=name_info,
            username_info=username_info,
            source=source,
            time=time
        )

        if NOTIFICATION_SEND_MODE == "bot":
            await send_notification_with_rate_limit(text, session, NOTIFICATION_TOPIC_NEW_USERS)
        else:
            await send_notification_to_admins(text, session, NOTIFICATION_TOPIC_NEW_USERS)
        
    except Exception as e:
        logger.error(f"[Notifications] Ошибка отправки уведомления о регистрации пользователя {user_id}: {e}")



async def handle_payment_success(
    user_id: int,
    amount: float,
    payment_system: str = None,
    status: str = "success",
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    currency: str = None,
    original_amount: float = None,
    session: AsyncSession = None,
    **kwargs
):
    if NOTIFICATIONS_ENABLED != "true" or NOTIFY_PAYMENT_SUCCESS != "true":
        return
        
    if amount < MIN_PAYMENT_AMOUNT_NOTIFY:
        return

    try:
        payment_system_name = PAYMENT_SYSTEM_NAMES.get(payment_system, payment_system or "Неизвестно")

        name_info = ""
        if first_name or last_name:
            name_parts = []
            if first_name:
                name_parts.append(first_name)
            if last_name:
                name_parts.append(last_name)
            name_info = NAME_INFO_TEMPLATE.format(name=" ".join(name_parts))

        username_info = ""
        if username:
            username_info = USERNAME_INFO_TEMPLATE.format(username=username)

        tz = pytz.timezone(NOTIFICATION_TIMEZONE)
        time = datetime.now(tz).strftime("%H:%M:%S")

        currency_info = ""
        if currency and currency != "RUB" and original_amount:
            currency_info = f" (было {original_amount} {currency})"
        elif currency and currency != "RUB":
            currency_info = f" ({currency})"

        text = PAYMENT_SUCCESS_TEMPLATE.format(
            amount=amount,
            currency_info=currency_info,
            payment_system=payment_system_name,
            user_id=user_id,
            name_info=name_info,
            username_info=username_info,
            time=time
        )

        if NOTIFICATION_SEND_MODE == "bot":
            await send_notification_with_rate_limit(text, session, NOTIFICATION_TOPIC_PAYMENTS)
        else:
            await send_notification_to_admins(text, session, NOTIFICATION_TOPIC_PAYMENTS)
        
    except Exception as e:
        logger.error(f"[Notifications] Ошибка отправки уведомления об оплате от пользователя {user_id}: {e}")


async def handle_user_message(
    user_id: int,
    message_text: str,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    session: AsyncSession = None,
    **kwargs
):
    if NOTIFICATIONS_ENABLED != "true" or NOTIFY_USER_MESSAGES != "true":
        return

    try:
        name_info = ""
        if first_name or last_name:
            name_parts = []
            if first_name:
                name_parts.append(first_name)
            if last_name:
                name_parts.append(last_name)
            name_info = NAME_INFO_TEMPLATE.format(name=" ".join(name_parts))

        username_info = ""
        if username:
            username_info = USERNAME_INFO_TEMPLATE.format(username=username)

        tz = pytz.timezone(NOTIFICATION_TIMEZONE)
        time = datetime.now(tz).strftime("%H:%M:%S")

        display_message = message_text

        text = USER_MESSAGE_TEMPLATE.format(
            user_id=user_id,
            name_info=name_info,
            username_info=username_info,
            message=display_message,
            time=time
        )

        if NOTIFICATION_SEND_MODE == "bot":
            await send_notification_with_rate_limit(text, session, NOTIFICATION_TOPIC_MESSAGES)
        else:
            await send_notification_to_admins(text, session, NOTIFICATION_TOPIC_MESSAGES)
        
    except Exception as e:
        logger.error(f"[Notifications] Ошибка отправки уведомления о сообщении от пользователя {user_id}: {e}")


async def send_notification_to_admins(text: str, session: AsyncSession, topic_id: str = None):
    from bot import bot
    
    async def send_to_admins_via_bot(bot_instance, session):
        result = await session.execute(select(Admin.tg_id))
        admin_ids = [row[0] for row in result.all()]
        
        if not admin_ids:
            logger.warning("[Notifications] В БД нет админов для отправки уведомлений")
            return
            
        for admin_id in admin_ids:
            try:
                await bot_instance.send_message(
                    chat_id=admin_id,
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            except TelegramForbiddenError:
                logger.warning(f"[Notifications] Бот заблокирован администратором {admin_id}")
            except TelegramRetryAfter as e:
                logger.warning(f"[Notifications] Лимит превышен для админа {admin_id}, ждем {e.retry_after} секунд")
                await asyncio.sleep(e.retry_after)
                try:
                    await bot_instance.send_message(
                        chat_id=admin_id,
                        text=text,
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception as retry_error:
                    logger.error(f"[Notifications] Ошибка повторной отправки администратору {admin_id}: {retry_error}")
            except TelegramBadRequest as e:
                logger.error(f"[Notifications] Ошибка отправки администратору {admin_id}: {e}")
            except Exception as e:
                logger.error(f"[Notifications] Неожиданная ошибка отправки администратору {admin_id}: {e}")
            
            await asyncio.sleep(0.1)

    async def send_to_channel_via_bot(bot_instance, channel_id, topic_id=None):
        send_kwargs = {"chat_id": channel_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}

        if topic_id and topic_id.strip():
            try:
                send_kwargs["message_thread_id"] = int(topic_id.strip())
            except ValueError:
                logger.warning(f"[Notifications] Некорректный ID треда: {topic_id}, отправляем в основной чат")
            
        try:
            await bot_instance.send_message(**send_kwargs)
        except TelegramRetryAfter as e:
            logger.warning(f"[Notifications] Лимит превышен в канале {channel_id}, ждем {e.retry_after} секунд")
            await asyncio.sleep(e.retry_after)
            await bot_instance.send_message(**send_kwargs)
        except Exception as e:
            logger.error(f"[Notifications] Не удалось отправить в канал {channel_id}: {e}")
            raise

    if not session:
        logger.error("[Notifications] Нет сессии БД для отправки уведомлений")
        return
        
    try:
        if NOTIFICATION_SEND_MODE == "default":
            await send_to_admins_via_bot(bot, session)
            
        elif NOTIFICATION_SEND_MODE == "bot":
            if not NOTIFICATION_BOT_TOKEN.strip():
                logger.error("[Notifications] NOTIFICATION_BOT_TOKEN не задан для режима 'bot', fallback на default")
                await send_to_admins_via_bot(bot, session)
                return
                
            notification_bot = Bot(token=NOTIFICATION_BOT_TOKEN)
            
            try:
                channel_id = NOTIFICATION_CHANNEL_ID.strip()
                
                if channel_id:
                    await send_to_channel_via_bot(notification_bot, channel_id, topic_id)
                else:
                    await send_to_admins_via_bot(notification_bot, session)
                    
            finally:
                await notification_bot.session.close()
        else:
            logger.error(f"[Notifications] Неизвестный режим отправки: {NOTIFICATION_SEND_MODE}, fallback на default")
            await send_to_admins_via_bot(bot, session)
            
    except Exception as e:
        logger.error(f"[Notifications] Критическая ошибка отправки уведомления: {e}")
        try:
            await send_to_admins_via_bot(bot, session)
        except Exception as fallback_error:
            logger.error(f"[Notifications] Ошибка fallback отправки: {fallback_error}")
