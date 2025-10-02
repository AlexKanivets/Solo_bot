from typing import Any
import asyncio

from hooks.hooks import register_hook
from logger import logger

from .handlers import (
    handle_user_registration,
    handle_payment_success,
    handle_user_message
)


def register_notification_hooks():
    register_hook("start_link", on_user_registered)
    register_hook("payment_success", on_payment_success)
    register_hook("user_message", on_user_message)
    logger.info("[Notifications] Хуки модуля уведомлений зарегистрированы")


async def on_user_registered(**kwargs):
    try:
        from database import check_user_exists
        
        message = kwargs.get("message")
        user_data = kwargs.get("user_data", {})
        part = kwargs.get("part", "")
        session = kwargs.get("session")
        
        if not message or not user_data or not session:
            return

        user_id = user_data.get("tg_id")
        if not user_id:
            return

        user_exists = await check_user_exists(session, user_id)
        if user_exists:
            return
            
        source_type = "direct"
        source_code = None
        
        if "partner" in part:
            source_type = "partner"
            source_code = part.replace("partner_", "").replace("partner", "")
        elif "referral" in part:
            source_type = "referral"
            source_code = part.replace("referral_", "").replace("referral", "")
        elif "utm" in part:
            source_type = "utm"
            source_code = part
        elif "gift" in part:
            source_type = "gift"
            source_code = part.replace("gift_", "").replace("gift", "")
        elif "coupon" in part:
            source_type = "coupon"
            source_code = part.replace("coupon_", "").replace("coupon", "")
        
        await handle_user_registration(
            user_id=user_id,
            username=user_data.get("username"),
            first_name=user_data.get("first_name"),
            last_name=user_data.get("last_name"),
            source_code=source_code,
            source_type=source_type,
            session=session
        )
        
    except Exception as e:
        logger.error(f"[Notifications Hook] Ошибка в on_user_registered: {e}")


async def on_payment_success(**kwargs):
    try:
        if not kwargs.get("username") and kwargs.get("session") and kwargs.get("user_id"):
            try:
                from database.models import User
                from sqlalchemy import select
                
                result = await kwargs["session"].execute(select(User).where(User.tg_id == kwargs["user_id"]))
                user = result.scalar_one_or_none()
                if user:
                    kwargs["username"] = user.username
                    kwargs["first_name"] = user.first_name
                    kwargs["last_name"] = user.last_name
            except Exception as e:
                logger.warning(f"[Notifications Hook] Не удалось получить данные пользователя: {e}")

        if not kwargs.get("payment_system") and kwargs.get("session"):
            try:
                from database.payments import get_last_payments
                
                user_id = kwargs.get("user_id")
                amount = kwargs.get("amount")
                
                if user_id and amount:
                    recent_payments = await get_last_payments(kwargs["session"], user_id, limit=5)
                    
                    for payment in recent_payments:
                        payment_amount = float(payment['amount'])
                        payment_system = payment['payment_system']
                        
                        if abs(payment_amount - amount) < 0.01:
                            kwargs["payment_system"] = payment_system
                            break
                    else:
                        logger.warning(f"[Notifications Hook] ❌ Платежная система не найдена для amount={amount}")
            except Exception as e:
                logger.warning(f"[Notifications Hook] Не удалось получить платежную систему из БД: {e}")
        
        await handle_payment_success(**kwargs)
    except Exception as e:
        logger.error(f"[Notifications Hook] Ошибка в on_payment_success: {e}")


async def on_user_message(**kwargs):
    try:
        await handle_user_message(**kwargs)
    except Exception as e:
        logger.error(f"[Notifications Hook] Ошибка в on_user_message: {e}")


register_notification_hooks()
