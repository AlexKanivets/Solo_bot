from datetime import datetime
from typing import Any
import asyncpg

import pytz

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery

from filters.admin import IsAdminFilter
from logger import logger
from utils.csv_export import export_hot_leads_csv, export_keys_csv, export_payments_csv, export_users_csv

from ..panel.keyboard import AdminPanelCallback, build_admin_back_kb
from .keyboard import build_stats_kb
from bot import bot
from config import DATABASE_URL, ADMIN_ID

router = Router()


@router.callback_query(
    AdminPanelCallback.filter(F.action == "stats"),
    IsAdminFilter(),
)
async def handle_stats(callback_query: CallbackQuery, session: Any):
    try:
        total_users = await session.fetchval("SELECT COUNT(*) FROM users")
        total_keys = await session.fetchval("SELECT COUNT(*) FROM keys")
        total_referrals = await session.fetchval("SELECT COUNT(*) FROM referrals")
        users_updated_today = await session.fetchval("SELECT COUNT(*) FROM users WHERE updated_at >= CURRENT_DATE")

        total_payments_today = int(await session.fetchval("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE created_at >= CURRENT_DATE"))
        total_payments_yesterday = int(await session.fetchval("""
            SELECT COALESCE(SUM(amount), 0) FROM payments
            WHERE created_at >= CURRENT_DATE - interval '1 day' AND created_at < CURRENT_DATE
        """))
        total_payments_week = int(await session.fetchval("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE created_at >= date_trunc('week', CURRENT_DATE)"))
        total_payments_month = int(await session.fetchval("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE created_at >= date_trunc('month', CURRENT_DATE)"))
        total_payments_last_month = int(await session.fetchval("""
            SELECT COALESCE(SUM(amount), 0) FROM payments
            WHERE created_at >= date_trunc('month', CURRENT_DATE - interval '1 month') AND created_at < date_trunc('month', CURRENT_DATE)
        """))
        total_payments_all_time = int(await session.fetchval("SELECT COALESCE(SUM(amount), 0) FROM payments"))

        registrations_today = await session.fetchval("SELECT COUNT(*) FROM users WHERE created_at >= CURRENT_DATE")
        registrations_yesterday = await session.fetchval("""
            SELECT COUNT(*) FROM users
            WHERE created_at >= CURRENT_DATE - interval '1 day' AND created_at < CURRENT_DATE
        """)
        registrations_week = await session.fetchval("SELECT COUNT(*) FROM users WHERE created_at >= date_trunc('week', CURRENT_DATE)")
        registrations_month = await session.fetchval("SELECT COUNT(*) FROM users WHERE created_at >= date_trunc('month', CURRENT_DATE)")
        registrations_last_month = await session.fetchval("""
            SELECT COUNT(*) FROM users
            WHERE created_at >= date_trunc('month', CURRENT_DATE - interval '1 month') AND created_at < date_trunc('month', CURRENT_DATE)
        """)

        active_keys = await session.fetchval("SELECT COUNT(*) FROM keys WHERE expiry_time > $1", int(datetime.utcnow().timestamp() * 1000))
        expired_keys = total_keys - active_keys

        tariffs = await session.fetch("SELECT id, name, duration_days FROM tariffs WHERE is_active = TRUE")
        tariff_map = {t["id"]: t["name"] for t in tariffs}
        durations = [(t["id"], t["name"], t["duration_days"]) for t in tariffs]

        tariff_counter: dict[str, int] = {}

        keys_with_tariffs = await session.fetch("SELECT tariff_id FROM keys WHERE tariff_id IS NOT NULL")
        for row in keys_with_tariffs:
            name = tariff_map.get(row["tariff_id"], "Неизвестно")
            tariff_counter[name] = tariff_counter.get(name, 0) + 1

        keys_without_tariffs = await session.fetch("SELECT created_at, expiry_time FROM keys WHERE tariff_id IS NULL")
        for row in keys_without_tariffs:
            duration_days = (row["expiry_time"] - row["created_at"]) / (1000 * 60 * 60 * 24)
            if durations:
                closest = min(durations, key=lambda t: abs(t[2] - duration_days))
                name = closest[1]
            else:
                name = "Неизвестно"
            tariff_counter[name] = tariff_counter.get(name, 0) + 1

        tariff_order = {t["name"]: t["id"] for t in sorted(tariffs, key=lambda t: t["id"])}
        tariff_stats_text = "\n".join(
            f"     • {name}: <b>{tariff_counter[name]}</b>"
            for name in sorted(tariff_counter.keys(), key=lambda name: tariff_order.get(name, float('inf')))
        )

        if not tariff_stats_text:
            tariff_stats_text = "     • Нет активных тарифов"


        hot_leads_count = await session.fetchval("""
            SELECT COUNT(DISTINCT u.tg_id)
            FROM users u
            JOIN payments p ON u.tg_id = p.tg_id
            LEFT JOIN keys k ON u.tg_id = k.tg_id
            WHERE p.status = 'success' AND k.tg_id IS NULL
        """)

        trial_only_count = await session.fetchval("""
            SELECT COUNT(DISTINCT k.tg_id)
            FROM keys k
            LEFT JOIN tariffs t ON k.tariff_id = t.id
            LEFT JOIN payments p ON k.tg_id = p.tg_id
            WHERE p.id IS NULL
        """)

        moscow_tz = pytz.timezone("Europe/Moscow")
        update_time = datetime.now(moscow_tz).strftime("%d.%m.%y %H:%M:%S")

        stats_message = (
            "📊 <b>Статистика проекта</b>\n\n"
            "👤 <b>Пользователи:</b>\n"
            f"├ 🗓️ За день: <b>{registrations_today}</b>\n"
            f"├ 🗓️ Вчера: <b>{registrations_yesterday}</b>\n"
            f"├ 📆 За неделю: <b>{registrations_week}</b>\n"
            f"├ 🗓️ За месяц: <b>{registrations_month}</b>\n"
            f"├ 📅 За прошлый месяц: <b>{registrations_last_month}</b>\n"
            f"└ 🌐 Всего: <b>{total_users}</b>\n\n"
            "💡 <b>Активность:</b>\n"
            f"└ 👥 Сегодня были активны: <b>{users_updated_today}</b>\n\n"
            "🤝 <b>Реферальная система:</b>\n"
            f"└ 👥 Всего привлечено: <b>{total_referrals}</b>\n\n"
            "🔐 <b>Подписки:</b>\n"
            f"├ 📦 Всего сгенерировано: <b>{total_keys}</b>\n"
            f"├ ✅ Активных: <b>{active_keys}</b>\n"
            f"├ ❌ Просроченных: <b>{expired_keys}</b>\n"
            f"├ 🎁 Только триал: <b>{trial_only_count}</b>\n"
            f"└ 📋 По тарифам:\n{tariff_stats_text}\n\n"
            "💰 <b>Финансы:</b>\n"
            f"├ 📅 За день: <b>{total_payments_today} ₽</b>\n"
            f"├ 📆 Вчера: <b>{total_payments_yesterday} ₽</b>\n"
            f"├ 📆 За неделю: <b>{total_payments_week} ₽</b>\n"
            f"├ 📆 За месяц: <b>{total_payments_month} ₽</b>\n"
            f"├ 📆 Прошлый месяц: <b>{total_payments_last_month} ₽</b>\n"
            f"└ 🏦 Всего: <b>{total_payments_all_time} ₽</b>\n\n"
            f"🔥 <b>Горящие лиды</b>: <b>{hot_leads_count}</b> (платили, но не продлили)\n\n"
            f"⏱️ <i>Последнее обновление:</i> <code>{update_time}</code>"
        )

        await callback_query.message.edit_text(text=stats_message, reply_markup=build_stats_kb())

    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Error in user_stats_menu: {e}")
    except Exception as e:
        logger.error(f"Error in user_stats_menu: {e}")
        await callback_query.answer("\u041fроизошла ошибка при получении статистики", show_alert=True)


@router.callback_query(
    AdminPanelCallback.filter(F.action == "stats_export_users_csv"),
    IsAdminFilter(),
)
async def handle_export_users_csv(callback_query: CallbackQuery, session: Any):
    kb = build_admin_back_kb("stats")
    try:
        export = await export_users_csv(session)
        await callback_query.message.answer_document(document=export, caption="📥 Экспорт пользователей в CSV")
    except Exception as e:
        logger.error(f"Ошибка при экспорте пользователей в CSV: {e}")
        await callback_query.message.edit_text(text=f"❗ Произошла ошибка при экспорте: {e}", reply_markup=kb)


@router.callback_query(
    AdminPanelCallback.filter(F.action == "stats_export_payments_csv"),
    IsAdminFilter(),
)
async def handle_export_payments_csv(callback_query: CallbackQuery, session: Any):
    kb = build_admin_back_kb("stats")
    try:
        export = await export_payments_csv(session)
        await callback_query.message.answer_document(document=export, caption="📥 Экспорт платежей в CSV")
    except Exception as e:
        logger.error(f"Ошибка при экспорте платежей в CSV: {e}")
        await callback_query.message.edit_text(text=f"❗ Произошла ошибка при экспорте: {e}", reply_markup=kb)


@router.callback_query(
    AdminPanelCallback.filter(F.action == "stats_export_hot_leads_csv"),
    IsAdminFilter(),
)
async def handle_export_hot_leads_csv(callback_query: CallbackQuery, session: Any):
    kb = build_admin_back_kb("stats")
    try:
        export = await export_hot_leads_csv(session)
        await callback_query.message.answer_document(document=export, caption="📥 Экспорт горящих лидов")
    except Exception as e:
        logger.error(f"Ошибка при экспорте 'горящих лидов': {e}")
        await callback_query.message.edit_text(text=f"❗ Произошла ошибка при экспорте: {e}", reply_markup=kb)


@router.callback_query(
    AdminPanelCallback.filter(F.action == "stats_export_keys_csv"),
    IsAdminFilter(),
)
async def handle_export_keys_csv(callback_query: CallbackQuery, session: Any):
    kb = build_admin_back_kb("stats")
    try:
        export = await export_keys_csv(session)
        await callback_query.message.answer_document(document=export, caption="📥 Экспорт подписок в CSV")
    except Exception as e:
        logger.error(f"Ошибка при экспорте подписок в CSV: {e}")
        await callback_query.message.edit_text(text=f"❗ Произошла ошибка при экспорте: {e}", reply_markup=kb)


async def send_daily_stats_report():
    try:
        conn = await asyncpg.connect(DATABASE_URL)

        registrations_today = await conn.fetchval("SELECT COUNT(*) FROM users WHERE created_at >= CURRENT_DATE")
        payments_today = int(await conn.fetchval("SELECT COALESCE(SUM(amount), 0) FROM payments WHERE created_at >= CURRENT_DATE"))
        active_keys = await conn.fetchval("SELECT COUNT(*) FROM keys WHERE expiry_time > $1", int(datetime.utcnow().timestamp() * 1000))

        update_time = datetime.now(pytz.timezone("Europe/Moscow")).strftime("%d.%m.%y %H:%M")

        text = (
            "🗓️ <b>Сводка за день</b>\n\n"
            f"👤 Новых пользователей: <b>{registrations_today}</b>\n"
            f"💰 Оплачено: <b>{payments_today} ₽</b>\n"
            f"🔐 Активных ключей: <b>{active_keys}</b>\n\n"
            f"⏱️ <i>{update_time} МСК</i>"
        )

        for admin_id in ADMIN_ID:
            await bot.send_message(admin_id, text)

        await conn.close()

    except Exception as e:
        logger.error(f"[Stats] Ошибка при отправке ежедневной статистики: {e}")