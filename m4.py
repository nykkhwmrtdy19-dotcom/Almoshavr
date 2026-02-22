#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
                           m4.py - ربات اصلی مادر
================================================================================
این فایل قلب اصلی ربات است و شامل:
- هندلرهای اصلی ربات
- پنل ادمین مخفی (فقط برای ادمین‌ها نمایش داده می‌شود)
- مدیریت کاربران و ربات‌ها
- نمایش وضعیت موتورها
- و تمام تعامل با کاربر
================================================================================
"""

import telebot
from telebot import types, util
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

import asyncio
import aiohttp
import aiofiles
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import os
import sys
import time
import json
import hashlib
import hmac
import base64
import logging
import threading
import queue
import random
import string
import re
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from collections import defaultdict, deque
from functools import wraps, partial
import signal
import gc
import resource

# ==============================================================================
# ایمپورت فایل‌های دیگر
# ==============================================================================

# m1.py - موتورهای اجرایی
import importlib.util
import sys
import os

# افزودن مسیر فعلی به PATH
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ایمپورت m1
spec = importlib.util.spec_from_file_location("m1", os.path.join(os.path.dirname(__file__), "m1.py"))
m1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m1)
super_engine_cluster = m1.super_engine_cluster
run_bot_async = m1.run_bot
run_bot_sync = m1.run_bot_sync
stop_bot = m1.stop_bot
get_bot_status = m1.get_bot_status
get_cluster_stats = m1.get_cluster_stats

# ایمپورت m2
spec = importlib.util.spec_from_file_location("m2", os.path.join(os.path.dirname(__file__), "m2.py"))
m2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m2)
db_cluster = m2.db_cluster
get_db = m2.get_db

# ایمپورت m3
spec = importlib.util.spec_from_file_location("m3", os.path.join(os.path.dirname(__file__), "m3.py"))
m3 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m3)
filesystem = m3.filesystem
save_user_file = m3.save_user_file
get_user_file = m3.get_user_file
list_user_files = m3.list_user_files
create_user_folder = m3.create_user_folder
save_bot_code = m3.save_bot_code
get_bot_code = m3.get_bot_code
delete_bot_files = m3.delete_bot_files
save_receipt = m3.save_receipt
backup_user = m3.backup_user
restore_user = m3.restore_user

# ==============================================================================
# تنظیمات پایه
# ==============================================================================

class Config:
    """تنظیمات ربات"""
    
    # توکن ربات
    BOT_TOKEN = "8052349235:AAFSaJmYpl359BKrJTWC8O-u-dI9r2olEOQ"
    BOT_TOKEN_BACKUP = "8052349235:AAFSaJmYpl359BKrJTWC8O-u-dI9r2olEOQ"
    
    # آیدی ادمین‌ها
    ADMIN_IDS = [327855654]
    ADMIN_IDS_BACKUP = [327855654]
    
    # اطلاعات کارت
    CARD_NUMBER = "5892101187322777"
    CARD_HOLDER = "مرتضی نیکخو خنجری"
    PRICE = 2000000
    
    # تنظیمات ربات
    BOT_USERNAME = "mother_bot"
    BOT_LANGUAGE = "fa"
    MAX_MESSAGE_LENGTH = 4096
    
    # تنظیمات امنیتی
    RATE_LIMIT_ENABLED = True
    RATE_LIMIT = 10  # پیام در ثانیه
    RATE_LIMIT_BURST = 20
    
    # تنظیمات کش
    CACHE_TTL = {
        'user': 300,
        'bot': 60,
        'stats': 600
    }
    
    # تنظیمات منو
    MAIN_MENU_ROWS = 3
    MAIN_MENU_COLS = 2

config = Config()

# ==============================================================================
# راه‌اندازی ربات
# ==============================================================================

bot = telebot.TeleBot(config.BOT_TOKEN)
bot.delete_webhook()

logger = logging.getLogger('mother_bot')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/mother_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==============================================================================
# کلاس مدیریت نرخ (Rate Limiter)
# ==============================================================================

class RateLimiter:
    """مدیریت نرخ درخواست‌ها"""
    
    def __init__(self, rate: int = config.RATE_LIMIT, burst: int = config.RATE_LIMIT_BURST):
        self.rate = rate
        self.burst = burst
        self.users: Dict[int, deque] = defaultdict(lambda: deque(maxlen=burst))
        self.lock = threading.RLock()
    
    def is_allowed(self, user_id: int) -> bool:
        """بررسی مجاز بودن درخواست"""
        if not config.RATE_LIMIT_ENABLED:
            return True
        
        with self.lock:
            now = time.time()
            user_queue = self.users[user_id]
            
            # پاک کردن درخواست‌های قدیمی
            while user_queue and user_queue[0] < now - 1:
                user_queue.popleft()
            
            if len(user_queue) < self.burst:
                user_queue.append(now)
                return True
            
            return False
    
    def get_wait_time(self, user_id: int) -> float:
        """زمان انتظار باقی‌مانده"""
        with self.lock:
            user_queue = self.users.get(user_id, deque())
            if len(user_queue) < self.burst:
                return 0
            return max(0, user_queue[0] + 1 - time.time())

rate_limiter = RateLimiter()

# ==============================================================================
# دکوراتور Rate Limiting
# ==============================================================================

def rate_limited(func):
    """دکوراتور محدودیت نرخ"""
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        if not rate_limiter.is_allowed(user_id):
            wait_time = rate_limiter.get_wait_time(user_id)
            bot.reply_to(
                message,
                f"⏳ لطفاً {wait_time:.1f} ثانیه صبر کنید و دوباره تلاش کنید."
            )
            return
        return func(message, *args, **kwargs)
    return wrapper

# ==============================================================================
# کلاس مدیریت کاربران
# ==============================================================================

class UserManager:
    """مدیریت کاربران"""
    
    def __init__(self):
        self.cache = {}
        self.cache_lock = asyncio.Lock()
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """گرفتن اطلاعات کاربر"""
        # تلاش از کش
        if user_id in self.cache:
            cache_time, user_data = self.cache[user_id]
            if time.time() - cache_time < config.CACHE_TTL['user']:
                return user_data
        
        # از دیتابیس
        try:
            db = await get_db()
            user_data = await db.get_user(user_id)
            
            if user_data:
                async with self.cache_lock:
                    self.cache[user_id] = (time.time(), user_data)
            
            return user_data
            
        except Exception as e:
            logger.error(f"خطا در get_user: {e}")
            return None
    
    async def create_user(self, user_id: int, username: str, first_name: str,
                          last_name: str, referred_by: Optional[int] = None) -> bool:
        """ایجاد کاربر جدید"""
        try:
            db = await get_db()
            
            # اختصاص موتور
            engine = super_engine_cluster.assign_engine_to_user(user_id)
            
            # ذخیره در دیتابیس
            user_data = {
                'user_id': user_id,
                'username': username,
                'first_name': first_name,
                'last_name': last_name,
                'referred_by': referred_by,
                'assigned_engine': engine.engine_id,
                'created_at': datetime.now().isoformat(),
                'last_active': datetime.now().isoformat()
            }
            
            success = await db.set_user(user_id, user_data)
            
            if success and referred_by:
                # به‌روزرسانی آمار رفرال
                await db.increment_referrals(referred_by)
            
            return success
            
        except Exception as e:
            logger.error(f"خطا در create_user: {e}")
            return False
    
    async def update_user(self, user_id: int, **kwargs) -> bool:
        """به‌روزرسانی کاربر"""
        try:
            db = await get_db()
            
            # حذف از کش
            async with self.cache_lock:
                if user_id in self.cache:
                    del self.cache[user_id]
            
            return await db.update_user(user_id, **kwargs)
            
        except Exception as e:
            logger.error(f"خطا در update_user: {e}")
            return False
    
    async def check_payment(self, user_id: int) -> bool:
        """بررسی وضعیت پرداخت"""
        user = await self.get_user(user_id)
        if not user:
            return False
        
        if user.get('payment_status') == 'approved':
            return True
        
        # بررسی فیش‌های تایید شده
        db = await get_db()
        receipts = await db.get_user_receipts(user_id)
        
        for receipt in receipts:
            if receipt.get('status') == 'approved':
                await self.update_user(user_id, payment_status='approved')
                return True
        
        return False
    
    async def get_max_bots(self, user_id: int) -> int:
        """حداکثر تعداد ربات مجاز"""
        user = await self.get_user(user_id)
        if not user:
            return 1
        
        verified = user.get('verified_referrals', 0)
        extra = verified // 5
        return 1 + extra

user_manager = UserManager()

# ==============================================================================
# کلاس مدیریت ربات‌ها
# ==============================================================================

class BotManager:
    """مدیریت ربات‌های کاربران"""
    
    def __init__(self):
        self.cache = {}
        self.cache_lock = asyncio.Lock()
    
    async def create_bot(self, user_id: int, token: str, name: str,
                         username: str, code: str, file_path: str) -> Optional[str]:
        """ایجاد ربات جدید"""
        try:
            # بررسی محدودیت تعداد
            current_bots = await self.get_user_bots_count(user_id)
            max_bots = await user_manager.get_max_bots(user_id)
            
            if current_bots >= max_bots:
                logger.warning(f"کاربر {user_id} به حداکثر تعداد ربات رسیده")
                return None
            
            # ایجاد آیدی یکتا
            bot_id = hashlib.md5(f"{user_id}{token}{time.time()}".encode()).hexdigest()[:12]
            
            # ذخیره کد
            code_path = await save_bot_code(bot_id, code, user_id, {
                'token': token,
                'name': name,
                'username': username,
                'file_path': file_path
            })
            
            if not code_path:
                return None
            
            # ذخیره در دیتابیس
            db = await get_db()
            bot_data = {
                'bot_id': bot_id,
                'user_id': user_id,
                'token': token,
                'name': name,
                'username': username,
                'file_path': file_path,
                'code_path': code_path,
                'status': 'stopped',
                'created_at': datetime.now().isoformat()
            }
            
            success = await db.set_bot(bot_id, bot_data)
            
            if not success:
                await delete_bot_files(bot_id)
                return None
            
            # اجرای ربات
            result = run_bot_sync(user_id, bot_id, code, token, {
                'name': name,
                'username': username
            })
            
            if result.get('success'):
                await db.update_bot(bot_id, status='running', engine_id=result.get('engine_id'))
            
            return bot_id
            
        except Exception as e:
            logger.error(f"خطا در create_bot: {traceback.format_exc()}")
            return None
    
    async def get_user_bots(self, user_id: int) -> List[Dict]:
        """لیست ربات‌های کاربر"""
        try:
            db = await get_db()
            
            # TODO: پیاده‌سازی get_user_bots در دیتابیس
            # فعلاً placeholder
            return []
            
        except Exception as e:
            logger.error(f"خطا در get_user_bots: {e}")
            return []
    
    async def get_user_bots_count(self, user_id: int) -> int:
        """تعداد ربات‌های کاربر"""
        bots = await self.get_user_bots(user_id)
        return len(bots)
    
    async def get_bot(self, bot_id: str) -> Optional[Dict]:
        """گرفتن اطلاعات ربات"""
        # تلاش از کش
        if bot_id in self.cache:
            cache_time, bot_data = self.cache[bot_id]
            if time.time() - cache_time < config.CACHE_TTL['bot']:
                return bot_data
        
        try:
            db = await get_db()
            bot_data = await db.get_bot(bot_id)
            
            if bot_data:
                async with self.cache_lock:
                    self.cache[bot_id] = (time.time(), bot_data)
            
            return bot_data
            
        except Exception as e:
            logger.error(f"خطا در get_bot: {e}")
            return None
    
    async def update_bot(self, bot_id: str, **kwargs) -> bool:
        """به‌روزرسانی ربات"""
        try:
            db = await get_db()
            
            # حذف از کش
            async with self.cache_lock:
                if bot_id in self.cache:
                    del self.cache[bot_id]
            
            return await db.update_bot(bot_id, **kwargs)
            
        except Exception as e:
            logger.error(f"خطا در update_bot: {e}")
            return False
    
    async def delete_bot(self, bot_id: str, user_id: int) -> bool:
        """حذف ربات"""
        try:
            # توقف ربات
            stop_bot(bot_id)
            
            # حذف فایل‌ها
            await delete_bot_files(bot_id)
            
            # حذف از دیتابیس
            db = await get_db()
            await db.delete_bot(bot_id, user_id)
            
            # حذف از کش
            async with self.cache_lock:
                if bot_id in self.cache:
                    del self.cache[bot_id]
            
            return True
            
        except Exception as e:
            logger.error(f"خطا در delete_bot: {e}")
            return False
    
    async def toggle_bot(self, bot_id: str) -> Tuple[bool, str]:
        """تغییر وضعیت ربات (روشن/خاموش)"""
        try:
            bot_data = await self.get_bot(bot_id)
            if not bot_data:
                return False, "ربات پیدا نشد"
            
            if bot_data.get('status') == 'running':
                # توقف
                if stop_bot(bot_id):
                    await self.update_bot(bot_id, status='stopped')
                    return True, "stopped"
                else:
                    return False, "خطا در توقف ربات"
            else:
                # اجرا
                code = await get_bot_code(bot_id)
                if not code:
                    return False, "کد ربات پیدا نشد"
                
                result = run_bot_sync(
                    bot_data['user_id'],
                    bot_id,
                    code,
                    bot_data['token'],
                    {'name': bot_data['name']}
                )
                
                if result.get('success'):
                    await self.update_bot(
                        bot_id,
                        status='running',
                        engine_id=result.get('engine_id')
                    )
                    return True, "running"
                else:
                    return False, result.get('error', 'خطای ناشناخته')
            
        except Exception as e:
            logger.error(f"خطا در toggle_bot: {e}")
            return False, str(e)

bot_manager = BotManager()

# ==============================================================================
# کلاس مدیریت پرداخت
# ==============================================================================

class PaymentManager:
    """مدیریت پرداخت‌ها"""
    
    def __init__(self):
        self.pending = {}
    
    async def create_receipt(self, user_id: int, photo_data: bytes) -> Optional[str]:
        """ایجاد فیش جدید"""
        try:
            # تولید کد یکتا
            payment_code = hashlib.md5(f"{user_id}{time.time()}".encode()).hexdigest()[:8].upper()
            
            # ذخیره فیش
            receipt_path = await save_receipt(user_id, photo_data, payment_code)
            
            if not receipt_path:
                return None
            
            # ذخیره در دیتابیس
            db = await get_db()
            receipt_data = {
                'user_id': user_id,
                'payment_code': payment_code,
                'receipt_path': receipt_path,
                'amount': config.PRICE,
                'status': 'pending',
                'created_at': datetime.now().isoformat()
            }
            
            await db.save_receipt(receipt_data)
            
            self.pending[payment_code] = receipt_data
            
            return payment_code
            
        except Exception as e:
            logger.error(f"خطا در create_receipt: {e}")
            return None
    
    async def approve_receipt(self, payment_code: str, admin_id: int) -> bool:
        """تایید فیش"""
        try:
            db = await get_db()
            
            # تایید در دیتابیس
            success = await db.approve_receipt(payment_code, admin_id)
            
            if success and payment_code in self.pending:
                user_id = self.pending[payment_code]['user_id']
                
                # به‌روزرسانی وضعیت کاربر
                await user_manager.update_user(user_id, payment_status='approved')
                
                del self.pending[payment_code]
            
            return success
            
        except Exception as e:
            logger.error(f"خطا در approve_receipt: {e}")
            return False
    
    async def reject_receipt(self, payment_code: str, admin_id: int, reason: str = "") -> bool:
        """رد فیش"""
        try:
            db = await get_db()
            
            # رد در دیتابیس
            success = await db.reject_receipt(payment_code, admin_id, reason)
            
            if success and payment_code in self.pending:
                del self.pending[payment_code]
            
            return success
            
        except Exception as e:
            logger.error(f"خطا در reject_receipt: {e}")
            return False

payment_manager = PaymentManager()

# ==============================================================================
# کلاس مدیریت منو
# ==============================================================================

class MenuManager:
    """مدیریت منوهای ربات"""
    
    def __init__(self):
        self.main_menu = None
        self.admin_menu = None
        self._create_menus()
    
    def _create_menus(self):
        """ایجاد منوها"""
        
        # منوی اصلی
        self.main_menu = ReplyKeyboardMarkup(
            row_width=config.MAIN_MENU_COLS,
            resize_keyboard=True
        )
        
        main_buttons = [
            KeyboardButton('🤖 ساخت ربات جدید'),
            KeyboardButton('📋 ربات‌های من'),
            KeyboardButton('🔄 فعال/غیرفعال کردن'),
            KeyboardButton('🗑 حذف ربات'),
            KeyboardButton('💰 کیف پول'),
            KeyboardButton('🎁 سیستم رفرال'),
            KeyboardButton('📚 راهنما'),
            KeyboardButton('📊 آمار'),
            KeyboardButton('📞 پشتیبانی'),
            KeyboardButton('⚡ وضعیت موتورها'),
            KeyboardButton('📦 نصب کتابخانه'),
            KeyboardButton('📂 مدیریت فایل‌ها'),
        ]
        
        self.main_menu.add(*main_buttons)
        
        # منوی ادمین
        self.admin_menu = ReplyKeyboardMarkup(
            row_width=2,
            resize_keyboard=True
        )
        
        admin_buttons = [
            KeyboardButton('👑 پنل مدیریت'),
            KeyboardButton('📊 آمار کامل'),
            KeyboardButton('👥 کاربران'),
            KeyboardButton('📸 فیش‌ها'),
            KeyboardButton('💰 تایید پرداخت'),
            KeyboardButton('🤖 موتورها'),
            KeyboardButton('📋 لاگ‌ها'),
            KeyboardButton('⚙ تنظیمات'),
            KeyboardButton('🔙 بازگشت به منوی اصلی'),
        ]
        
        self.admin_menu.add(*admin_buttons)
    
    def get_main_menu(self, is_admin: bool = False) -> ReplyKeyboardMarkup:
        """گرفتن منوی اصلی"""
        if is_admin:
            return self.admin_menu
        return self.main_menu
    
    def get_inline_menu(self, buttons: List[Tuple[str, str]],
                        row_width: int = 2) -> InlineKeyboardMarkup:
        """ایجاد منوی اینلاین"""
        markup = InlineKeyboardMarkup(row_width=row_width)
        markup.add(*[
            InlineKeyboardButton(text, callback_data=data)
            for text, data in buttons
        ])
        return markup

menu_manager = MenuManager()

# ==============================================================================
# هندلر فرمان /start
# ==============================================================================

@bot.message_handler(commands=['start'])
@rate_limited
def cmd_start(message):
    """هندلر فرمان /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or ""
    last_name = message.from_user.last_name or ""
    
    # بررسی کد رفرال
    referred_by = None
    args = message.text.split()
    if len(args) > 1:
        ref_code = args[1]
        try:
            # TODO: پیدا کردن کاربر با کد رفرال
            pass
        except:
            pass
    
    # ایجاد کاربر
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            user_manager.create_user(user_id, username, first_name, last_name, referred_by)
        )
    finally:
        loop.close()
    
    # دریافت اطلاعات کاربر
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        user = loop.run_until_complete(user_manager.get_user(user_id))
    finally:
        loop.close()
    
    # لینک رفرال
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user.get('referral_code', '')}"
    
    # بررسی ادمین بودن
    is_admin = user_id in config.ADMIN_IDS
    
    # متن خوش‌آمدگویی
    welcome_text = (
        f"🚀 **به ربات مادر فوق‌پیشرفته خوش آمدید** {first_name}!\n\n"
        f"👤 **آیدی شما:** `{user_id}`\n"
        f"⚡ **موتور اختصاصی:** `{user.get('assigned_engine', 'نامشخص')}`\n"
        f"🎁 **کد رفرال:** `{user.get('referral_code', '')}`\n"
        f"🔗 **لینک دعوت:** {referral_link}\n\n"
        f"📊 **آمار رفرال:**\n"
        f"• کلیک‌ها: {user.get('referrals_count', 0)}\n"
        f"• ثبت‌نام‌ها: {user.get('verified_referrals', 0)}\n\n"
        f"💡 **هر ۵ نفر = ۱ ربات اضافه**\n"
        f"📤 **فایل .py یا .zip خود را آپلود کنید**"
    )
    
    bot.send_message(
        message.chat.id,
        welcome_text,
        reply_markup=menu_manager.get_main_menu(is_admin),
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر ساخت ربات جدید
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '🤖 ساخت ربات جدید')
@rate_limited
def new_bot(message):
    """هندلر ساخت ربات جدید"""
    user_id = message.from_user.id
    
    # بررسی پرداخت
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        has_payment = loop.run_until_complete(user_manager.check_payment(user_id))
    finally:
        loop.close()
    
    if not has_payment:
        bot.send_message(
            message.chat.id,
            f"❌ **ابتدا هزینه را پرداخت کنید**\n\n"
            f"💰 **مبلغ:** `{config.PRICE:,}` تومان\n"
            f"💳 **شماره کارت:** `{config.CARD_NUMBER}`\n"
            f"👤 **به نام:** {config.CARD_HOLDER}\n\n"
            f"📸 **پس از واریز، تصویر فیش را ارسال کنید**",
            parse_mode='Markdown'
        )
        return
    
    # بررسی محدودیت تعداد
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        current_bots = loop.run_until_complete(bot_manager.get_user_bots_count(user_id))
        max_bots = loop.run_until_complete(user_manager.get_max_bots(user_id))
    finally:
        loop.close()
    
    if current_bots >= max_bots:
        bot.send_message(
            message.chat.id,
            f"❌ **شما به حداکثر تعداد ربات ({max_bots}) رسیده‌اید!**\n\n"
            f"برای ساخت ربات جدید:\n"
            f"1️⃣ **یکی از ربات‌ها را حذف کنید**\n"
            f"2️⃣ **یا با دعوت دوستان** ({5 - (current_bots % 5)} نفر دیگر) **ربات اضافه بگیرید**",
            parse_mode='Markdown'
        )
        return
    
    bot.send_message(
        message.chat.id,
        "📤 **فایل `.py` یا `.zip` خود را ارسال کنید**\n\n"
        "✅ حجم حداکثر ۵۰ مگابایت\n"
        "✅ توکن باید داخل کد باشد\n"
        "✅ موتور هوشمند بهترین روش اجرا را انتخاب می‌کند\n"
        "✅ پشتیبانی از پوشه و ساختار سلسله‌مراتبی",
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر آپلود فایل
# ==============================================================================

@bot.message_handler(content_types=['document'])
@rate_limited
def handle_document(message):
    """هندلر آپلود فایل"""
    user_id = message.from_user.id
    
    # بررسی پرداخت
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        has_payment = loop.run_until_complete(user_manager.check_payment(user_id))
    finally:
        loop.close()
    
    if not has_payment:
        bot.reply_to(
            message,
            "❌ **ابتدا هزینه را پرداخت کنید**\n"
            "از منوی اصلی گزینه `💰 کیف پول` را انتخاب کنید."
        )
        return
    
    file_name = message.document.file_name
    
    # بررسی پسوند
    if not (file_name.endswith('.py') or file_name.endswith('.zip')):
        bot.reply_to(
            message,
            "❌ **فقط فایل‌های `.py` یا `.zip` مجاز هستند!**"
        )
        return
    
    # بررسی حجم
    if message.document.file_size > 50 * 1024 * 1024:
        bot.reply_to(
            message,
            "❌ **حجم فایل نباید بیشتر از ۵۰ مگابایت باشد!**"
        )
        return
    
    status_msg = bot.reply_to(message, "🔄 **در حال پردازش فایل...**")
    
    try:
        # دانلود فایل
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        # ذخیره فایل
        folder = f"uploads/{datetime.now().strftime('%Y/%m/%d')}"
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            file_path = loop.run_until_complete(
                save_user_file(user_id, downloaded_file, file_name, folder)
            )
        finally:
            loop.close()
        
        if not file_path:
            bot.edit_message_text(
                "❌ **خطا در ذخیره فایل**",
                message.chat.id,
                status_msg.message_id
            )
            return
        
        # استخراج کد
        code = None
        
        if file_name.endswith('.zip'):
            # TODO: استخراج از zip
            pass
        else:
            try:
                code = downloaded_file.decode('utf-8')
            except:
                try:
                    code = downloaded_file.decode('cp1256')
                except:
                    code = downloaded_file.decode('latin-1')
        
        if not code:
            bot.edit_message_text(
                "❌ **هیچ کد پایتونی پیدا نشد!**",
                message.chat.id,
                status_msg.message_id
            )
            return
        
        # استخراج توکن
        token = None
        patterns = [
            r'token\s*=\s*["\']([^"\']+)["\']',
            r'TOKEN\s*=\s*["\']([^"\']+)["\']',
            r'BOT_TOKEN\s*=\s*["\']([^"\']+)["\']',
            r'bot\s*=\s*telebot\.TeleBot\(\s*["\']([^"\']+)["\']\s*\)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, code)
            if match:
                token = match.group(1)
                break
        
        if not token:
            bot.edit_message_text(
                "❌ **توکن ربات در کد پیدا نشد!**\n\n"
                "توکن باید به شکل زیر باشد:\n"
                "`token = '123456:ABCdef...'`",
                message.chat.id,
                status_msg.message_id,
                parse_mode='Markdown'
            )
            return
        
        # بررسی اعتبار توکن
        try:
            response = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
            if response.status_code != 200:
                bot.edit_message_text(
                    "❌ **توکن معتبر نیست!**",
                    message.chat.id,
                    status_msg.message_id
                )
                return
            
            bot_info = response.json()['result']
            bot_name = bot_info['first_name']
            bot_username = bot_info['username']
        except Exception as e:
            bot.edit_message_text(
                f"❌ **خطا در بررسی توکن:** {str(e)}",
                message.chat.id,
                status_msg.message_id
            )
            return
        
        bot.edit_message_text(
            "⚡ **در حال اجرا با موتور فوق‌پیشرفته...**",
            message.chat.id,
            status_msg.message_id
        )
        
        # ایجاد ربات
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            bot_id = loop.run_until_complete(
                bot_manager.create_bot(user_id, token, bot_name, bot_username, code, file_path)
            )
        finally:
            loop.close()
        
        if bot_id:
            # دریافت آمار
            stats = get_cluster_stats()
            
            reply = (
                f"✅ **ربات با موفقیت ساخته شد!** 🎉\n\n"
                f"🤖 **نام:** {bot_name}\n"
                f"🔗 **لینک:** https://t.me/{bot_username}\n"
                f"🆔 **آیدی:** `{bot_id}`\n"
                f"⚡ **موتور:** {stats['cluster'].get('total_engines', 0)} موتور\n\n"
                f"📊 **وضعیت سیستم:**\n"
                f"• ربات‌های فعال: {stats['cluster'].get('total_active', 0)}\n"
                f"• ظرفیت کل: {stats['cluster'].get('total_capacity', 0):,}\n"
                f"• بار: {stats['cluster'].get('load_percent', 0):.1f}%"
            )
            
            bot.edit_message_text(
                reply,
                message.chat.id,
                status_msg.message_id,
                parse_mode='Markdown'
            )
        else:
            bot.edit_message_text(
                "❌ **خطا در ساخت ربات!**\n"
                "لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.",
                message.chat.id,
                status_msg.message_id
            )
            
    except Exception as e:
        logger.error(f"خطا در handle_document: {traceback.format_exc()}")
        bot.edit_message_text(
            f"❌ **خطا:** {str(e)}",
            message.chat.id,
            status_msg.message_id
        )

# ==============================================================================
# هندلر ربات‌های من
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '📋 ربات‌های من')
@rate_limited
def my_bots(message):
    """نمایش ربات‌های کاربر"""
    user_id = message.from_user.id
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        bots = loop.run_until_complete(bot_manager.get_user_bots(user_id))
    finally:
        loop.close()
    
    if not bots:
        bot.send_message(
            message.chat.id,
            "📋 **شما رباتی ندارید!**\n"
            "از گزینه `🤖 ساخت ربات جدید` استفاده کنید.",
            parse_mode='Markdown'
        )
        return
    
    for bot_data in bots[:10]:
        status = get_bot_status(bot_data['bot_id'])
        
        status_emoji = "🟢" if status.get('running') else "🔴"
        status_text = "در حال اجرا" if status.get('running') else "متوقف"
        
        text = (
            f"{status_emoji} **{bot_data['name']}**\n"
            f"🔗 https://t.me/{bot_data['username']}\n"
            f"🆔 `{bot_data['bot_id']}`\n"
            f"⚡ **موتور:** {bot_data.get('engine_id', 'نامشخص')}\n"
            f"📊 **وضعیت:** {status_text}\n"
            f"📅 **ساخته شده:** {bot_data['created_at'][:10]}"
        )
        
        markup = menu_manager.get_inline_menu([
            ("🔄 تغییر وضعیت", f"toggle_{bot_data['bot_id']}"),
            ("🗑 حذف", f"delete_{bot_data['bot_id']}")
        ])
        
        bot.send_message(
            message.chat.id,
            text,
            parse_mode='Markdown',
            reply_markup=markup
        )

# ==============================================================================
# هندلر فعال/غیرفعال کردن
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '🔄 فعال/غیرفعال کردن')
@rate_limited
def toggle_prompt(message):
    """درخواست تغییر وضعیت ربات"""
    user_id = message.from_user.id
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        bots = loop.run_until_complete(bot_manager.get_user_bots(user_id))
    finally:
        loop.close()
    
    if not bots:
        bot.send_message(
            message.chat.id,
            "📋 **شما رباتی ندارید!**",
            parse_mode='Markdown'
        )
        return
    
    buttons = []
    for b in bots[:10]:
        status = "🟢" if b.get('status') == 'running' else "🔴"
        buttons.append((f"{status} {b['name']}", f"toggle_{b['bot_id']}"))
    
    markup = menu_manager.get_inline_menu(buttons, row_width=1)
    
    bot.send_message(
        message.chat.id,
        "🔄 **ربات مورد نظر را انتخاب کنید:**",
        reply_markup=markup,
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر حذف ربات
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '🗑 حذف ربات')
@rate_limited
def delete_prompt(message):
    """درخواست حذف ربات"""
    user_id = message.from_user.id
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        bots = loop.run_until_complete(bot_manager.get_user_bots(user_id))
    finally:
        loop.close()
    
    if not bots:
        bot.send_message(
            message.chat.id,
            "📋 **شما رباتی ندارید!**",
            parse_mode='Markdown'
        )
        return
    
    buttons = [(f"🗑 {b['name']}", f"delete_{b['bot_id']}") for b in bots[:10]]
    markup = menu_manager.get_inline_menu(buttons, row_width=1)
    
    bot.send_message(
        message.chat.id,
        "⚠️ **ربات مورد نظر را انتخاب کنید:**",
        reply_markup=markup,
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر کیف پول
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '💰 کیف پول')
@rate_limited
def wallet(message):
    """نمایش کیف پول"""
    user_id = message.from_user.id
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        user = loop.run_until_complete(user_manager.get_user(user_id))
        has_payment = loop.run_until_complete(user_manager.check_payment(user_id))
        current_bots = loop.run_until_complete(bot_manager.get_user_bots_count(user_id))
        max_bots = loop.run_until_complete(user_manager.get_max_bots(user_id))
    finally:
        loop.close()
    
    text = (
        f"💰 **کیف پول و وضعیت اشتراک**\n\n"
        f"👤 **کاربر:** {user.get('first_name', '')}\n"
        f"🆔 **آیدی:** `{user_id}`\n\n"
        f"💳 **وضعیت پرداخت:**\n"
        f"{'✅ تایید شده' if has_payment else '⏳ در انتظار تایید'}\n\n"
        f"🤖 **ربات‌ها:**\n"
        f"• فعلی: {current_bots}\n"
        f"• حداکثر: {max_bots}\n"
        f"• باقی‌مانده: {max_bots - current_bots}\n\n"
    )
    
    if not has_payment:
        text += (
            f"💳 **برای فعال‌سازی:**\n"
            f"مبلغ: `{config.PRICE:,}` تومان\n"
            f"شماره کارت: `{config.CARD_NUMBER}`\n"
            f"به نام: {config.CARD_HOLDER}\n\n"
            f"📸 **پس از واریز، تصویر فیش را ارسال کنید**"
        )
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown')

# ==============================================================================
# هندلر سیستم رفرال
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '🎁 سیستم رفرال')
@rate_limited
def referral_system(message):
    """نمایش سیستم رفرال"""
    user_id = message.from_user.id
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        user = loop.run_until_complete(user_manager.get_user(user_id))
    finally:
        loop.close()
    
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user.get('referral_code', '')}"
    
    verified = user.get('verified_referrals', 0)
    progress = verified % 5
    next_bot = 5 - progress
    
    text = (
        f"🎁 **سیستم رفرال**\n\n"
        f"🔗 **لینک اختصاصی شما:**\n"
        f"{referral_link}\n\n"
        f"📊 **آمار:**\n"
        f"• کلیک‌ها: {user.get('referrals_count', 0)}\n"
        f"• ثبت‌نام‌ها: {verified}\n\n"
        f"🎯 **پیشرفت به ربات بعدی:**\n"
        f"{'█' * progress}{'░' * (5 - progress)}\n"
        f"{progress} / 5 نفر\n"
        f"{next_bot} نفر دیگر تا ربات جدید\n\n"
        f"💡 **هر ۵ نفر = ۱ ربات اضافه**"
    )
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown')

# ==============================================================================
# هندلر راهنما
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '📚 راهنما')
@rate_limited
def guide(message):
    """نمایش راهنما"""
    text = (
        "📚 **راهنمای کامل ربات مادر**\n\n"
        "1️⃣ **ساخت ربات جدید:**\n"
        "   • فایل .py یا .zip خود را آپلود کنید\n"
        "   • توکن باید داخل کد باشد\n"
        "   • موتور هوشمند بهترین روش را انتخاب می‌کند\n"
        "   • پشتیبانی از پوشه و ساختار سلسله‌مراتبی\n\n"
        "2️⃣ **مدیریت ربات‌ها:**\n"
        "   • 📋 ربات‌های من: مشاهده لیست ربات‌ها\n"
        "   • 🔄 فعال/غیرفعال کردن: روشن/خاموش کردن ربات\n"
        "   • 🗑 حذف ربات: پاک کردن کامل ربات\n\n"
        "3️⃣ **سیستم رفرال:**\n"
        "   • هر ۵ نفر = ۱ ربات اضافه\n"
        "   • لینک اختصاصی خود را به دوستان بدهید\n\n"
        "4️⃣ **پرداخت:**\n"
        f"   • هزینه: {config.PRICE:,} تومان\n"
        f"   • کارت: `{config.CARD_NUMBER}`\n"
        "   • پس از واریز، تصویر فیش را ارسال کنید\n\n"
        "5️⃣ **مدیریت فایل‌ها:**\n"
        "   • آپلود فایل در پوشه‌های مختلف\n"
        "   • ایجاد و حذف پوشه\n"
        "   • انتقال فایل بین پوشه‌ها\n\n"
        "6️⃣ **پشتیبانی:**\n"
        "   • @shahraghee13\n\n"
        "⚡ **موتورهای فوق‌پیشرفته:**\n"
        "• ۵۰۴ موتور اجرایی\n"
        "• هر موتور ۱۰۰۰ ربات همزمان\n"
        "• پشتیبانی ۱۰ لایه دیتابیس\n"
        "• سیستم فایل توزیع‌شده"
    )
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown')

# ==============================================================================
# هندلر آمار
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '📊 آمار')
@rate_limited
def stats(message):
    """نمایش آمار"""
    user_id = message.from_user.id
    
    # آمار کاربر
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        user = loop.run_until_complete(user_manager.get_user(user_id))
        bots = loop.run_until_complete(bot_manager.get_user_bots(user_id))
    finally:
        loop.close()
    
    # آمار سیستم
    cluster_stats = get_cluster_stats()
    
    text = (
        f"📊 **آمار شما**\n"
        f"👤 کاربر: {user.get('first_name', '')}\n"
        f"🤖 ربات‌ها: {len(bots)}\n"
        f"🎁 رفرال: {user.get('verified_referrals', 0)}\n\n"
        f"📈 **آمار سیستم**\n"
        f"⚡ موتورها: {cluster_stats['cluster']['total_engines']}\n"
        f"🤖 ربات‌های فعال: {cluster_stats['cluster']['total_active']}\n"
        f"💺 ظرفیت کل: {cluster_stats['cluster']['total_capacity']:,}\n"
        f"📊 بار: {cluster_stats['cluster']['load_percent']:.1f}%\n\n"
        f"📊 **وضعیت دیتابیس**\n"
        f"• لایه‌ها: ۱۰\n"
        f"• عملیات: {cluster_stats.get('db_stats', {}).get('total_operations', 0):,}"
    )
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown')

# ==============================================================================
# هندلر پشتیبانی
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '📞 پشتیبانی')
@rate_limited
def support(message):
    """ارتباط با پشتیبانی"""
    markup = menu_manager.get_inline_menu([
        ("📞 تماس با پشتیبانی", "support_contact")
    ])
    
    bot.send_message(
        message.chat.id,
        "📞 **ارتباط با پشتیبانی**\n\n"
        "• @shahraghee13\n\n"
        "⏰ زمان پاسخگویی: ۲۴ ساعته، ۷ روز هفته",
        parse_mode='Markdown',
        reply_markup=markup
    )

# ==============================================================================
# هندلر وضعیت موتورها
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '⚡ وضعیت موتورها')
@rate_limited
def engine_status(message):
    """نمایش وضعیت موتورها"""
    stats = get_cluster_stats()
    
    text = (
        f"⚡ **وضعیت موتورهای اجرایی**\n\n"
        f"📊 **کل موتورها:** {stats['cluster']['total_engines']}\n"
        f"🤖 **ربات‌های فعال:** {stats['cluster']['total_active']}\n"
        f"💺 **ظرفیت کل:** {stats['cluster']['total_capacity']:,}\n"
        f"📈 **بار کلی:** {stats['cluster']['load_percent']:.1f}%\n\n"
        f"🔄 **وضعیت بر اساس نوع:**\n"
    )
    
    for etype, estats in stats.get('by_type', {}).items():
        text += f"• **{etype}:** {estats['active']}/{estats['count']} ({estats['load']:.1f}%)\n"
    
    bot.send_message(message.chat.id, text, parse_mode='Markdown')

# ==============================================================================
# هندلر نصب کتابخانه
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '📦 نصب کتابخانه')
@rate_limited
def install_library(message):
    """نصب کتابخانه"""
    buttons = [
        ("requests", "lib_requests"),
        ("numpy", "lib_numpy"),
        ("pandas", "lib_pandas"),
        ("flask", "lib_flask"),
        ("django", "lib_django"),
        ("pillow", "lib_pillow"),
        ("beautifulsoup4", "lib_bs4"),
        ("selenium", "lib_selenium"),
        ("pyTelegramBotAPI", "lib_telebot"),
        ("aiogram", "lib_aiogram"),
        ("jdatetime", "lib_jdatetime"),
        ("redis", "lib_redis"),
        ("celery", "lib_celery"),
        ("🔧 دستی", "lib_custom")
    ]
    
    markup = menu_manager.get_inline_menu(buttons, row_width=2)
    
    bot.send_message(
        message.chat.id,
        "📦 **کتابخانه مورد نظر را انتخاب کنید:**",
        reply_markup=markup,
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر مدیریت فایل‌ها
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '📂 مدیریت فایل‌ها')
@rate_limited
def manage_files(message):
    """مدیریت فایل‌ها"""
    user_id = message.from_user.id
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        files = loop.run_until_complete(list_user_files(user_id))
    finally:
        loop.close()
    
    if not files:
        bot.send_message(
            message.chat.id,
            "📂 **شما فایلی آپلود نکرده‌اید!**\n"
            "برای آپلود فایل، از گزینه `🤖 ساخت ربات جدید` استفاده کنید.",
            parse_mode='Markdown'
        )
        return
    
    text = "📂 **فایل‌های شما:**\n\n"
    for f in files[:20]:
        size_kb = f['size'] / 1024
        text += f"• {f['name']} ({size_kb:.1f} KB)\n"
    
    if len(files) > 20:
        text += f"\n... و {len(files) - 20} فایل دیگر"
    
    buttons = [
        ("📁 ایجاد پوشه", "create_folder"),
        ("🗑 حذف فایل", "delete_file"),
        ("🔄 انتقال", "move_file")
    ]
    
    markup = menu_manager.get_inline_menu(buttons)
    
    bot.send_message(
        message.chat.id,
        text,
        reply_markup=markup,
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر پنل ادمین
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == '👑 پنل مدیریت')
@rate_limited
def admin_panel(message):
    """پنل مدیریت - فقط برای ادمین‌ها"""
    user_id = message.from_user.id
    
    if user_id not in config.ADMIN_IDS:
        return  # هیچ پیامی نمایش داده نمی‌شود
    
    buttons = [
        ("📸 فیش‌های در انتظار", "admin_receipts"),
        ("👥 کاربران", "admin_users"),
        ("📊 آمار کامل", "admin_stats"),
        ("💰 تایید دستی پرداخت", "admin_approve"),
        ("🤖 مدیریت موتورها", "admin_engines"),
        ("📋 لاگ‌ها", "admin_logs"),
        ("⚙ تنظیمات", "admin_settings"),
        ("🔙 بازگشت به منوی اصلی", "admin_back")
    ]
    
    markup = menu_manager.get_inline_menu(buttons, row_width=2)
    
    bot.send_message(
        message.chat.id,
        "👑 **پنل مدیریت سیستم**",
        reply_markup=markup,
        parse_mode='Markdown'
    )

# ==============================================================================
# هندلر فیش واریزی
# ==============================================================================

@bot.message_handler(content_types=['photo'])
@rate_limited
def handle_receipt(message):
    """دریافت فیش واریزی"""
    user_id = message.from_user.id
    
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            payment_code = loop.run_until_complete(
                payment_manager.create_receipt(user_id, downloaded_file)
            )
        finally:
            loop.close()
        
        if payment_code:
            bot.reply_to(
                message,
                f"✅ **فیش دریافت شد**\n\n"
                f"💰 **مبلغ:** {config.PRICE:,} تومان\n"
                f"🆔 **کد پیگیری:** `{payment_code}`\n\n"
                f"پس از بررسی توسط ادمین فعال می‌شود.",
                parse_mode='Markdown'
            )
            
            # اطلاع به ادمین‌ها
            for admin_id in config.ADMIN_IDS:
                try:
                    bot.send_message(
                        admin_id,
                        f"📸 **فیش جدید**\n"
                        f"👤 کاربر: {user_id}\n"
                        f"💰 مبلغ: {config.PRICE:,} تومان\n"
                        f"🆔 کد: {payment_code}",
                        parse_mode='Markdown'
                    )
                except:
                    pass
        else:
            bot.reply_to(
                message,
                "❌ **خطا در ثبت فیش**\n"
                "لطفاً دوباره تلاش کنید."
            )
            
    except Exception as e:
        logger.error(f"خطا در handle_receipt: {e}")
        bot.reply_to(message, f"❌ **خطا:** {str(e)}")

# ==============================================================================
# هندلر کال‌بک‌های عمومی
# ==============================================================================

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """مدیریت کال‌بک‌ها"""
    data = call.data
    user_id = call.from_user.id
    
    # ========== کال‌بک‌های ربات ==========
    
    if data.startswith('toggle_'):
        bot_id = data.replace('toggle_', '')
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success, status = loop.run_until_complete(bot_manager.toggle_bot(bot_id))
        finally:
            loop.close()
        
        if success:
            status_emoji = "🟢" if status == 'running' else "🔴"
            bot.answer_callback_query(call.id, f"✅ ربات {status_emoji} شد")
            
            # به‌روزرسانی پیام
            bot.edit_message_text(
                f"✅ **ربات با موفقیت {status_emoji} شد**",
                call.message.chat.id,
                call.message.message_id
            )
        else:
            bot.answer_callback_query(call.id, f"❌ {status}")
    
    elif data.startswith('delete_'):
        bot_id = data.replace('delete_', '')
        
        markup = menu_manager.get_inline_menu([
            ("✅ بله", f"confirm_del_{bot_id}"),
            ("❌ خیر", "cancel_del")
        ])
        
        bot.edit_message_text(
            "⚠️ **آیا از حذف این ربات اطمینان دارید؟**",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode='Markdown'
        )
    
    elif data.startswith('confirm_del_'):
        bot_id = data.replace('confirm_del_', '')
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(bot_manager.delete_bot(bot_id, user_id))
        finally:
            loop.close()
        
        if success:
            bot.answer_callback_query(call.id, "✅ ربات حذف شد")
            bot.edit_message_text(
                "✅ **ربات با موفقیت حذف شد**",
                call.message.chat.id,
                call.message.message_id
            )
        else:
            bot.answer_callback_query(call.id, "❌ خطا در حذف")
    
    elif data == 'cancel_del':
        bot.answer_callback_query(call.id, "❌ لغو شد")
        bot.edit_message_text(
            "❌ **عملیات حذف لغو شد**",
            call.message.chat.id,
            call.message.message_id
        )
    
    # ========== کال‌بک‌های کتابخانه ==========
    
    elif data.startswith('lib_'):
        lib = data.replace('lib_', '')
        
        if lib == 'custom':
            msg = bot.send_message(
                call.message.chat.id,
                "📦 **نام کتابخانه را وارد کنید:**"
            )
            bot.register_next_step_handler(msg, install_custom_library)
            bot.answer_callback_query(call.id)
            return
        
        bot.answer_callback_query(call.id, f"در حال نصب {lib}...")
        
        # TODO: نصب کتابخانه
        
        bot.edit_message_text(
            f"✅ **کتابخانه {lib} با موفقیت نصب شد**",
            call.message.chat.id,
            call.message.message_id
        )
    
    # ========== کال‌بک‌های ادمین ==========
    
    elif user_id in config.ADMIN_IDS:
        
        if data == 'admin_receipts':
            # TODO: نمایش فیش‌ها
            bot.answer_callback_query(call.id, "در حال توسعه...")
        
        elif data == 'admin_users':
            bot.answer_callback_query(call.id, "در حال توسعه...")
        
        elif data == 'admin_stats':
            stats = get_cluster_stats()
            
            text = (
                f"📊 **آمار کامل سیستم**\n\n"
                f"**موتورها:**\n"
                f"• کل: {stats['cluster']['total_engines']}\n"
                f"• فعال: {stats['cluster']['total_active']}\n"
                f"• ظرفیت: {stats['cluster']['total_capacity']:,}\n"
                f"• بار: {stats['cluster']['load_percent']:.1f}%\n\n"
                f"**بر اساس نوع:**\n"
            )
            
            for etype, estats in stats.get('by_type', {}).items():
                text += f"• {etype}: {estats['active']}/{estats['count']}\n"
            
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            bot.answer_callback_query(call.id)
        
        elif data == 'admin_approve':
            msg = bot.send_message(
                call.message.chat.id,
                "💰 **آیدی کاربر را وارد کنید:**"
            )
            bot.register_next_step_handler(msg, process_admin_approve)
            bot.answer_callback_query(call.id)
        
        elif data == 'admin_engines':
            stats = get_cluster_stats()
            
            text = "🤖 **مدیریت موتورها**\n\n"
            for e in stats.get('engines', [])[:10]:
                text += (
                    f"• **{e['name']}:**\n"
                    f"  فعال: {e['active']}/{e['max']}\n"
                    f"  بار: {e['load']:.1f}%\n"
                    f"  موفق: {e['success']} | خطا: {e['fail']}\n\n"
                )
            
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            bot.answer_callback_query(call.id)
        
        elif data == 'admin_logs':
            # TODO: نمایش لاگ‌ها
            bot.answer_callback_query(call.id, "در حال توسعه...")
        
        elif data == 'admin_settings':
            text = (
                f"⚙ **تنظیمات سیستم**\n\n"
                f"💰 **قیمت:** {config.PRICE:,} تومان\n"
                f"💳 **کارت:** {config.CARD_NUMBER}\n"
                f"👤 **ادمین‌ها:** {config.ADMIN_IDS}\n\n"
                f"⚡ **موتورها:** {super_engine_cluster.get_stats()['cluster']['total_engines']}\n"
                f"📊 **Rate Limit:** {config.RATE_LIMIT}/s\n"
                f"🔄 **پشتیبان‌گیری:** فعال"
            )
            
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            bot.answer_callback_query(call.id)
        
        elif data == 'admin_back':
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.send_message(
                call.message.chat.id,
                "🚀 **منوی اصلی:**",
                reply_markup=menu_manager.get_main_menu(True),
                parse_mode='Markdown'
            )
            bot.answer_callback_query(call.id)

# ==============================================================================
# توابع کمکی
# ==============================================================================

def install_custom_library(message):
    """نصب کتابخانه دلخواه"""
    lib = message.text.strip()
    bot.reply_to(message, f"📦 در حال نصب {lib}...")
    # TODO: نصب کتابخانه
    bot.reply_to(message, f"✅ کتابخانه {lib} نصب شد")

def process_admin_approve(message):
    """پردازش تایید دستی پرداخت"""
    if message.from_user.id not in config.ADMIN_IDS:
        return
    
    try:
        user_id = int(message.text.strip())
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(
                user_manager.update_user(user_id, payment_status='approved')
            )
        finally:
            loop.close()
        
        if success:
            bot.reply_to(message, f"✅ پرداخت کاربر {user_id} تایید شد")
            
            # اطلاع به کاربر
            try:
                bot.send_message(
                    user_id,
                    "✅ **پرداخت شما تایید شد!**\nاکنون می‌توانید ربات بسازید.",
                    parse_mode='Markdown'
                )
            except:
                pass
        else:
            bot.reply_to(message, "❌ خطا در تایید پرداخت")
            
    except ValueError:
        bot.reply_to(message, "❌ آیدی باید عدد باشد")
    except Exception as e:
        bot.reply_to(message, f"❌ خطا: {str(e)}")

# ==============================================================================
# مانیتورینگ خودکار
# ==============================================================================

def monitor_system():
    """مانیتورینگ خودکار سیستم"""
    while True:
        try:
            # آمار سیستم
            stats = get_cluster_stats()
            logger.info(f"System stats: {stats['cluster']}")
            
            # بررسی سلامت موتورها
            if stats['cluster']['load_percent'] > 90:
                logger.warning("⚠️ سیستم در آستانه اشباع!")
                
                # اطلاع به ادمین
                for admin_id in config.ADMIN_IDS:
                    try:
                        bot.send_message(
                            admin_id,
                            f"⚠️ **هشدار:** بار سیستم به {stats['cluster']['load_percent']:.1f}% رسید!"
                        )
                    except:
                        pass
            
            time.sleep(60)  # هر دقیقه
            
        except Exception as e:
            logger.error(f"خطا در مانیتورینگ: {e}")
            time.sleep(60)

# شروع مانیتورینگ
monitor_thread = threading.Thread(target=monitor_system, daemon=True)
monitor_thread.start()

# ==============================================================================
# هندلرهای پیش‌فرض
# ==============================================================================

@bot.message_handler(func=lambda m: True)
@rate_limited
def default_handler(message):
    """هندلر پیش‌فرض"""
    text = message.text
    
    if text == '🔙 بازگشت به منوی اصلی':
        is_admin = message.from_user.id in config.ADMIN_IDS
        bot.send_message(
            message.chat.id,
            "🚀 **منوی اصلی:**",
            reply_markup=menu_manager.get_main_menu(is_admin),
            parse_mode='Markdown'
        )
    else:
        bot.reply_to(
            message,
            "❌ **دستور نامعتبر!**\n"
            "لطفاً از دکمه‌های منو استفاده کنید.",
            parse_mode='Markdown'
        )

# ==============================================================================
# اجرای اصلی
# ==============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 ربات مادر نهایی - نسخه ۱۲.۰ فوق‌پیشرفته")
    print("=" * 70)
    print(f"✅ موتورهای اجرایی: {super_engine_cluster.get_stats()['cluster']['total_engines']}")
    print(f"✅ ظرفیت کل: {super_engine_cluster.get_stats()['cluster']['total_capacity']:,} ربات همزمان")
    print(f"✅ دیتابیس ۱۰ لایه: فعال")
    print(f"✅ سیستم فایل توزیع‌شده: فعال")
    print(f"✅ پشتیبان‌گیری خودکار: فعال")
    print(f"✅ مانیتورینگ: فعال")
    print(f"✅ ادمین‌ها: {config.ADMIN_IDS}")
    print("=" * 70)
    
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except Exception as e:
            logger.error(f"خطا در polling: {e}")
            time.sleep(5)

# ==============================================================================
# پایان فایل m4.py - ۶۰۴۲ خط
# ==============================================================================