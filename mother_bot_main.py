#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ربات مادر نهایی - فایل اصلی
نسخه 14.0 - ایزوله اولترا
"""

from mother_bot_core import *

# ==================== توابع کمکی ====================

def generate_referral_code(user_id):
    return hashlib.sha3_256(f"{user_id}_{time.time()}_{secrets.token_hex(16)}".encode()).hexdigest()[:12]

def get_user(user_id):
    try:
        with get_db() as conn:
            user = conn.execute(
                'SELECT * FROM users WHERE user_id = ?',
                (user_id,)
            ).fetchone()
            return dict(user) if user else None
    except:
        return None

def create_user(user_id, username, first_name, last_name, referred_by=None):
    try:
        with get_db() as conn:
            now = datetime.now().isoformat()
            referral_code = generate_referral_code(user_id)
            
            conn.execute('''
                INSERT OR IGNORE INTO users 
                (user_id, username, first_name, last_name, referral_code, referred_by, created_at, last_active, payment_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name, referral_code, referred_by, now, now, 'pending'))
            
            conn.execute('''
                UPDATE users SET last_active = ? WHERE user_id = ?
            ''', (now, user_id))
            conn.commit()
            
            if referred_by:
                conn.execute('''
                    UPDATE users SET referrals_count = referrals_count + 1
                    WHERE user_id = ?
                ''', (referred_by,))
                conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error in create_user: {e}")
        return False

def check_payment(user_id):
    try:
        with get_db() as conn:
            user = conn.execute('SELECT payment_status FROM users WHERE user_id = ?', (user_id,)).fetchone()
            if user and user['payment_status'] == 'approved':
                return True
            
            receipt = conn.execute('''
                SELECT id FROM receipts 
                WHERE user_id = ? AND status = 'approved'
                ORDER BY created_at DESC LIMIT 1
            ''', (user_id,)).fetchone()
            
            if receipt:
                conn.execute('UPDATE users SET payment_status = ? WHERE user_id = ?', 
                            ('approved', user_id))
                conn.commit()
                return True
            return False
    except:
        return False

def check_bot_limit(user_id):
    try:
        with get_db() as conn:
            user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
            if not user:
                return True, 1, 0
            
            extra_bots = user['verified_referrals'] // 5
            max_bots = 1 + extra_bots
            current_bots = conn.execute('SELECT COUNT(*) FROM bots WHERE user_id = ?', (user_id,)).fetchone()[0]
            
            return current_bots < max_bots, max_bots, current_bots
    except:
        return True, 1, 0

def get_user_bots(user_id):
    try:
        with get_db() as conn:
            bots = conn.execute('''
                SELECT * FROM bots WHERE user_id = ? ORDER BY created_at DESC
            ''', (user_id,)).fetchall()
            return [dict(bot) for bot in bots]
    except:
        return []

def get_bot(bot_id):
    try:
        with get_db() as conn:
            bot = conn.execute('SELECT * FROM bots WHERE id = ?', (bot_id,)).fetchone()
            if bot:
                bot = dict(bot)
                if 'token' in bot:
                    bot['token'] = crypto.decrypt(bot['token'])
            return bot
    except:
        return None

def update_bot_status(bot_id, status, pid=None, container_id=None, isolation_method=None, engine_id=None):
    try:
        with get_db() as conn:
            if pid and container_id and isolation_method and engine_id:
                conn.execute('''
                    UPDATE bots SET status = ?, pid = ?, container_id = ?, isolation_method = ?, engine_id = ?, last_active = ? WHERE id = ?
                ''', (status, pid, container_id, isolation_method, engine_id, datetime.now().isoformat(), bot_id))
            elif pid and engine_id:
                conn.execute('''
                    UPDATE bots SET status = ?, pid = ?, engine_id = ?, last_active = ? WHERE id = ?
                ''', (status, pid, engine_id, datetime.now().isoformat(), bot_id))
            else:
                conn.execute('''
                    UPDATE bots SET status = ?, last_active = ? WHERE id = ?
                ''', (status, datetime.now().isoformat(), bot_id))
            conn.commit()
            return True
    except:
        return False

def delete_bot(bot_id, user_id):
    try:
        with get_db() as conn:
            bot = conn.execute('SELECT * FROM bots WHERE id = ? AND user_id = ?', (bot_id, user_id)).fetchone()
            if not bot:
                return False
            
            isolation.stop_isolated(bot_id)
            
            if bot['file_path'] and os.path.exists(bot['file_path']):
                os.remove(bot['file_path'])
            
            if bot['folder_path'] and os.path.exists(bot['folder_path']):
                shutil.rmtree(bot['folder_path'])
            
            conn.execute('DELETE FROM bots WHERE id = ?', (bot_id,))
            conn.execute('UPDATE users SET bots_count = bots_count - 1 WHERE user_id = ?', (user_id,))
            conn.commit()
            return True
    except:
        return False

def save_uploaded_file(user_id, file_data, file_name):
    try:
        user_dir = os.path.join(FILES_DIR, str(user_id))
        os.makedirs(user_dir, exist_ok=True)
        
        timestamp = int(time.time())
        file_path = os.path.join(user_dir, f"{timestamp}_{file_name}")
        
        with open(file_path, 'wb') as f:
            f.write(file_data)
        
        return file_path
    except:
        return None

def extract_files_from_zip(zip_path, extract_to, user_id=None):
    py_files = []
    try:
        safe_zip, threats = security.scan_zip(zip_path)
        if not safe_zip:
            security.block_threat(f"ZIP_{zip_path}", f"Unsafe ZIP: {threats}")
            return []
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        
        for root, _, files in os.walk(extract_to):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        
                        safe, threats = security.scan_file_content(content.encode())
                        if safe:
                            py_files.append({
                                'name': file,
                                'path': file_path,
                                'content': content
                            })
                        else:
                            quarantine_path = os.path.join(QUARANTINE_DIR, f"user_{user_id}_{file}")
                            shutil.move(file_path, quarantine_path)
                            security.log_security_event("QUARANTINED_FILE", f"{file} - {threats}")
                    except:
                        pass
    except Exception as e:
        logger.error(f"Error extracting ZIP: {e}")
    return py_files

def extract_token_from_code(code):
    patterns = [
        r'token\s*=\s*["\']([^"\']+)["\']',
        r'TOKEN\s*=\s*["\']([^"\']+)["\']',
        r'API_TOKEN\s*=\s*["\']([^"\']+)["\']',
        r'BOT_TOKEN\s*=\s*["\']([^"\']+)["\']',
        r'bot\s*=\s*telebot\.TeleBot\(\s*["\']([^"\']+)["\']\s*\)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, code, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

def add_bot(user_id, bot_id, token, name, username, file_path, folder_path=None, pid=None, container_id=None, isolation_method='simple', engine_id=None):
    try:
        with get_db() as conn:
            now = datetime.now().isoformat()
            status = 'running' if pid else 'stopped'
            
            encrypted_token = crypto.encrypt(token)
            
            conn.execute('''
                INSERT INTO bots 
                (id, user_id, token, name, username, file_path, folder_path, pid, container_id, isolation_method, engine_id, status, created_at, last_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (bot_id, user_id, encrypted_token, name, username, file_path, folder_path, pid, container_id, isolation_method, engine_id, status, now, now))
            
            conn.execute('''
                UPDATE users SET bots_count = bots_count + 1, last_active = ?
                WHERE user_id = ?
            ''', (now, user_id))
            conn.commit()
            
            user = conn.execute('SELECT referred_by FROM users WHERE user_id = ?', (user_id,)).fetchone()
            if user and user['referred_by']:
                conn.execute('''
                    UPDATE users SET verified_referrals = verified_referrals + 1
                    WHERE user_id = ?
                ''', (user['referred_by'],))
                conn.commit()
            
            return True
    except Exception as e:
        logger.error(f"Error in add_bot: {e}")
        return False

# ==================== کتابخانه منیجر با امنیت ====================
class SecurityLibraryManager:
    def __init__(self):
        self.common_libs = {
            'requests': 'requests',
            'numpy': 'numpy',
            'pandas': 'pandas',
            'flask': 'flask',
            'django': 'django',
            'pillow': 'Pillow',
            'beautifulsoup4': 'beautifulsoup4',
            'selenium': 'selenium',
            'pyTelegramBotAPI': 'pyTelegramBotAPI',
            'aiogram': 'aiogram',
            'jdatetime': 'jdatetime'
        }
    
    def install(self, lib_name, chat_id, user_id, message_id=None):
        if lib_name in security.dangerous_libraries:
            security.log_security_event("DANGEROUS_LIBRARY", f"User {user_id} tried to install {lib_name}")
            bot.send_message(chat_id, f"❌ کتابخانه {lib_name} ممنوع است!")
            return False
        
        try:
            msg = bot.send_message(chat_id, f"🔄 در حال نصب {lib_name}...")
            
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", lib_name, "--no-cache-dir"],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                bot.edit_message_text(
                    f"✅ کتابخانه {lib_name} با موفقیت نصب شد.",
                    chat_id,
                    msg.message_id
                )
                return True
            else:
                bot.edit_message_text(
                    f"❌ خطا در نصب {lib_name}:\n{result.stderr[:200]}",
                    chat_id,
                    msg.message_id
                )
                return False
                
        except subprocess.TimeoutExpired:
            bot.send_message(chat_id, f"❌ زمان نصب {lib_name} بیش از حد طول کشید.")
            return False
        except Exception as e:
            bot.send_message(chat_id, f"❌ خطا: {str(e)}")
            return False

security_library_manager = SecurityLibraryManager()

# ==================== سیستم خطایابی پیشرفته ====================
class AdvancedErrorHandler:
    def __init__(self):
        self.error_count = 0
        self.error_types = defaultdict(int)
        self.last_error = None
        self.error_callbacks = {}
    
    def register_callback(self, error_type, callback):
        self.error_callbacks[error_type] = callback
    
    def handle(self, func):
        def wrapper(message, *args, **kwargs):
            try:
                return func(message, *args, **kwargs)
            except Exception as e:
                self.error_count += 1
                error_type = type(e).__name__
                self.error_types[error_type] += 1
                
                error_id = hashlib.sha3_256(f"{time.time()}{e}{secrets.token_hex(8)}".encode()).hexdigest()[:12]
                error_msg = str(e)[:200]
                
                logger.error(f"Error {error_id}: {error_type} - {error_msg}")
                logger.error(traceback.format_exc())
                
                try:
                    with get_db() as conn:
                        conn.execute('''
                            INSERT INTO errors (id, type, message, user_id, timestamp)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (error_id, error_type, error_msg, message.from_user.id, datetime.now().isoformat()))
                        conn.commit()
                except:
                    pass
                
                if error_type in self.error_callbacks:
                    try:
                        self.error_callbacks[error_type](message, e)
                    except:
                        pass
                
                try:
                    bot.reply_to(
                        message,
                        f"❌ خطایی رخ داد!\n"
                        f"🆔 کد خطا: `{error_id}`\n"
                        f"📋 نوع: {error_type}\n"
                        f"📞 با پشتیبانی تماس بگیرید."
                    )
                except:
                    pass
        return wrapper
    
    def handle_callback(self, func):
        def wrapper(call, *args, **kwargs):
            try:
                return func(call, *args, **kwargs)
            except Exception as e:
                self.error_count += 1
                error_type = type(e).__name__
                self.error_types[error_type] += 1
                
                error_id = hashlib.sha3_256(f"{time.time()}{e}{secrets.token_hex(8)}".encode()).hexdigest()[:12]
                
                logger.error(f"Callback Error {error_id}: {error_type} - {e}")
                logger.error(traceback.format_exc())
                
                try:
                    bot.answer_callback_query(
                        call.id,
                        f"❌ خطا! کد: {error_id}"
                    )
                except:
                    pass
        return wrapper
    
    def get_stats(self):
        return {
            'total': self.error_count,
            'by_type': dict(self.error_types),
            'last_error': self.last_error
        }

error_handler = AdvancedErrorHandler()

# ==================== منوی اصلی ====================
def get_main_menu(is_admin=False):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    
    buttons = [
        types.KeyboardButton('🤖 ساخت ربات جدید'),
        types.KeyboardButton('📋 ربات‌های من'),
        types.KeyboardButton('🔄 فعال/غیرفعال کردن'),
        types.KeyboardButton('🗑 حذف ربات'),
        types.KeyboardButton('💰 کیف پول و رفرال'),
        types.KeyboardButton('📚 راهنما'),
        types.KeyboardButton('📦 نصب کتابخانه'),
        types.KeyboardButton('📊 آمار'),
        types.KeyboardButton('📞 پشتیبانی'),
        types.KeyboardButton('⚡ وضعیت موتورها'),
        types.KeyboardButton('📁 مدیریت فایل‌ها'),
        types.KeyboardButton('🔒 وضعیت امنیت'),
    ]
    
    if is_admin:
        buttons.extend([
            types.KeyboardButton('👑 پنل ادمین'),
            types.KeyboardButton('🔒 گزارش امنیتی'),
            types.KeyboardButton('📊 خطاها'),
            types.KeyboardButton('📦 وضعیت ایزوله'),
        ])
    
    markup.add(*buttons)
    return markup

# ==================== هندلر استارت ====================
@bot.message_handler(commands=['start'])
@error_handler.handle
def cmd_start(message):
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or ""
    last_name = message.from_user.last_name or ""
    
    referred_by = None
    args = message.text.split()
    if len(args) > 1:
        ref_code = args[1]
        try:
            with get_db() as conn:
                referrer = conn.execute('SELECT user_id FROM users WHERE referral_code = ?', (ref_code,)).fetchone()
                if referrer:
                    referred_by = referrer['user_id']
                    
                    try:
                        bot.send_message(
                            referred_by,
                            f"🎉 یک نفر با لینک رفرال شما وارد شد!\n\n👤 {first_name}\n🆔 {user_id}"
                        )
                    except:
                        pass
        except:
            pass
    
    create_user(user_id, username, first_name, last_name, referred_by)
    
    user = get_user(user_id) or {'referral_code': '', 'referrals_count': 0, 'verified_referrals': 0}
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user['referral_code']}"
    
    is_admin = user_id in ADMIN_IDS
    markup = get_main_menu(is_admin)
    
    welcome_text = (
        f"🚀 به ربات مادر نهایی خوش آمدید {first_name}!\n\n"
        f"👤 آیدی شما: {user_id}\n"
        f"🎁 کد رفرال شما: {user['referral_code']}\n"
        f"🔗 لینک دعوت: {referral_link}\n\n"
        f"📊 آمار رفرال:\n"
        f"• کلیک‌ها: {user['referrals_count']}\n"
        f"• ساخته شده: {user['verified_referrals']}\n\n"
        f"💡 هر ۵ نفر = ۱ ربات اضافه\n"
        f"📤 فایل خود را آپلود کنید\n"
        f"🔒 همه ربات‌ها در محیط ایزوله اجرا می‌شوند"
    )
    
    bot.send_message(
        message.chat.id,
        welcome_text,
        reply_markup=markup
    )

# ==================== کیف پول و رفرال ====================
@bot.message_handler(func=lambda m: m.text == '💰 کیف پول و رفرال')
@error_handler.handle
def wallet_ref(message):
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        bot.send_message(message.chat.id, "❌ لطفاً /start را بزنید")
        return
    
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user['referral_code']}"
    
    payment_approved = check_payment(user_id)
    can_create, max_bots, current_bots = check_bot_limit(user_id)
    
    text = f"💰 کیف پول و سیستم رفرال\n\n"
    text += f"👤 کاربر: {user['first_name']}\n"
    text += f"🆔 آیدی: {user_id}\n\n"
    text += f"💳 وضعیت پرداخت:\n"
    text += f"{'✅ تایید شده' if payment_approved else '⏳ در انتظار تایید'}\n\n"
    text += f"🎁 کد رفرال شما:\n{user['referral_code']}\n"
    text += f"🔗 لینک دعوت:\n{referral_link}\n\n"
    text += f"📊 آمار رفرال:\n"
    text += f"• کلیک‌ها: {user['referrals_count']}\n"
    text += f"• ساخته شده: {user['verified_referrals']}\n\n"
    text += f"🤖 ربات‌ها:\n"
    text += f"• فعلی: {current_bots}\n"
    text += f"• حداکثر: {max_bots}\n\n"
    
    if not payment_approved:
        text += f"💳 برای ساخت ربات:\n"
        text += f"مبلغ: {PRICE:,} تومان\n"
        text += f"شماره کارت: {CARD_NUMBER}\n\n"
        text += f"📸 پس از واریز، تصویر فیش را ارسال کنید"
    
    bot.send_message(message.chat.id, text)

# ==================== فیش واریزی ====================
@bot.message_handler(content_types=['photo'])
@error_handler.handle
def handle_receipt(message):
    user_id = message.from_user.id
    
    try:
        with get_db() as conn:
            existing = conn.execute('''
                SELECT id FROM receipts 
                WHERE user_id = ? AND status = 'pending'
            ''', (user_id,)).fetchone()
            
            if existing:
                bot.reply_to(message, "⏳ شما یک فیش در انتظار بررسی دارید")
                return
    except:
        pass
    
    try:
        file_info = bot.get_file(message.photo[-1].file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        payment_code = hashlib.sha3_256(f"{user_id}_{time.time()}_{secrets.token_hex(8)}".encode()).hexdigest()[:12].upper()
        receipt_path = os.path.join(RECEIPTS_DIR, f"{user_id}_{payment_code}.jpg")
        
        with open(receipt_path, 'wb') as f:
            f.write(downloaded_file)
        
        with get_db() as conn:
            conn.execute('''
                INSERT INTO receipts (user_id, amount, receipt_path, created_at, payment_code)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, PRICE, receipt_path, datetime.now().isoformat(), payment_code))
            conn.commit()
        
        bot.reply_to(
            message,
            f"✅ فیش دریافت شد\n"
            f"💰 مبلغ: {PRICE:,} تومان\n"
            f"🆔 کد پیگیری: {payment_code}\n\n"
            f"پس از بررسی توسط ادمین فعال می‌شود"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                bot.send_message(
                    admin_id,
                    f"📸 فیش جدید\n👤 {user_id}\n💰 {PRICE:,} تومان\n🆔 {payment_code}"
                )
            except:
                pass
    except Exception as e:
        bot.reply_to(message, f"❌ خطا: {str(e)}")

# ==================== وضعیت امنیت ====================
@bot.message_handler(func=lambda m: m.text == '🔒 وضعیت امنیت')
@error_handler.handle
def security_status(message):
    isolation_stats = isolation.get_stats()
    security_report = security.get_security_report()
    
    text = f"🔒 **وضعیت امنیتی**\n\n"
    text += f"**ایزوله‌سازی:**\n"
    text += f"• کانتینرهای فعال: {isolation_stats['docker']}\n"
    text += f"• سندباکس‌های فعال: {isolation_stats['sandbox']}\n"
    text += f"• روش‌های موجود: {', '.join(isolation_stats['methods'])}\n\n"
    
    text += f"**لایه‌های امنیتی:**\n"
    for layer, info in security_report['layers'].items():
        status = "✅" if info['enabled'] else "❌"
        text += f"• {status} {info['name']}\n"
    
    text += f"\n**آمار:**\n"
    text += f"• تهدیدات شناسایی شده: {security_report['threats_detected']}\n"
    text += f"• تهدیدات مسدود شده: {security_report['threats_blocked']}\n"
    text += f"• قوانین فایروال: {security_report['firewall_rules']}"
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== نصب کتابخانه ====================
@bot.message_handler(func=lambda m: m.text == '📦 نصب کتابخانه')
@error_handler.handle
def install_library_menu(message):
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    libs = [
        ('requests', 'requests'),
        ('numpy', 'numpy'),
        ('pandas', 'pandas'),
        ('flask', 'flask'),
        ('django', 'django'),
        ('pillow', 'Pillow'),
        ('beautifulsoup4', 'beautifulsoup4'),
        ('selenium', 'selenium'),
        ('pyTelegramBotAPI', 'pyTelegramBotAPI'),
        ('aiogram', 'aiogram'),
        ('jdatetime', 'jdatetime'),
        ('🔧 دستی', 'custom')
    ]
    
    for name, data in libs:
        markup.add(types.InlineKeyboardButton(name, callback_data=f"lib_{data}"))
    
    bot.send_message(
        message.chat.id,
        "📦 کتابخانه مورد نظر را انتخاب کنید:",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('lib_'))
@error_handler.handle_callback
def install_library_callback(call):
    lib = call.data.replace('lib_', '')
    user_id = call.from_user.id
    
    if lib == 'custom':
        msg = bot.send_message(
            call.message.chat.id,
            "📦 نام کتابخانه را وارد کنید:"
        )
        bot.register_next_step_handler(msg, install_custom_library)
        bot.answer_callback_query(call.id)
        return
    
    bot.answer_callback_query(call.id, f"در حال نصب {lib}...")
    security_library_manager.install(lib, call.message.chat.id, user_id)

def install_custom_library(message):
    lib = message.text.strip()
    user_id = message.from_user.id
    security_library_manager.install(lib, message.chat.id, user_id)

# ==================== ساخت ربات جدید ====================
@bot.message_handler(func=lambda m: m.text == '🤖 ساخت ربات جدید')
@error_handler.handle
def new_bot(message):
    user_id = message.from_user.id
    
    if not check_payment(user_id):
        bot.send_message(
            message.chat.id,
            f"❌ ابتدا هزینه را پرداخت کنید\n"
            f"💰 مبلغ: {PRICE:,} تومان\n"
            f"💳 کارت: {CARD_NUMBER}"
        )
        return
    
    can_create, max_bots, current_bots = check_bot_limit(user_id)
    
    if not can_create:
        bot.send_message(
            message.chat.id,
            f"❌ شما به حداکثر تعداد ربات ({max_bots}) رسیده‌اید!\n"
            f"برای ساخت ربات جدید:\n"
            f"1️⃣ یکی از ربات‌ها را حذف کنید\n"
            f"2️⃣ یا با دعوت دوستان ربات اضافه بگیرید"
        )
        return
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🐳 Docker", callback_data="isolate_docker"),
        types.InlineKeyboardButton("🔒 nsjail", callback_data="isolate_nsjail"),
        types.InlineKeyboardButton("🔥 firejail", callback_data="isolate_firejail"),
        types.InlineKeyboardButton("📦 bubblewrap", callback_data="isolate_bubblewrap"),
        types.InlineKeyboardButton("⚡ خودکار", callback_data="isolate_auto"),
    )
    
    bot.send_message(
        message.chat.id,
        "🔒 **روش ایزوله‌سازی را انتخاب کنید:**\n"
        "• Docker: ایزوله کامل با کانتینر\n"
        "• nsjail: سندباکس فوق‌امن\n"
        "• firejail: محدودیت منابع\n"
        "• bubblewrap: سندباکس سبک\n"
        "• خودکار: انتخاب بهترین روش",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('isolate_'))
@error_handler.handle_callback
def select_isolation(call):
    isolation_method = call.data.replace('isolate_', '')
    
    bot.edit_message_text(
        f"✅ روش ایزوله‌سازی **{isolation_method}** انتخاب شد.\n"
        f"📤 فایل خود را آپلود کنید.",
        call.message.chat.id,
        call.message.message_id
    )
    bot.answer_callback_query(call.id)

# ==================== آپلود فایل ====================
@bot.message_handler(content_types=['document'])
@error_handler.handle
def handle_build_file(message):
    user_id = message.from_user.id
    
    if not check_payment(user_id):
        bot.reply_to(message, "❌ ابتدا هزینه را پرداخت کنید")
        return
    
    file_name = message.document.file_name
    
    if not (file_name.endswith('.py') or file_name.endswith('.zip')):
        bot.reply_to(message, "❌ فقط فایل‌های `.py` یا `.zip` مجاز هستند!")
        return
    
    if message.document.file_size > 50 * 1024 * 1024:
        bot.reply_to(message, "❌ حجم فایل نباید بیشتر از ۵۰ مگابایت باشد!")
        return
    
    status_msg = bot.reply_to(message, "🔄 در حال پردازش فایل...")
    
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        safe, threats = security.scan_file_content(downloaded_file)
        if not safe:
            bot.edit_message_text(f"❌ فایل ناامن!\n{threats}", message.chat.id, status_msg.message_id)
            security.log_security_event("UNSAFE_FILE", f"User {user_id} - {file_name}")
            return
        
        file_path = save_uploaded_file(user_id, downloaded_file, file_name)
        if not file_path:
            bot.edit_message_text("❌ خطا در ذخیره فایل", message.chat.id, status_msg.message_id)
            return
        
        main_code = ""
        
        if file_name.endswith('.zip'):
            extract_dir = os.path.join(TEMP_DIR, f"extract_{user_id}_{int(time.time())}")
            os.makedirs(extract_dir, exist_ok=True)
            
            py_files = extract_files_from_zip(file_path, extract_dir, user_id)
            for pf in py_files:
                if pf['name'] in ['bot.py', 'main.py', 'run.py']:
                    main_code = pf['content']
                    break
            
            if not main_code and py_files:
                main_code = py_files[0]['content']
            
            shutil.rmtree(extract_dir, ignore_errors=True)
        else:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    main_code = f.read()
            except:
                with open(file_path, 'r', encoding='cp1256') as f:
                    main_code = f.read()
        
        if not main_code:
            bot.edit_message_text("❌ هیچ فایل پایتونی پیدا نشد!", message.chat.id, status_msg.message_id)
            return
        
        safe_code, code_threats = security.scan_code(main_code)
        if not safe_code:
            bot.edit_message_text(f"❌ کد خطرناک!\n{code_threats}", message.chat.id, status_msg.message_id)
            security.log_security_event("UNSAFE_CODE", f"User {user_id}")
            return
        
        token = extract_token_from_code(main_code)
        if not token:
            bot.edit_message_text("❌ توکن در کد پیدا نشد!", message.chat.id, status_msg.message_id)
            return
        
        try:
            response = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
            if response.status_code != 200:
                bot.edit_message_text("❌ توکن معتبر نیست!", message.chat.id, status_msg.message_id)
                return
            
            bot_info = response.json()['result']
            bot_name = bot_info['first_name']
            bot_username = bot_info['username']
        except Exception as e:
            bot.edit_message_text(f"❌ خطا در بررسی توکن: {str(e)}", message.chat.id, status_msg.message_id)
            return
        
        bot.edit_message_text("⚡ در حال اجرا در محیط ایزوله...", message.chat.id, status_msg.message_id)
        
        bot_id = hashlib.sha3_256(f"{user_id}{token}{time.time()}{secrets.token_hex(8)}".encode()).hexdigest()[:16]
        
        isolation_method = cache_get(f"isolation_{user_id}") or "auto"
        
        result = isolation.run_isolated(bot_id, main_code, token, isolation_method)
        
        if result['success']:
            add_bot(
                user_id, bot_id, token, bot_name, bot_username, file_path, None,
                result.get('pid'), result.get('container_id'), result.get('method'), 0
            )
            
            masked_token = crypto.mask(token)
            
            reply = f"✅ ربات با موفقیت ساخته شد! 🎉\n\n"
            reply += f"🤖 نام: {bot_name}\n"
            reply += f"🔗 لینک: https://t.me/{bot_username}\n"
            reply += f"🆔 آیدی: `{bot_id}`\n"
            reply += f"🔒 ایزوله: {result.get('method', 'simple')}\n"
            
            if 'container_id' in result:
                reply += f"🐳 کانتینر: {result['container_id']}\n"
            if 'pid' in result:
                reply += f"🔄 PID: {result['pid']}\n"
            
            reply += f"🔐 توکن: `{masked_token}`\n"
            
            bot.edit_message_text(reply, message.chat.id, status_msg.message_id, parse_mode="Markdown")
        else:
            error = result.get('error', 'خطای ناشناخته')
            bot.edit_message_text(f"❌ خطا در اجرا:\n{error}", message.chat.id, status_msg.message_id)
        
    except Exception as e:
        logger.error(f"Error: {traceback.format_exc()}")
        bot.edit_message_text(f"❌ خطا: {str(e)}", message.chat.id, status_msg.message_id)

# ==================== ربات‌های من ====================
@bot.message_handler(func=lambda m: m.text == '📋 ربات‌های من')
@error_handler.handle
def my_bots(message):
    user_id = message.from_user.id
    bots = get_user_bots(user_id)
    
    if not bots:
        bot.send_message(message.chat.id, "📋 شما رباتی ندارید!")
        return
    
    for b in bots[:10]:
        status_info = isolation.get_isolated_status(b['id']) if b['isolation_method'] != 'simple' else {'running': False}
        
        status_emoji = "🟢" if status_info.get('running') or b['status'] == 'running' else "🔴"
        status_text = "در حال اجرا" if status_info.get('running') or b['status'] == 'running' else "متوقف"
        
        masked_token = crypto.mask(b['token'])
        
        text = f"{status_emoji} **{b['name']}**\n"
        text += f"🔗 https://t.me/{b['username']}\n"
        text += f"🆔 `{b['id']}`\n"
        text += f"🔒 ایزوله: {b.get('isolation_method', 'simple')}\n"
        text += f"🔐 توکن: `{masked_token}`\n"
        text += f"📊 وضعیت: {status_text}\n"
        
        if status_info.get('running'):
            text += f"💻 CPU: {status_info.get('cpu', 0):.1f}%\n"
            text += f"🧠 RAM: {status_info.get('memory', 0):.1f}%\n"
        
        text += f"📅 {b['created_at'][:10]}\n"
        
        bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== فعال/غیرفعال کردن ====================
@bot.message_handler(func=lambda m: m.text == '🔄 فعال/غیرفعال کردن')
@error_handler.handle
def toggle_prompt(message):
    user_id = message.from_user.id
    bots = get_user_bots(user_id)
    
    if not bots:
        bot.send_message(message.chat.id, "📋 شما رباتی ندارید!")
        return
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    for b in bots:
        status = "🟢" if b['status'] == 'running' else "🔴"
        btn = types.InlineKeyboardButton(
            f"{status} {b['name']}",
            callback_data=f"toggle_{b['id']}"
        )
        markup.add(btn)
    
    bot.send_message(
        message.chat.id,
        "🔄 ربات مورد نظر را انتخاب کنید:",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('toggle_'))
@error_handler.handle_callback
def toggle_bot(call):
    bot_id = call.data.replace('toggle_', '')
    user_id = call.from_user.id
    bot_info = get_bot(bot_id)
    
    if not bot_info or bot_info['user_id'] != user_id:
        bot.answer_callback_query(call.id, "❌ ربات پیدا نشد!")
        return
    
    if bot_info['status'] == 'running':
        if isolation.stop_isolated(bot_id):
            update_bot_status(bot_id, 'stopped')
            bot.answer_callback_query(call.id, "✅ ربات متوقف شد")
            bot.edit_message_text(
                f"✅ ربات {bot_info['name']} متوقف شد.",
                call.message.chat.id,
                call.message.message_id
            )
        else:
            bot.answer_callback_query(call.id, "❌ خطا در توقف!")
    else:
        bot.answer_callback_query(call.id, "❌ ربات فعال نیست")

# ==================== حذف ربات ====================
@bot.message_handler(func=lambda m: m.text == '🗑 حذف ربات')
@error_handler.handle
def delete_prompt(message):
    user_id = message.from_user.id
    bots = get_user_bots(user_id)
    
    if not bots:
        bot.send_message(message.chat.id, "📋 شما رباتی ندارید!")
        return
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    for b in bots:
        btn = types.InlineKeyboardButton(
            f"🗑 {b['name']}",
            callback_data=f"delete_{b['id']}"
        )
        markup.add(btn)
    
    bot.send_message(
        message.chat.id,
        "⚠️ ربات مورد نظر را انتخاب کنید:",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_'))
@error_handler.handle_callback
def confirm_delete(call):
    bot_id = call.data.replace('delete_', '')
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn1 = types.InlineKeyboardButton("✅ بله", callback_data=f"confirm_del_{bot_id}")
    btn2 = types.InlineKeyboardButton("❌ خیر", callback_data="cancel_del")
    markup.add(btn1, btn2)
    
    bot.edit_message_text(
        "⚠️ آیا از حذف این ربات اطمینان دارید؟",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_del_'))
@error_handler.handle_callback
def do_delete(call):
    bot_id = call.data.replace('confirm_del_', '')
    user_id = call.from_user.id
    
    if delete_bot(bot_id, user_id):
        bot.edit_message_text(
            "✅ ربات با موفقیت حذف شد.",
            call.message.chat.id,
            call.message.message_id
        )
    else:
        bot.edit_message_text(
            "❌ خطا در حذف ربات!",
            call.message.chat.id,
            call.message.message_id
        )

@bot.callback_query_handler(func=lambda call: call.data == 'cancel_del')
@error_handler.handle_callback
def cancel_delete(call):
    bot.edit_message_text(
        "❌ عملیات حذف لغو شد.",
        call.message.chat.id,
        call.message.message_id
    )

# ==================== راهنما ====================
@bot.message_handler(func=lambda m: m.text == '📚 راهنما')
@error_handler.handle
def guide(message):
    user = get_user(message.from_user.id) or {'referral_code': ''}
    bot_username = bot.get_me().username
    referral_link = f"https://t.me/{bot_username}?start={user['referral_code']}"
    
    text = (
        "📚 **راهنمای کامل**\n\n"
        "1️⃣ **ساخت ربات:**\n"
        f"   • کارت: `{CARD_NUMBER}`\n"
        "   • فایل خود را آپلود کنید\n"
        "   • ربات در محیط ایزوله اجرا می‌شود\n"
        "   • توکن داخل کد باشه\n\n"
        "2️⃣ **ایزوله‌سازی:**\n"
        "   • Docker: ایزوله کامل با کانتینر\n"
        "   • nsjail: سندباکس فوق‌امن\n"
        "   • firejail: محدودیت منابع\n"
        "   • bubblewrap: سندباکس سبک\n\n"
        "3️⃣ **رفرال:**\n"
        f"   • لینک شما: {referral_link}\n"
        "   • هر ۵ نفر = ۱ ربات اضافه\n\n"
        "4️⃣ **کتابخانه:**\n"
        "   • از منوی نصب کتابخانه استفاده کن\n\n"
        "5️⃣ **پشتیبانی:**\n"
        "   • @shahraghee13"
    )
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== آمار ====================
@bot.message_handler(func=lambda m: m.text == '📊 آمار')
@error_handler.handle
def stats(message):
    try:
        with get_db() as conn:
            total_users = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            total_bots = conn.execute('SELECT COUNT(*) FROM bots').fetchone()[0]
            running_bots = conn.execute('SELECT COUNT(*) FROM bots WHERE status = "running"').fetchone()[0]
            total_payments = conn.execute('SELECT COUNT(*) FROM receipts WHERE status = "approved"').fetchone()[0]
        
        isolation_stats = isolation.get_stats()
        
        text = f"📊 **آمار**\n\n"
        text += f"👥 کاربران: {total_users}\n"
        text += f"🤖 کل ربات‌ها: {total_bots}\n"
        text += f"🟢 فعال: {running_bots}\n"
        text += f"💰 پرداخت‌ها: {total_payments}\n\n"
        text += f"🔒 **ایزوله‌سازی:**\n"
        text += f"🐳 Docker: {isolation_stats['docker']}\n"
        text += f"📦 Sandbox: {isolation_stats['sandbox']}\n"
        
        bot.send_message(message.chat.id, text, parse_mode="Markdown")
    except:
        bot.send_message(message.chat.id, "📊 آمار در دسترس نیست")

# ==================== پشتیبانی ====================
@bot.message_handler(func=lambda m: m.text == '📞 پشتیبانی')
@error_handler.handle
def support(message):
    bot.send_message(
        message.chat.id,
        "📞 **پشتیبانی:** @shahraghee13",
        parse_mode="Markdown"
    )

# ==================== وضعیت موتورها ====================
@bot.message_handler(func=lambda m: m.text == '⚡ وضعیت موتورها')
@error_handler.handle
def engine_status(message):
    isolation_stats = isolation.get_stats()
    
    text = f"⚡ **وضعیت ایزوله‌سازی**\n\n"
    text += f"📊 **روش‌های موجود:**\n"
    
    for method in isolation_stats['methods']:
        text += f"• ✅ {method}\n"
    
    text += f"\n📈 **آمار فعلی:**\n"
    text += f"🐳 کانتینرهای فعال: {isolation_stats['docker']}\n"
    text += f"📦 سندباکس‌های فعال: {isolation_stats['sandbox']}\n"
    
    text += f"\n🛠 **وضعیت:**\n"
    text += f"• Docker: {'✅' if isolation_stats['docker_available'] else '❌'}\n"
    text += f"• nsjail: {'✅' if isolation_stats['nsjail_available'] else '❌'}\n"
    text += f"• firejail: {'✅' if isolation_stats['firejail_available'] else '❌'}\n"
    text += f"• bubblewrap: {'✅' if isolation_stats['bubblewrap_available'] else '❌'}\n"
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== مدیریت فایل‌ها ====================
@bot.message_handler(func=lambda m: m.text == '📁 مدیریت فایل‌ها')
@error_handler.handle
def manage_files(message):
    user_id = message.from_user.id
    user_dir = os.path.join(FILES_DIR, str(user_id))
    
    if not os.path.exists(user_dir):
        os.makedirs(user_dir)
        bot.send_message(message.chat.id, "📁 پوشه فایل‌های شما ایجاد شد.")
    
    files = os.listdir(user_dir)
    
    if not files:
        bot.send_message(message.chat.id, "📁 پوشه شما خالی است.")
        return
    
    text = "📁 **فایل‌های شما:**\n\n"
    for f in files[-20:]:
        f_path = os.path.join(user_dir, f)
        if os.path.isfile(f_path):
            size = os.path.getsize(f_path) / 1024
            modified = datetime.fromtimestamp(os.path.getmtime(f_path)).strftime("%Y-%m-%d %H:%M")
            text += f"• {f} ({size:.1f} KB) - {modified}\n"
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== پنل ادمین ====================
@bot.message_handler(func=lambda m: m.text == '👑 پنل ادمین')
@error_handler.handle
def admin_panel(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ شما دسترسی ادمین ندارید!")
        return
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📸 فیش‌ها", callback_data="admin_receipts"),
        types.InlineKeyboardButton("👥 کاربران", callback_data="admin_users"),
        types.InlineKeyboardButton("📊 آمار کامل", callback_data="admin_stats"),
        types.InlineKeyboardButton("💰 تایید پرداخت", callback_data="admin_approve"),
        types.InlineKeyboardButton("🔒 امنیت", callback_data="admin_security"),
        types.InlineKeyboardButton("📦 ایزوله", callback_data="admin_isolation"),
        types.InlineKeyboardButton("📋 لاگ‌ها", callback_data="admin_logs"),
        types.InlineKeyboardButton("🔙 بازگشت", callback_data="admin_back")
    )
    
    bot.send_message(
        message.chat.id,
        "👑 **پنل مدیریت:**",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# ==================== گزارش امنیتی ====================
@bot.message_handler(func=lambda m: m.text == '🔒 گزارش امنیتی')
@error_handler.handle
def security_report(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ شما دسترسی ادمین ندارید!")
        return
    
    report = security.get_security_report()
    isolation_stats = isolation.get_stats()
    
    text = f"🔒 **گزارش امنیتی**\n\n"
    text += f"📊 **لایه‌های امنیتی:**\n"
    
    for layer, info in report['layers'].items():
        status = "✅" if info['enabled'] else "❌"
        text += f"• {status} {info['name']}\n"
    
    text += f"\n📈 **آمار:**\n"
    text += f"• تهدیدات شناسایی شده: {report['threats_detected']}\n"
    text += f"• تهدیدات مسدود شده: {report['threats_blocked']}\n"
    text += f"• قوانین فایروال: {report['firewall_rules']}\n"
    text += f"• رویدادهای اخیر: {len(report['recent_events'])}\n\n"
    
    text += f"🔒 **ایزوله‌سازی:**\n"
    text += f"• روش‌های موجود: {', '.join(isolation_stats['methods'])}\n"
    text += f"• کانتینرهای فعال: {isolation_stats['docker']}\n"
    text += f"• سندباکس‌های فعال: {isolation_stats['sandbox']}\n"
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== وضعیت ایزوله ====================
@bot.message_handler(func=lambda m: m.text == '📦 وضعیت ایزوله')
@error_handler.handle
def isolation_status(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats = isolation.get_stats()
    
    text = f"📦 **وضعیت ایزوله‌سازی**\n\n"
    text += f"**روش‌های موجود:**\n"
    for method in stats['methods']:
        text += f"• ✅ {method}\n"
    
    text += f"\n**آمار فعلی:**\n"
    text += f"🐳 Docker: {stats['docker']}\n"
    text += f"📦 nsjail: {stats['sandbox']}\n\n"
    
    text += f"**وضعیت سرویس‌ها:**\n"
    text += f"• Docker: {'✅' if stats['docker_available'] else '❌'}\n"
    text += f"• nsjail: {'✅' if stats['nsjail_available'] else '❌'}\n"
    text += f"• firejail: {'✅' if stats['firejail_available'] else '❌'}\n"
    text += f"• bubblewrap: {'✅' if stats['bubblewrap_available'] else '❌'}\n"
    text += f"• cgroups: {'✅' if stats['cgroups_available'] else '❌'}\n"
    text += f"• seccomp: {'✅' if stats['seccomp_available'] else '❌'}\n"
    text += f"• apparmor: {'✅' if stats['apparmor_available'] else '❌'}\n"
    text += f"• selinux: {'✅' if stats['selinux_available'] else '❌'}\n"
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== خطاها ====================
@bot.message_handler(func=lambda m: m.text == '📊 خطاها')
@error_handler.handle
def error_stats(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats = error_handler.get_stats()
    
    text = f"📊 **آمار خطاها**\n\n"
    text += f"❌ کل خطاها: {stats['total']}\n\n"
    text += f"**بر اساس نوع:**\n"
    
    for error_type, count in stats['by_type'].items():
        text += f"• {error_type}: {count}\n"
    
    bot.send_message(message.chat.id, text, parse_mode="Markdown")

# ==================== کال‌بک‌های ادمین ====================
@bot.callback_query_handler(func=lambda call: call.data == "admin_security")
@error_handler.handle_callback
def admin_security(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    report = security.get_security_report()
    
    text = f"🔒 **گزارش امنیتی**\n\n"
    text += f"📊 **آمار:**\n"
    text += f"• تهدیدات شناسایی شده: {report['threats_detected']}\n"
    text += f"• تهدیدات مسدود شده: {report['threats_blocked']}\n"
    text += f"• قوانین فایروال: {report['firewall_rules']}\n\n"
    
    text += f"📋 **رویدادهای اخیر:**\n"
    for event in report['recent_events'][-5:]:
        text += f"• {event['timestamp'][:19]} - {event['type']}\n"
    
    bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_isolation")
@error_handler.handle_callback
def admin_isolation(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    stats = isolation.get_stats()
    
    text = f"📦 **مدیریت ایزوله‌سازی**\n\n"
    text += f"**روش‌های موجود:**\n"
    for method in stats['methods']:
        text += f"• ✅ {method}\n"
    
    text += f"\n**کانتینرهای فعال:**\n"
    for container_id in list(isolation.active_containers.keys())[:10]:
        text += f"• {container_id}\n"
    
    text += f"\n**سندباکس‌های فعال:**\n"
    for jail_id in list(isolation.active_jails.keys())[:10]:
        text += f"• {jail_id}\n"
    
    bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_logs")
@error_handler.handle_callback
def admin_logs(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    log_file = os.path.join(LOGS_DIR, 'mother_bot.log')
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            lines = f.readlines()[-50:]
            text = "📋 **آخرین لاگ‌ها:**\n\n" + ''.join(lines[-20:])
            if len(text) > 4000:
                text = text[:4000] + "..."
            bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
    else:
        bot.send_message(call.message.chat.id, "📋 لاگی وجود ندارد")
    
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_receipts")
@error_handler.handle_callback
def admin_receipts(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    try:
        with get_db() as conn:
            receipts = conn.execute('''
                SELECT * FROM receipts WHERE status = 'pending' ORDER BY created_at DESC
            ''').fetchall()
        
        if not receipts:
            bot.send_message(call.message.chat.id, "📸 فیش در انتظار نیست")
            return
        
        for r in receipts:
            text = f"🆔 {r['id']}\n👤 {r['user_id']}\n💰 {r['amount']:,} تومان\n🆔 {r['payment_code']}"
            
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("✅ تایید", callback_data=f"approve_{r['id']}"),
                types.InlineKeyboardButton("❌ رد", callback_data=f"reject_{r['id']}")
            )
            
            if os.path.exists(r['receipt_path']):
                with open(r['receipt_path'], 'rb') as f:
                    bot.send_photo(call.message.chat.id, f, caption=text, reply_markup=markup)
            else:
                bot.send_message(call.message.chat.id, text, reply_markup=markup)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ خطا: {str(e)}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('approve_'))
@error_handler.handle_callback
def approve_receipt(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    try:
        receipt_id = int(call.data.replace('approve_', ''))
        
        with get_db() as conn:
            receipt = conn.execute('SELECT * FROM receipts WHERE id = ?', (receipt_id,)).fetchone()
            if receipt:
                conn.execute('''
                    UPDATE receipts SET status = ?, reviewed_at = ?, reviewed_by = ?
                    WHERE id = ?
                ''', ('approved', datetime.now().isoformat(), call.from_user.id, receipt_id))
                
                conn.execute('''
                    UPDATE users SET payment_status = ?, payment_date = ?
                    WHERE user_id = ?
                ''', ('approved', datetime.now().isoformat(), receipt['user_id']))
                
                conn.commit()
                
                try:
                    bot.send_message(
                        receipt['user_id'],
                        f"✅ فیش شما تایید شد!\nاکنون می‌توانید ربات بسازید."
                    )
                except:
                    pass
        
        bot.answer_callback_query(call.id, "✅ تایید شد")
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        bot.answer_callback_query(call.id, "❌ خطا")

@bot.callback_query_handler(func=lambda call: call.data.startswith('reject_'))
@error_handler.handle_callback
def reject_receipt(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    try:
        receipt_id = int(call.data.replace('reject_', ''))
        
        with get_db() as conn:
            receipt = conn.execute('SELECT * FROM receipts WHERE id = ?', (receipt_id,)).fetchone()
            if receipt:
                conn.execute('''
                    UPDATE receipts SET status = ?, reviewed_at = ?, reviewed_by = ?
                    WHERE id = ?
                ''', ('rejected', datetime.now().isoformat(), call.from_user.id, receipt_id))
                conn.commit()
                
                try:
                    bot.send_message(
                        receipt['user_id'],
                        f"❌ فیش شما رد شد\nبا پشتیبانی تماس بگیرید: @shahraghee13"
                    )
                except:
                    pass
        
        bot.answer_callback_query(call.id, "❌ رد شد")
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        bot.answer_callback_query(call.id, "❌ خطا")

@bot.callback_query_handler(func=lambda call: call.data == "admin_users")
@error_handler.handle_callback
def admin_users(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    try:
        with get_db() as conn:
            users = conn.execute('''
                SELECT user_id, username, first_name, bots_count, verified_referrals, 
                       payment_status, created_at, security_level
                FROM users ORDER BY created_at DESC LIMIT 20
            ''').fetchall()
        
        text = "👥 **۲۰ کاربر آخر:**\n\n"
        for u in users:
            payment = "✅" if u['payment_status'] == 'approved' else "⏳"
            text += f"{payment} {u['user_id']} - {u['first_name']}\n"
            text += f"   🤖 {u['bots_count']} | 🎁 {u['verified_referrals']} | 🔒 {u['security_level']}\n\n"
        
        bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
    except:
        bot.send_message(call.message.chat.id, "❌ خطا")

@bot.callback_query_handler(func=lambda call: call.data == "admin_stats")
@error_handler.handle_callback
def admin_stats(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    try:
        with get_db() as conn:
            total_users = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            total_bots = conn.execute('SELECT COUNT(*) FROM bots').fetchone()[0]
            running_bots = conn.execute('SELECT COUNT(*) FROM bots WHERE status = "running"').fetchone()[0]
            total_receipts = conn.execute('SELECT COUNT(*) FROM receipts').fetchone()[0]
            pending = conn.execute('SELECT COUNT(*) FROM receipts WHERE status = "pending"').fetchone()[0]
            approved = conn.execute('SELECT COUNT(*) FROM receipts WHERE status = "approved"').fetchone()[0]
            total_amount = conn.execute('SELECT SUM(amount) FROM receipts WHERE status = "approved"').fetchone()[0] or 0
            paid_users = conn.execute('SELECT COUNT(*) FROM users WHERE payment_status = "approved"').fetchone()[0]
        
        isolation_stats = isolation.get_stats()
        security_report = security.get_security_report()
        error_stats = error_handler.get_stats()
        
        text = f"📊 **آمار کامل**\n\n"
        text += f"👥 **کاربران:**\n"
        text += f"• کل: {total_users}\n"
        text += f"• پرداخت کرده: {paid_users}\n"
        text += f"• درصد: {paid_users / total_users * 100:.1f}%\n\n"
        
        text += f"🤖 **ربات‌ها:**\n"
        text += f"• کل: {total_bots}\n"
        text += f"• فعال: {running_bots}\n"
        text += f"• غیرفعال: {total_bots - running_bots}\n\n"
        
        text += f"💰 **مالی:**\n"
        text += f"• کل فیش‌ها: {total_receipts}\n"
        text += f"• در انتظار: {pending}\n"
        text += f"• تایید شده: {approved}\n"
        text += f"• مجموع واریزی: {total_amount:,} تومان\n\n"
        
        text += f"🔒 **امنیت:**\n"
        text += f"• تهدیدات شناسایی شده: {security_report['threats_detected']}\n"
        text += f"• تهدیدات مسدود شده: {security_report['threats_blocked']}\n"
        text += f"• قوانین فایروال: {security_report['firewall_rules']}\n\n"
        
        text += f"📦 **ایزوله‌سازی:**\n"
        text += f"• کانتینرهای فعال: {isolation_stats['docker']}\n"
        text += f"• سندباکس‌های فعال: {isolation_stats['sandbox']}\n\n"
        
        text += f"❌ **خطاها:**\n"
        text += f"• کل: {error_stats['total']}\n"
        
        bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ خطا: {str(e)}")

@bot.callback_query_handler(func=lambda call: call.data == "admin_approve")
@error_handler.handle_callback
def admin_approve_prompt(call):
    if call.from_user.id not in ADMIN_IDS:
        bot.answer_callback_query(call.id, "⛔ دسترسی ندارید!")
        return
    
    msg = bot.send_message(
        call.message.chat.id,
        "💰 **آیدی کاربر را وارد کنید:**"
    )
    bot.register_next_step_handler(msg, process_admin_approve)

def process_admin_approve(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.reply_to(message, "⛔ دسترسی ندارید!")
        return
    
    try:
        user_id = int(message.text.strip())
        
        with get_db() as conn:
            conn.execute('''
                UPDATE users SET payment_status = ?, payment_date = ?
                WHERE user_id = ?
            ''', ('approved', datetime.now().isoformat(), user_id))
            conn.commit()
        
        bot.reply_to(message, f"✅ پرداخت کاربر {user_id} تایید شد")
        
        try:
            bot.send_message(
                user_id,
                f"✅ پرداخت شما تایید شد!\nاکنون می‌توانید ربات بسازید."
            )
        except:
            pass
            
    except ValueError:
        bot.reply_to(message, "❌ آیدی باید عدد باشد")
    except Exception as e:
        bot.reply_to(message, f"❌ خطا: {str(e)}")

@bot.callback_query_handler(func=lambda call: call.data == "admin_back")
@error_handler.handle_callback
def admin_back(call):
    user_id = call.from_user.id
    is_admin = user_id in ADMIN_IDS
    markup = get_main_menu(is_admin)
    
    bot.delete_message(call.message.chat.id, call.message.message_id)
    bot.send_message(call.message.chat.id, "🚀 **منوی اصلی:**", reply_markup=markup, parse_mode="Markdown")

# ==================== مانیتورینگ ====================
def monitor_system():
    while True:
        try:
            with get_db() as conn:
                running_bots = conn.execute('SELECT id, isolation_method FROM bots WHERE status = "running"').fetchall()
                
                for bot in running_bots:
                    if bot['isolation_method'] != 'simple':
                        status = isolation.get_isolated_status(bot['id'])
                        if not status.get('running'):
                            conn.execute('UPDATE bots SET status = ? WHERE id = ?', ('stopped', bot['id']))
                            conn.commit()
                            logger.info(f"⚠️ ربات {bot['id']} متوقف شد")
            
            now = time.time()
            for f in os.listdir(TEMP_DIR):
                f_path = os.path.join(TEMP_DIR, f)
                if os.path.isfile(f_path) and now - os.path.getmtime(f_path) > 3600:
                    try:
                        os.remove(f_path)
                    except:
                        pass
            
            with CACHE_LOCK:
                expired = [k for k, v in CACHE.items() if v[1] < now]
                for k in expired:
                    del CACHE[k]
            
            time.sleep(30)
        except Exception as e:
            logger.error(f"Error in monitor: {e}")
            time.sleep(60)

monitor_thread = threading.Thread(target=monitor_system, daemon=True)
monitor_thread.start()

# ==================== اجرا ====================
if __name__ == "__main__":
    print("=" * 80)
    print("🚀 ربات مادر نهایی - نسخه 14.0 ایزوله اولترا")
    print("=" * 80)
    print("✅ ویژگی‌های جدید:")
    print("   • ایزوله‌سازی کامل با Docker/nsjail/firejail/bubblewrap")
    print("   • رمزنگاری چندلایه AES-256 + ChaCha20")
    print("   • سیستم امنیتی ۱۰ لایه")
    print("   • محدودیت منابع با cgroups")
    print("   • محدودیت syscall با seccomp")
    print("   • پروفایل apparmor/selinux")
    print("   • تشخیص تهدید با IDS/IPS")
    print("   • مانیتورینگ لحظه‌ای")
    print("=" * 80)
    print(f"✅ زیرموتورها: ۵۰۰")
    print(f"✅ ظرفیت کل: ۵۰۰,۰۰۰ ربات")
    print(f"✅ Thread Pool: ۱۰۰۰۰")
    print(f"✅ رمزنگاری: فعال (۳ لایه)")
    print(f"✅ ایزوله‌سازی: فعال")
    print(f"✅ امنیت: فعال (۱۰ لایه)")
    print(f"✅ ادمین: {ADMIN_IDS}")
    print("=" * 80)
    
    while True:
        try:
            bot.infinity_polling(timeout=60)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)
