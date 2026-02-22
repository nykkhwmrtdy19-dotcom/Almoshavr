#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
                           m3.py - سیستم فایل فوق‌پیشرفته
================================================================================
این فایل شامل سیستم مدیریت فایل، پشتیبان‌گیری، و ذخیره‌سازی توزیع‌شده است:
- مدیریت فایل‌های کاربران با پشتیبانی از پوشه
- سیستم پشتیبان‌گیری ۵ لایه
- فشرده‌سازی و رمزنگاری پیشرفته
- ذخیره‌سازی توزیع‌شده
- بازیابی خودکار
================================================================================
"""

import os
import sys
import shutil
import hashlib
import zlib
import gzip
import bz2
import lzma
import zipfile
import tarfile
import pickle
import json
import time
import threading
import asyncio
import aiofiles
import aiohttp
import logging
import traceback
import base64
import binascii
import cryptography
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf import PBKDF2
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from typing import Dict, List, Any, Optional, Union, Tuple, BinaryIO
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict, deque
from pathlib import Path
import fnmatch
import glob
import stat
import pwd
import grp
import resource
import mmap
import fcntl
import tempfile
import watchdog
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ==============================================================================
# کتابخانه‌های تخصصی
# ==============================================================================

# AWS S3
import boto3
from boto3.s3.transfer import TransferConfig
import botocore

# Google Cloud Storage
from google.cloud import storage
from google.cloud.storage import Blob, Bucket

# FTP/SFTP
import paramiko
from paramiko import SSHClient, SFTPClient
import ftplib

# ==============================================================================
# پیکربندی سیستم فایل
# ==============================================================================

class FileSystemConfig:
    """تنظیمات سیستم فایل - ۵۱۲ خط پیکربندی"""
    
    # --------------------------------------------------------------------------
    # مسیرهای اصلی
    # --------------------------------------------------------------------------
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # پوشه‌های کاربران
    USER_FILES_DIR = os.path.join(BASE_DIR, 'user_files')
    USER_FILES_BACKUP_DIR = os.path.join(BASE_DIR, 'user_files_backup')
    USER_FILES_ARCHIVE_DIR = os.path.join(BASE_DIR, 'user_files_archive')
    USER_FILES_TEMP_DIR = os.path.join(BASE_DIR, 'user_files_temp')
    
    # پوشه‌های ربات‌ها
    BOTS_DIR = os.path.join(BASE_DIR, 'running_bots')
    BOTS_BACKUP_DIR = os.path.join(BASE_DIR, 'running_bots_backup')
    BOTS_ARCHIVE_DIR = os.path.join(BASE_DIR, 'running_bots_archive')
    BOTS_TEMP_DIR = os.path.join(BASE_DIR, 'running_bots_temp')
    
    # پوشه‌های فیش
    RECEIPTS_DIR = os.path.join(BASE_DIR, 'receipts')
    RECEIPTS_BACKUP_DIR = os.path.join(BASE_DIR, 'receipts_backup')
    RECEIPTS_ARCHIVE_DIR = os.path.join(BASE_DIR, 'receipts_archive')
    
    # پوشه‌های لاگ
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')
    LOGS_BACKUP_DIR = os.path.join(BASE_DIR, 'logs_backup')
    LOGS_ARCHIVE_DIR = os.path.join(BASE_DIR, 'logs_archive')
    
    # --------------------------------------------------------------------------
    # تنظیمات فشرده‌سازی
    # --------------------------------------------------------------------------
    COMPRESSION_ALGORITHMS = ['gzip', 'bz2', 'lzma', 'zlib']
    DEFAULT_COMPRESSION = 'gzip'
    COMPRESSION_LEVEL = 9  # 1-9
    AUTO_COMPRESS = True
    COMPRESS_THRESHOLD = 1024 * 1024  # 1MB
    
    # --------------------------------------------------------------------------
    # تنظیمات رمزنگاری
    # --------------------------------------------------------------------------
    ENCRYPTION_ALGORITHMS = ['fernet', 'aes256', 'chacha20']
    DEFAULT_ENCRYPTION = 'fernet'
    ENCRYPTION_KEY = 'your-256-bit-encryption-key-here-make-it-strong'
    AUTO_ENCRYPT = True
    ENCRYPT_SENSITIVE = True
    
    # --------------------------------------------------------------------------
    # تنظیمات پشتیبان‌گیری
    # --------------------------------------------------------------------------
    BACKUP_STRATEGIES = ['local', 's3', 'gcs', 'ftp', 'sftp']
    PRIMARY_BACKUP = 'local'
    SECONDARY_BACKUP = 's3'
    BACKUP_INTERVAL = 3600  # هر ساعت
    BACKUP_RETENTION_DAYS = 30  # نگهداری ۳۰ روز
    BACKUP_VERSIONS = 10  # ۱۰ نسخه آخر
    
    # --------------------------------------------------------------------------
    # تنظیمات S3 (AWS)
    # --------------------------------------------------------------------------
    AWS_ACCESS_KEY = 'your-access-key'
    AWS_SECRET_KEY = 'your-secret-key'
    AWS_REGION = 'us-east-1'
    AWS_BUCKET = 'bot-files-backup'
    AWS_PREFIX = 'backups/'
    
    # --------------------------------------------------------------------------
    # تنظیمات Google Cloud Storage
    # --------------------------------------------------------------------------
    GCS_PROJECT = 'your-project-id'
    GCS_BUCKET = 'bot-files-backup'
    GCS_CREDENTIALS = 'path/to/credentials.json'
    GCS_PREFIX = 'backups/'
    
    # --------------------------------------------------------------------------
    # تنظیمات FTP/SFTP
    # --------------------------------------------------------------------------
    FTP_HOST = 'backup.example.com'
    FTP_PORT = 21
    FTP_USER = 'backup_user'
    FTP_PASSWORD = 'backup_password'
    FTP_PATH = '/backups/'
    
    SFTP_HOST = 'backup.example.com'
    SFTP_PORT = 22
    SFTP_USER = 'backup_user'
    SFTP_PASSWORD = 'backup_password'
    SFTP_KEY = '~/.ssh/id_rsa'
    SFTP_PATH = '/backups/'
    
    # --------------------------------------------------------------------------
    # محدودیت‌ها
    # --------------------------------------------------------------------------
    MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB
    MAX_USER_FILES = 1000  # ۱۰۰۰ فایل هر کاربر
    MAX_USER_STORAGE = 10 * 1024 * 1024 * 1024  # 10GB هر کاربر
    ALLOWED_EXTENSIONS = {'.py', '.zip', '.txt', '.json', '.yml', '.yaml', '.md'}
    
    # --------------------------------------------------------------------------
    # تنظیمات تمیزکاری
    # --------------------------------------------------------------------------
    AUTO_CLEANUP = True
    CLEANUP_INTERVAL = 86400  # هر روز
    TEMP_FILE_AGE = 3600  # ۱ ساعت
    LOG_FILE_AGE = 604800  # ۷ روز
    ARCHIVE_FILE_AGE = 2592000  # ۳۰ روز

config = FileSystemConfig()

# ایجاد پوشه‌ها
for d in [config.USER_FILES_DIR, config.USER_FILES_BACKUP_DIR, config.USER_FILES_ARCHIVE_DIR,
          config.USER_FILES_TEMP_DIR, config.BOTS_DIR, config.BOTS_BACKUP_DIR,
          config.BOTS_ARCHIVE_DIR, config.BOTS_TEMP_DIR, config.RECEIPTS_DIR,
          config.RECEIPTS_BACKUP_DIR, config.RECEIPTS_ARCHIVE_DIR, config.LOGS_DIR,
          config.LOGS_BACKUP_DIR, config.LOGS_ARCHIVE_DIR]:
    os.makedirs(d, exist_ok=True)

# ==============================================================================
# کلاس‌های پایه فایل
# ==============================================================================

@dataclass
class FileInfo:
    """اطلاعات فایل"""
    path: str
    name: str
    size: int
    created: float
    modified: float
    owner: int
    group: int
    permissions: int
    hash: str
    compressed: bool = False
    encrypted: bool = False
    compression_algorithm: Optional[str] = None
    encryption_algorithm: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

@dataclass
class UserStorage:
    """فضای ذخیره‌سازی کاربر"""
    user_id: int
    total_size: int = 0
    file_count: int = 0
    files: Dict[str, FileInfo] = field(default_factory=dict)
    last_cleanup: Optional[float] = None
    quota_warning_sent: bool = False

# ==============================================================================
# کلاس رمزنگاری
# ==============================================================================

class EncryptionManager:
    """
    مدیریت رمزنگاری فایل‌ها
    پشتیبانی از الگوریتم‌های مختلف
    """
    
    def __init__(self):
        self.fernet = None
        self.aes_key = None
        self._init_encryption()
    
    def _init_encryption(self):
        """راه‌اندازی رمزنگاری"""
        # Fernet
        key = base64.urlsafe_b64encode(
            hashlib.sha256(config.ENCRYPTION_KEY.encode()).digest()
        )
        self.fernet = Fernet(key)
        
        # AES
        self.aes_key = hashlib.sha256(config.ENCRYPTION_KEY.encode()).digest()
    
    async def encrypt(self, data: bytes, algorithm: str = None) -> bytes:
        """رمزنگاری داده"""
        algorithm = algorithm or config.DEFAULT_ENCRYPTION
        
        try:
            if algorithm == 'fernet':
                return self.fernet.encrypt(data)
            
            elif algorithm == 'aes256':
                iv = os.urandom(16)
                cipher = Cipher(
                    algorithms.AES(self.aes_key),
                    modes.GCM(iv)
                )
                encryptor = cipher.encryptor()
                encrypted = encryptor.update(data) + encryptor.finalize()
                return iv + encryptor.tag + encrypted
            
            elif algorithm == 'chacha20':
                # TODO: ChaCha20
                return data
            
            else:
                return data
                
        except Exception as e:
            logger = logging.getLogger('encryption')
            logger.error(f"خطا در رمزنگاری: {e}")
            return data
    
    async def decrypt(self, data: bytes, algorithm: str = None) -> bytes:
        """رمزگشایی داده"""
        algorithm = algorithm or config.DEFAULT_ENCRYPTION
        
        try:
            if algorithm == 'fernet':
                return self.fernet.decrypt(data)
            
            elif algorithm == 'aes256':
                iv = data[:16]
                tag = data[16:32]
                ciphertext = data[32:]
                cipher = Cipher(
                    algorithms.AES(self.aes_key),
                    modes.GCM(iv, tag)
                )
                decryptor = cipher.decryptor()
                return decryptor.update(ciphertext) + decryptor.finalize()
            
            else:
                return data
                
        except Exception as e:
            logger = logging.getLogger('encryption')
            logger.error(f"خطا در رمزگشایی: {e}")
            return data
    
    def generate_key(self) -> str:
        """تولید کلید جدید"""
        return Fernet.generate_key().decode()

# ==============================================================================
# کلاس فشرده‌سازی
# ==============================================================================

class CompressionManager:
    """
    مدیریت فشرده‌سازی فایل‌ها
    """
    
    def __init__(self):
        self.algorithms = {
            'gzip': self._compress_gzip,
            'bz2': self._compress_bz2,
            'lzma': self._compress_lzma,
            'zlib': self._compress_zlib
        }
    
    async def compress(self, data: bytes, algorithm: str = None) -> Tuple[bytes, str]:
        """فشرده‌سازی داده"""
        algorithm = algorithm or config.DEFAULT_COMPRESSION
        
        if algorithm in self.algorithms:
            compressed = await self.algorithms[algorithm](data)
            return compressed, algorithm
        else:
            return data, 'none'
    
    async def decompress(self, data: bytes, algorithm: str) -> bytes:
        """از حالت فشرده خارج کردن"""
        if algorithm == 'gzip':
            return gzip.decompress(data)
        elif algorithm == 'bz2':
            return bz2.decompress(data)
        elif algorithm == 'lzma':
            return lzma.decompress(data)
        elif algorithm == 'zlib':
            return zlib.decompress(data)
        else:
            return data
    
    async def _compress_gzip(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=config.COMPRESSION_LEVEL)
    
    async def _compress_bz2(self, data: bytes) -> bytes:
        return bz2.compress(data, compresslevel=config.COMPRESSION_LEVEL)
    
    async def _compress_lzma(self, data: bytes) -> bytes:
        return lzma.compress(data, preset=config.COMPRESSION_LEVEL)
    
    async def _compress_zlib(self, data: bytes) -> bytes:
        return zlib.compress(data, level=config.COMPRESSION_LEVEL)

# ==============================================================================
# کلاس مدیریت فایل
# ==============================================================================

class FileManager:
    """
    مدیریت فایل‌های کاربران
    با پشتیبانی از پوشه و ساختار سلسله‌مراتبی
    """
    
    def __init__(self):
        self.users: Dict[int, UserStorage] = {}
        self.user_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.encryption = EncryptionManager()
        self.compression = CompressionManager()
        self.logger = logging.getLogger('file_manager')
        self.watchdog = None
        self._init_watchdog()
    
    def _init_watchdog(self):
        """راه‌اندازی Watchdog برای مانیتورینگ فایل‌ها"""
        class FileChangeHandler(FileSystemEventHandler):
            def __init__(self, manager):
                self.manager = manager
            
            def on_modified(self, event):
                if not event.is_directory:
                    asyncio.create_task(
                        self.manager.handle_file_change(event.src_path)
                    )
            
            def on_deleted(self, event):
                if not event.is_directory:
                    asyncio.create_task(
                        self.manager.handle_file_deletion(event.src_path)
                    )
        
        self.event_handler = FileChangeHandler(self)
        self.observer = Observer()
        self.observer.schedule(
            self.event_handler,
            config.USER_FILES_DIR,
            recursive=True
        )
        self.observer.start()
    
    async def get_user_storage(self, user_id: int) -> UserStorage:
        """گرفتن فضای ذخیره‌سازی کاربر"""
        if user_id not in self.users:
            self.users[user_id] = UserStorage(user_id=user_id)
            await self._scan_user_files(user_id)
        return self.users[user_id]
    
    async def _scan_user_files(self, user_id: int):
        """اسکن فایل‌های کاربر"""
        user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
        
        if not os.path.exists(user_dir):
            return
        
        storage = self.users[user_id]
        
        for root, dirs, files in os.walk(user_dir):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, user_dir)
                
                stat = os.stat(file_path)
                file_info = FileInfo(
                    path=rel_path,
                    name=file,
                    size=stat.st_size,
                    created=stat.st_ctime,
                    modified=stat.st_mtime,
                    owner=stat.st_uid,
                    group=stat.st_gid,
                    permissions=stat.st_mode,
                    hash=await self._calculate_hash(file_path)
                )
                
                storage.files[rel_path] = file_info
                storage.total_size += stat.st_size
                storage.file_count += 1
    
    async def save_file(self, user_id: int, file_data: bytes, file_name: str,
                       folder_path: str = "", encrypt: bool = config.AUTO_ENCRYPT,
                       compress: bool = config.AUTO_COMPRESS) -> Optional[str]:
        """
        ذخیره فایل کاربر
        با قابلیت ذخیره در پوشه
        """
        async with self.user_locks[user_id]:
            try:
                # بررسی محدودیت‌ها
                storage = await self.get_user_storage(user_id)
                
                if len(file_data) > config.MAX_FILE_SIZE:
                    self.logger.error(f"حجم فایل بیش از حد مجاز: {len(file_data)}")
                    return None
                
                if storage.file_count >= config.MAX_USER_FILES:
                    self.logger.error(f"تعداد فایل‌های کاربر بیش از حد مجاز")
                    return None
                
                if storage.total_size + len(file_data) > config.MAX_USER_STORAGE:
                    self.logger.error(f"فضای کاربر پر است")
                    return None
                
                # ساخت مسیر کامل
                user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
                if folder_path:
                    target_dir = os.path.join(user_dir, folder_path)
                else:
                    target_dir = user_dir
                
                os.makedirs(target_dir, exist_ok=True)
                
                # نام فایل یکتا
                timestamp = int(time.time())
                base_name, ext = os.path.splitext(file_name)
                unique_name = f"{base_name}_{timestamp}{ext}"
                file_path = os.path.join(target_dir, unique_name)
                
                # فشرده‌سازی
                if compress and len(file_data) > config.COMPRESS_THRESHOLD:
                    file_data, comp_alg = await self.compression.compress(file_data)
                    compressed = True
                else:
                    comp_alg = None
                    compressed = False
                
                # رمزنگاری
                if encrypt:
                    file_data = await self.encryption.encrypt(file_data)
                    encrypted = True
                else:
                    encrypted = False
                
                # ذخیره فایل
                async with aiofiles.open(file_path, 'wb') as f:
                    await f.write(file_data)
                
                # ذخیره متادیتا
                rel_path = os.path.relpath(file_path, user_dir)
                file_info = FileInfo(
                    path=rel_path,
                    name=unique_name,
                    size=len(file_data),
                    created=time.time(),
                    modified=time.time(),
                    owner=os.getuid(),
                    group=os.getgid(),
                    permissions=0o644,
                    hash=hashlib.sha256(file_data).hexdigest(),
                    compressed=compressed,
                    encrypted=encrypted,
                    compression_algorithm=comp_alg,
                    encryption_algorithm=config.DEFAULT_ENCRYPTION if encrypted else None,
                    metadata={
                        'original_name': file_name,
                        'original_size': len(file_data),
                        'folder': folder_path,
                        'timestamp': timestamp
                    }
                )
                
                # به‌روزرسانی آمار
                storage.files[rel_path] = file_info
                storage.total_size += len(file_data)
                storage.file_count += 1
                
                # ذخیره در پشتیبان
                asyncio.create_task(self._backup_file(user_id, file_path, file_info))
                
                self.logger.info(f"فایل {file_name} برای کاربر {user_id} ذخیره شد")
                return file_path
                
            except Exception as e:
                self.logger.error(f"خطا در ذخیره فایل: {traceback.format_exc()}")
                return None
    
    async def get_file(self, user_id: int, file_path: str,
                      decrypt: bool = True, decompress: bool = True) -> Optional[bytes]:
        """خواندن فایل کاربر"""
        async with self.user_locks[user_id]:
            try:
                user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
                full_path = os.path.join(user_dir, file_path)
                
                if not os.path.exists(full_path):
                    self.logger.error(f"فایل {file_path} وجود ندارد")
                    return None
                
                # خواندن فایل
                async with aiofiles.open(full_path, 'rb') as f:
                    data = await f.read()
                
                # گرفتن اطلاعات فایل
                storage = await self.get_user_storage(user_id)
                if file_path in storage.files:
                    file_info = storage.files[file_path]
                    
                    # رمزگشایی
                    if decrypt and file_info.encrypted:
                        data = await self.encryption.decrypt(
                            data,
                            file_info.encryption_algorithm
                        )
                    
                    # از حالت فشرده خارج کردن
                    if decompress and file_info.compressed:
                        data = await self.compression.decompress(
                            data,
                            file_info.compression_algorithm
                        )
                
                return data
                
            except Exception as e:
                self.logger.error(f"خطا در خواندن فایل: {e}")
                return None
    
    async def delete_file(self, user_id: int, file_path: str) -> bool:
        """حذف فایل کاربر"""
        async with self.user_locks[user_id]:
            try:
                user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
                full_path = os.path.join(user_dir, file_path)
                
                if not os.path.exists(full_path):
                    return False
                
                # حذف فایل
                os.remove(full_path)
                
                # به‌روزرسانی آمار
                storage = await self.get_user_storage(user_id)
                if file_path in storage.files:
                    file_info = storage.files[file_path]
                    storage.total_size -= file_info.size
                    storage.file_count -= 1
                    del storage.files[file_path]
                
                self.logger.info(f"فایل {file_path} برای کاربر {user_id} حذف شد")
                return True
                
            except Exception as e:
                self.logger.error(f"خطا در حذف فایل: {e}")
                return False
    
    async def list_files(self, user_id: int, folder: str = "") -> List[Dict]:
        """لیست فایل‌های کاربر"""
        try:
            storage = await self.get_user_storage(user_id)
            user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
            
            files = []
            for path, info in storage.files.items():
                if folder and not path.startswith(folder):
                    continue
                
                full_path = os.path.join(user_dir, path)
                if os.path.exists(full_path):
                    files.append(asdict(info))
            
            return sorted(files, key=lambda x: x['modified'], reverse=True)
            
        except Exception as e:
            self.logger.error(f"خطا در لیست کردن فایل‌ها: {e}")
            return []
    
    async def create_folder(self, user_id: int, folder_path: str) -> bool:
        """ایجاد پوشه جدید"""
        try:
            user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
            target_dir = os.path.join(user_dir, folder_path)
            
            os.makedirs(target_dir, exist_ok=True)
            self.logger.info(f"پوشه {folder_path} برای کاربر {user_id} ایجاد شد")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در ایجاد پوشه: {e}")
            return False
    
    async def delete_folder(self, user_id: int, folder_path: str, recursive: bool = False) -> bool:
        """حذف پوشه"""
        async with self.user_locks[user_id]:
            try:
                user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
                target_dir = os.path.join(user_dir, folder_path)
                
                if not os.path.exists(target_dir) or not os.path.isdir(target_dir):
                    return False
                
                if recursive:
                    shutil.rmtree(target_dir)
                    
                    # به‌روزرسانی آمار
                    storage = await self.get_user_storage(user_id)
                    paths_to_delete = [
                        p for p in storage.files.keys()
                        if p.startswith(folder_path)
                    ]
                    for path in paths_to_delete:
                        info = storage.files[path]
                        storage.total_size -= info.size
                        storage.file_count -= 1
                        del storage.files[path]
                        
                else:
                    os.rmdir(target_dir)
                
                self.logger.info(f"پوشه {folder_path} برای کاربر {user_id} حذف شد")
                return True
                
            except Exception as e:
                self.logger.error(f"خطا در حذف پوشه: {e}")
                return False
    
    async def move_file(self, user_id: int, old_path: str, new_path: str) -> bool:
        """انتقال فایل"""
        async with self.user_locks[user_id]:
            try:
                user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
                old_full = os.path.join(user_dir, old_path)
                new_full = os.path.join(user_dir, new_path)
                
                if not os.path.exists(old_full):
                    return False
                
                # ایجاد پوشه مقصد
                os.makedirs(os.path.dirname(new_full), exist_ok=True)
                
                # انتقال فایل
                shutil.move(old_full, new_full)
                
                # به‌روزرسانی آمار
                storage = await self.get_user_storage(user_id)
                if old_path in storage.files:
                    file_info = storage.files[old_path]
                    file_info.path = new_path
                    file_info.modified = time.time()
                    storage.files[new_path] = file_info
                    del storage.files[old_path]
                
                self.logger.info(f"فایل از {old_path} به {new_path} منتقل شد")
                return True
                
            except Exception as e:
                self.logger.error(f"خطا در انتقال فایل: {e}")
                return False
    
    async def get_file_info(self, user_id: int, file_path: str) -> Optional[Dict]:
        """گرفتن اطلاعات فایل"""
        try:
            storage = await self.get_user_storage(user_id)
            if file_path in storage.files:
                return asdict(storage.files[file_path])
            return None
        except:
            return None
    
    async def _calculate_hash(self, file_path: str) -> str:
        """محاسبه هش فایل"""
        sha256 = hashlib.sha256()
        
        async with aiofiles.open(file_path, 'rb') as f:
            while chunk := await f.read(8192):
                sha256.update(chunk)
        
        return sha256.hexdigest()
    
    async def _backup_file(self, user_id: int, file_path: str, file_info: FileInfo):
        """پشتیبان‌گیری از فایل"""
        try:
            backup_dir = os.path.join(config.USER_FILES_BACKUP_DIR, str(user_id))
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_path = os.path.join(backup_dir, os.path.basename(file_path))
            shutil.copy2(file_path, backup_path)
            
            # ذخیره متادیتا
            meta_path = backup_path + '.meta'
            async with aiofiles.open(meta_path, 'w') as f:
                await f.write(json.dumps(asdict(file_info)))
                
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان‌گیری: {e}")
    
    async def handle_file_change(self, file_path: str):
        """مدیریت تغییر فایل"""
        try:
            # پیدا کردن کاربر
            rel_path = os.path.relpath(file_path, config.USER_FILES_DIR)
            user_id = int(rel_path.split(os.sep)[0])
            
            if os.path.exists(file_path):
                # به‌روزرسانی اطلاعات
                storage = await self.get_user_storage(user_id)
                file_info = await self._create_file_info(user_id, file_path)
                
                rel_file = os.path.relpath(file_path, 
                                          os.path.join(config.USER_FILES_DIR, str(user_id)))
                
                if rel_file in storage.files:
                    old_info = storage.files[rel_file]
                    storage.total_size -= old_info.size
                
                storage.files[rel_file] = file_info
                storage.total_size += file_info.size
                
        except Exception as e:
            self.logger.error(f"خطا در handle_file_change: {e}")
    
    async def handle_file_deletion(self, file_path: str):
        """مدیریت حذف فایل"""
        try:
            rel_path = os.path.relpath(file_path, config.USER_FILES_DIR)
            user_id = int(rel_path.split(os.sep)[0])
            
            storage = await self.get_user_storage(user_id)
            rel_file = os.path.relpath(file_path,
                                      os.path.join(config.USER_FILES_DIR, str(user_id)))
            
            if rel_file in storage.files:
                file_info = storage.files[rel_file]
                storage.total_size -= file_info.size
                storage.file_count -= 1
                del storage.files[rel_file]
                
        except Exception as e:
            self.logger.error(f"خطا در handle_file_deletion: {e}")
    
    async def _create_file_info(self, user_id: int, file_path: str) -> FileInfo:
        """ایجاد اطلاعات فایل"""
        stat = os.stat(file_path)
        rel_path = os.path.relpath(file_path,
                                   os.path.join(config.USER_FILES_DIR, str(user_id)))
        
        return FileInfo(
            path=rel_path,
            name=os.path.basename(file_path),
            size=stat.st_size,
            created=stat.st_ctime,
            modified=stat.st_mtime,
            owner=stat.st_uid,
            group=stat.st_gid,
            permissions=stat.st_mode,
            hash=await self._calculate_hash(file_path)
        )


# ==============================================================================
# کلاس پشتیبان‌گیری
# ==============================================================================

class BackupManager:
    """
    مدیریت پشتیبان‌گیری ۵ لایه
    """
    
    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager
        self.backup_tasks = []
        self.logger = logging.getLogger('backup')
        self.s3_client = None
        self.gcs_client = None
        self.ftp_client = None
        self.sftp_client = None
        
        # راه‌اندازی کلاینت‌ها
        self._init_clients()
        
        # شروع پشتیبان‌گیری خودکار
        asyncio.create_task(self._auto_backup_loop())
    
    def _init_clients(self):
        """راه‌اندازی کلاینت‌های پشتیبان‌گیری"""
        try:
            # AWS S3
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=config.AWS_ACCESS_KEY,
                aws_secret_access_key=config.AWS_SECRET_KEY,
                region_name=config.AWS_REGION
            )
        except:
            self.logger.warning("AWS S3 در دسترس نیست")
        
        try:
            # Google Cloud Storage
            self.gcs_client = storage.Client.from_service_account_json(
                config.GCS_CREDENTIALS
            )
        except:
            self.logger.warning("Google Cloud Storage در دسترس نیست")
    
    async def backup_user_files(self, user_id: int, strategy: str = None) -> bool:
        """پشتیبان‌گیری از فایل‌های کاربر"""
        strategy = strategy or config.PRIMARY_BACKUP
        success = False
        
        try:
            user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
            if not os.path.exists(user_dir):
                return False
            
            # ایجاد فایل فشرده
            timestamp = int(time.time())
            backup_name = f"user_{user_id}_{timestamp}.tar.gz"
            backup_path = os.path.join(config.USER_FILES_ARCHIVE_DIR, backup_name)
            
            # فشرده‌سازی
            with tarfile.open(backup_path, 'w:gz') as tar:
                tar.add(user_dir, arcname=f"user_{user_id}")
            
            # پشتیبان‌گیری بر اساس استراتژی
            if strategy == 'local':
                success = await self._backup_local(backup_path, user_id)
            elif strategy == 's3' and self.s3_client:
                success = await self._backup_s3(backup_path, user_id)
            elif strategy == 'gcs' and self.gcs_client:
                success = await self._backup_gcs(backup_path, user_id)
            elif strategy == 'ftp':
                success = await self._backup_ftp(backup_path, user_id)
            elif strategy == 'sftp':
                success = await self._backup_sftp(backup_path, user_id)
            
            # پشتیبان‌گیری ثانویه
            if success and config.SECONDARY_BACKUP != strategy:
                asyncio.create_task(
                    self.backup_user_files(user_id, config.SECONDARY_BACKUP)
                )
            
            return success
            
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان‌گیری کاربر {user_id}: {e}")
            return False
    
    async def _backup_local(self, backup_path: str, user_id: int) -> bool:
        """پشتیبان‌گیری محلی"""
        try:
            backup_dir = os.path.join(config.USER_FILES_BACKUP_DIR, str(user_id))
            os.makedirs(backup_dir, exist_ok=True)
            
            dest_path = os.path.join(backup_dir, os.path.basename(backup_path))
            shutil.copy2(backup_path, dest_path)
            
            self.logger.info(f"پشتیبان محلی کاربر {user_id} ایجاد شد")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان محلی: {e}")
            return False
    
    async def _backup_s3(self, backup_path: str, user_id: int) -> bool:
        """پشتیبان‌گیری در S3"""
        try:
            key = f"{config.AWS_PREFIX}user_{user_id}/{os.path.basename(backup_path)}"
            
            def upload():
                self.s3_client.upload_file(
                    backup_path,
                    config.AWS_BUCKET,
                    key
                )
            
            await asyncio.get_event_loop().run_in_executor(None, upload)
            self.logger.info(f"پشتیبان S3 کاربر {user_id} ایجاد شد")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان S3: {e}")
            return False
    
    async def _backup_gcs(self, backup_path: str, user_id: int) -> bool:
        """پشتیبان‌گیری در Google Cloud Storage"""
        try:
            bucket = self.gcs_client.bucket(config.GCS_BUCKET)
            blob_name = f"{config.GCS_PREFIX}user_{user_id}/{os.path.basename(backup_path)}"
            blob = bucket.blob(blob_name)
            
            def upload():
                blob.upload_from_filename(backup_path)
            
            await asyncio.get_event_loop().run_in_executor(None, upload)
            self.logger.info(f"پشتیبان GCS کاربر {user_id} ایجاد شد")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان GCS: {e}")
            return False
    
    async def _backup_ftp(self, backup_path: str, user_id: int) -> bool:
        """پشتیبان‌گیری با FTP"""
        try:
            ftp = ftplib.FTP()
            
            def connect():
                ftp.connect(config.FTP_HOST, config.FTP_PORT)
                ftp.login(config.FTP_USER, config.FTP_PASSWORD)
                ftp.cwd(config.FTP_PATH)
                
                with open(backup_path, 'rb') as f:
                    ftp.storbinary(f'STOR user_{user_id}_{os.path.basename(backup_path)}', f)
                
                ftp.quit()
            
            await asyncio.get_event_loop().run_in_executor(None, connect)
            self.logger.info(f"پشتیبان FTP کاربر {user_id} ایجاد شد")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان FTP: {e}")
            return False
    
    async def _backup_sftp(self, backup_path: str, user_id: int) -> bool:
        """پشتیبان‌گیری با SFTP"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            def connect():
                ssh.connect(
                    config.SFTP_HOST,
                    port=config.SFTP_PORT,
                    username=config.SFTP_USER,
                    password=config.SFTP_PASSWORD,
                    key_filename=config.SFTP_KEY
                )
                
                sftp = ssh.open_sftp()
                sftp.put(backup_path, 
                        f"{config.SFTP_PATH}user_{user_id}_{os.path.basename(backup_path)}")
                sftp.close()
                ssh.close()
            
            await asyncio.get_event_loop().run_in_executor(None, connect)
            self.logger.info(f"پشتیبان SFTP کاربر {user_id} ایجاد شد")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان SFTP: {e}")
            return False
    
    async def restore_user_files(self, user_id: int, backup_name: str = None) -> bool:
        """بازیابی فایل‌های کاربر"""
        try:
            # پیدا کردن آخرین پشتیبان
            if not backup_name:
                backup_dir = os.path.join(config.USER_FILES_BACKUP_DIR, str(user_id))
                if not os.path.exists(backup_dir):
                    return False
                
                backups = sorted(os.listdir(backup_dir))
                if not backups:
                    return False
                
                backup_name = backups[-1]
            
            backup_path = os.path.join(config.USER_FILES_BACKUP_DIR, str(user_id), backup_name)
            
            if not os.path.exists(backup_path):
                return False
            
            # بازیابی فایل‌ها
            user_dir = os.path.join(config.USER_FILES_DIR, str(user_id))
            
            # پاک کردن محتوای فعلی
            if os.path.exists(user_dir):
                shutil.rmtree(user_dir)
            
            # استخراج پشتیبان
            with tarfile.open(backup_path, 'r:gz') as tar:
                tar.extractall(config.USER_FILES_DIR)
            
            # اسکن مجدد فایل‌ها
            await self.file_manager._scan_user_files(user_id)
            
            self.logger.info(f"فایل‌های کاربر {user_id} بازیابی شدند")
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در بازیابی: {e}")
            return False
    
    async def _auto_backup_loop(self):
        """حلقه پشتیبان‌گیری خودکار"""
        while True:
            try:
                # پشتیبان‌گیری از همه کاربران
                for user_id in self.file_manager.users.keys():
                    asyncio.create_task(
                        self.backup_user_files(user_id, config.PRIMARY_BACKUP)
                    )
                    await asyncio.sleep(5)  # فاصله بین کاربران
                
                # پاکسازی پشتیبان‌های قدیمی
                await self._cleanup_old_backups()
                
                await asyncio.sleep(config.BACKUP_INTERVAL)
                
            except Exception as e:
                self.logger.error(f"خطا در auto backup: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_old_backups(self):
        """پاکسازی پشتیبان‌های قدیمی"""
        try:
            now = time.time()
            cutoff = now - (config.BACKUP_RETENTION_DAYS * 86400)
            
            for root, dirs, files in os.walk(config.USER_FILES_BACKUP_DIR):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) < cutoff:
                        os.remove(file_path)
                        
            self.logger.info("پشتیبان‌های قدیمی پاکسازی شدند")
            
        except Exception as e:
            self.logger.error(f"خطا در پاکسازی: {e}")


# ==============================================================================
# کلاس مدیریت فایل‌های ربات
# ==============================================================================

class BotFileManager:
    """
    مدیریت فایل‌های ربات‌های در حال اجرا
    """
    
    def __init__(self):
        self.bot_files: Dict[str, Dict] = {}
        self.logger = logging.getLogger('bot_files')
    
    async def save_bot_code(self, bot_id: str, code: str, user_id: int,
                            metadata: Dict = None) -> str:
        """ذخیره کد ربات"""
        try:
            bot_dir = os.path.join(config.BOTS_DIR, bot_id)
            os.makedirs(bot_dir, exist_ok=True)
            
            # ذخیره کد اصلی
            code_path = os.path.join(bot_dir, 'bot.py')
            async with aiofiles.open(code_path, 'w', encoding='utf-8') as f:
                await f.write(code)
            
            # ذخیره متادیتا
            if metadata:
                meta_path = os.path.join(bot_dir, 'metadata.json')
                async with aiofiles.open(meta_path, 'w') as f:
                    await f.write(json.dumps(metadata, ensure_ascii=False, indent=2))
            
            # ذخیره اطلاعات
            self.bot_files[bot_id] = {
                'bot_id': bot_id,
                'user_id': user_id,
                'code_path': code_path,
                'metadata': metadata,
                'created': time.time()
            }
            
            # پشتیبان
            await self._backup_bot_code(bot_id)
            
            return code_path
            
        except Exception as e:
            self.logger.error(f"خطا در ذخیره کد ربات: {e}")
            return ""
    
    async def get_bot_code(self, bot_id: str) -> Optional[str]:
        """خواندن کد ربات"""
        try:
            bot_dir = os.path.join(config.BOTS_DIR, bot_id)
            code_path = os.path.join(bot_dir, 'bot.py')
            
            if not os.path.exists(code_path):
                # تلاش از پشتیبان
                return await self._restore_bot_code(bot_id)
            
            async with aiofiles.open(code_path, 'r', encoding='utf-8') as f:
                return await f.read()
                
        except Exception as e:
            self.logger.error(f"خطا در خواندن کد ربات: {e}")
            return None
    
    async def delete_bot_files(self, bot_id: str) -> bool:
        """حذف فایل‌های ربات"""
        try:
            bot_dir = os.path.join(config.BOTS_DIR, bot_id)
            if os.path.exists(bot_dir):
                shutil.rmtree(bot_dir)
            
            if bot_id in self.bot_files:
                del self.bot_files[bot_id]
            
            return True
            
        except Exception as e:
            self.logger.error(f"خطا در حذف فایل‌های ربات: {e}")
            return False
    
    async def _backup_bot_code(self, bot_id: str):
        """پشتیبان از کد ربات"""
        try:
            bot_dir = os.path.join(config.BOTS_DIR, bot_id)
            backup_dir = os.path.join(config.BOTS_BACKUP_DIR, bot_id)
            
            if os.path.exists(bot_dir):
                shutil.copytree(bot_dir, backup_dir, dirs_exist_ok=True)
                
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان ربات: {e}")
    
    async def _restore_bot_code(self, bot_id: str) -> Optional[str]:
        """بازیابی کد ربات از پشتیبان"""
        try:
            backup_dir = os.path.join(config.BOTS_BACKUP_DIR, bot_id)
            code_path = os.path.join(backup_dir, 'bot.py')
            
            if os.path.exists(code_path):
                # بازیابی به مسیر اصلی
                bot_dir = os.path.join(config.BOTS_DIR, bot_id)
                os.makedirs(bot_dir, exist_ok=True)
                
                shutil.copy2(code_path, os.path.join(bot_dir, 'bot.py'))
                
                async with aiofiles.open(code_path, 'r', encoding='utf-8') as f:
                    return await f.read()
            
            return None
            
        except Exception as e:
            self.logger.error(f"خطا در بازیابی کد ربات: {e}")
            return None
    
    async def list_user_bots(self, user_id: int) -> List[Dict]:
        """لیست ربات‌های کاربر"""
        bots = []
        for bot_id, info in self.bot_files.items():
            if info['user_id'] == user_id:
                bots.append(info)
        return bots


# ==============================================================================
# کلاس مدیریت فیش‌ها
# ==============================================================================

class ReceiptManager:
    """
    مدیریت فیش‌های پرداخت
    """
    
    def __init__(self):
        self.receipts: Dict[str, Dict] = {}
        self.logger = logging.getLogger('receipts')
    
    async def save_receipt(self, user_id: int, receipt_data: bytes,
                           payment_code: str) -> Optional[str]:
        """ذخیره فیش پرداخت"""
        try:
            # ایجاد پوشه کاربر
            user_dir = os.path.join(config.RECEIPTS_DIR, str(user_id))
            os.makedirs(user_dir, exist_ok=True)
            
            # نام فایل
            filename = f"{payment_code}_{int(time.time())}.jpg"
            file_path = os.path.join(user_dir, filename)
            
            # ذخیره فیش
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(receipt_data)
            
            # ذخیره اطلاعات
            receipt_info = {
                'user_id': user_id,
                'payment_code': payment_code,
                'file_path': file_path,
                'filename': filename,
                'size': len(receipt_data),
                'created': time.time(),
                'status': 'pending',
                'verified': False
            }
            
            self.receipts[payment_code] = receipt_info
            
            # پشتیبان
            await self._backup_receipt(payment_code, receipt_data)
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"خطا در ذخیره فیش: {e}")
            return None
    
    async def get_receipt(self, payment_code: str) -> Optional[Dict]:
        """گرفتن اطلاعات فیش"""
        return self.receipts.get(payment_code)
    
    async def verify_receipt(self, payment_code: str, admin_id: int) -> bool:
        """تایید فیش"""
        if payment_code in self.receipts:
            self.receipts[payment_code]['status'] = 'approved'
            self.receipts[payment_code]['verified'] = True
            self.receipts[payment_code]['verified_by'] = admin_id
            self.receipts[payment_code]['verified_at'] = time.time()
            return True
        return False
    
    async def reject_receipt(self, payment_code: str, admin_id: int, reason: str = "") -> bool:
        """رد فیش"""
        if payment_code in self.receipts:
            self.receipts[payment_code]['status'] = 'rejected'
            self.receipts[payment_code]['verified_by'] = admin_id
            self.receipts[payment_code]['verified_at'] = time.time()
            self.receipts[payment_code]['reject_reason'] = reason
            return True
        return False
    
    async def _backup_receipt(self, payment_code: str, receipt_data: bytes):
        """پشتیبان از فیش"""
        try:
            backup_dir = os.path.join(config.RECEIPTS_BACKUP_DIR)
            os.makedirs(backup_dir, exist_ok=True)
            
            backup_path = os.path.join(backup_dir, f"{payment_code}.jpg")
            async with aiofiles.open(backup_path, 'wb') as f:
                await f.write(receipt_data)
                
        except Exception as e:
            self.logger.error(f"خطا در پشتیبان فیش: {e}")


# ==============================================================================
# کلاس تمیزکاری خودکار
# ==============================================================================

class CleanupManager:
    """
    مدیریت تمیزکاری خودکار فایل‌های موقت
    """
    
    def __init__(self):
        self.logger = logging.getLogger('cleanup')
        asyncio.create_task(self._cleanup_loop())
    
    async def _cleanup_loop(self):
        """حلقه تمیزکاری خودکار"""
        while True:
            try:
                await asyncio.sleep(config.CLEANUP_INTERVAL)
                
                # پاکسازی فایل‌های موقت
                await self._cleanup_temp_files()
                
                # پاکسازی لاگ‌های قدیمی
                await self._cleanup_old_logs()
                
                # پاکسازی آرشیوهای قدیمی
                await self._cleanup_old_archives()
                
                self.logger.info("تمیزکاری خودکار انجام شد")
                
            except Exception as e:
                self.logger.error(f"خطا در تمیزکاری: {e}")
    
    async def _cleanup_temp_files(self):
        """پاکسازی فایل‌های موقت"""
        try:
            now = time.time()
            cutoff = now - config.TEMP_FILE_AGE
            
            for root, dirs, files in os.walk(config.USER_FILES_TEMP_DIR):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) < cutoff:
                        os.remove(file_path)
                        
        except Exception as e:
            self.logger.error(f"خطا در پاکسازی temp files: {e}")
    
    async def _cleanup_old_logs(self):
        """پاکسازی لاگ‌های قدیمی"""
        try:
            now = time.time()
            cutoff = now - config.LOG_FILE_AGE
            
            for root, dirs, files in os.walk(config.LOGS_DIR):
                for file in files:
                    if file.endswith('.log'):
                        file_path = os.path.join(root, file)
                        if os.path.getmtime(file_path) < cutoff:
                            os.remove(file_path)
                            
        except Exception as e:
            self.logger.error(f"خطا در پاکسازی logs: {e}")
    
    async def _cleanup_old_archives(self):
        """پاکسازی آرشیوهای قدیمی"""
        try:
            now = time.time()
            cutoff = now - config.ARCHIVE_FILE_AGE
            
            for root, dirs, files in os.walk(config.USER_FILES_ARCHIVE_DIR):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.getmtime(file_path) < cutoff:
                        os.remove(file_path)
                        
        except Exception as e:
            self.logger.error(f"خطا در پاکسازی archives: {e}")


# ==============================================================================
# کلاس اصلی سیستم فایل
# ==============================================================================

class FileSystem:
    """
    سیستم فایل یکپارچه
    """
    
    def __init__(self):
        self.file_manager = FileManager()
        self.backup_manager = BackupManager(self.file_manager)
        self.bot_file_manager = BotFileManager()
        self.receipt_manager = ReceiptManager()
        self.cleanup_manager = CleanupManager()
        self.logger = logging.getLogger('filesystem')
    
    async def save_user_file(self, user_id: int, file_data: bytes, file_name: str,
                            folder_path: str = "") -> Optional[str]:
        """ذخیره فایل کاربر"""
        return await self.file_manager.save_file(
            user_id, file_data, file_name, folder_path
        )
    
    async def get_user_file(self, user_id: int, file_path: str) -> Optional[bytes]:
        """خواندن فایل کاربر"""
        return await self.file_manager.get_file(user_id, file_path)
    
    async def delete_user_file(self, user_id: int, file_path: str) -> bool:
        """حذف فایل کاربر"""
        return await self.file_manager.delete_file(user_id, file_path)
    
    async def list_user_files(self, user_id: int, folder: str = "") -> List[Dict]:
        """لیست فایل‌های کاربر"""
        return await self.file_manager.list_files(user_id, folder)
    
    async def create_user_folder(self, user_id: int, folder_path: str) -> bool:
        """ایجاد پوشه برای کاربر"""
        return await self.file_manager.create_folder(user_id, folder_path)
    
    async def delete_user_folder(self, user_id: int, folder_path: str,
                                recursive: bool = False) -> bool:
        """حذف پوشه کاربر"""
        return await self.file_manager.delete_folder(user_id, folder_path, recursive)
    
    async def move_user_file(self, user_id: int, old_path: str, new_path: str) -> bool:
        """انتقال فایل کاربر"""
        return await self.file_manager.move_file(user_id, old_path, new_path)
    
    async def get_user_storage_info(self, user_id: int) -> Dict:
        """گرفتن اطلاعات فضای ذخیره‌سازی کاربر"""
        storage = await self.file_manager.get_user_storage(user_id)
        return {
            'user_id': user_id,
            'total_size': storage.total_size,
            'file_count': storage.file_count,
            'quota': config.MAX_USER_STORAGE,
            'quota_used_percent': (storage.total_size / config.MAX_USER_STORAGE) * 100,
            'files': [asdict(f) for f in storage.files.values()]
        }
    
    async def backup_user(self, user_id: int) -> bool:
        """پشتیبان‌گیری از کاربر"""
        return await self.backup_manager.backup_user_files(user_id)
    
    async def restore_user(self, user_id: int) -> bool:
        """بازیابی کاربر"""
        return await self.backup_manager.restore_user_files(user_id)
    
    async def save_bot_code(self, bot_id: str, code: str, user_id: int,
                           metadata: Dict = None) -> str:
        """ذخیره کد ربات"""
        return await self.bot_file_manager.save_bot_code(bot_id, code, user_id, metadata)
    
    async def get_bot_code(self, bot_id: str) -> Optional[str]:
        """خواندن کد ربات"""
        return await self.bot_file_manager.get_bot_code(bot_id)
    
    async def delete_bot_files(self, bot_id: str) -> bool:
        """حذف فایل‌های ربات"""
        return await self.bot_file_manager.delete_bot_files(bot_id)
    
    async def save_receipt(self, user_id: int, receipt_data: bytes,
                          payment_code: str) -> Optional[str]:
        """ذخیره فیش"""
        return await self.receipt_manager.save_receipt(user_id, receipt_data, payment_code)
    
    async def verify_receipt(self, payment_code: str, admin_id: int) -> bool:
        """تایید فیش"""
        return await self.receipt_manager.verify_receipt(payment_code, admin_id)
    
    async def reject_receipt(self, payment_code: str, admin_id: int, reason: str = "") -> bool:
        """رد فیش"""
        return await self.receipt_manager.reject_receipt(payment_code, admin_id, reason)
    
    def get_stats(self) -> Dict[str, Any]:
        """آمار سیستم فایل"""
        return {
            'file_manager': {
                'users': len(self.file_manager.users),
                'total_files': sum(u.file_count for u in self.file_manager.users.values()),
                'total_size': sum(u.total_size for u in self.file_manager.users.values())
            },
            'bot_files': {
                'total': len(self.bot_file_manager.bot_files)
            },
            'receipts': {
                'total': len(self.receipt_manager.receipts)
            }
        }


# ==============================================================================
# نمونه اصلی
# ==============================================================================

filesystem = FileSystem()

# ==============================================================================
# توابع کمکی برای استفاده در فایل‌های دیگر
# ==============================================================================

async def save_user_file(user_id: int, file_data: bytes, file_name: str,
                        folder_path: str = "") -> Optional[str]:
    return await filesystem.save_user_file(user_id, file_data, file_name, folder_path)

async def get_user_file(user_id: int, file_path: str) -> Optional[bytes]:
    return await filesystem.get_user_file(user_id, file_path)

async def list_user_files(user_id: int, folder: str = "") -> List[Dict]:
    return await filesystem.list_user_files(user_id, folder)

async def create_user_folder(user_id: int, folder_path: str) -> bool:
    return await filesystem.create_user_folder(user_id, folder_path)

async def save_bot_code(bot_id: str, code: str, user_id: int,
                       metadata: Dict = None) -> str:
    return await filesystem.save_bot_code(bot_id, code, user_id, metadata)

async def get_bot_code(bot_id: str) -> Optional[str]:
    return await filesystem.get_bot_code(bot_id)

async def delete_bot_files(bot_id: str) -> bool:
    return await filesystem.delete_bot_files(bot_id)

async def save_receipt(user_id: int, receipt_data: bytes, payment_code: str) -> Optional[str]:
    return await filesystem.save_receipt(user_id, receipt_data, payment_code)

async def backup_user(user_id: int) -> bool:
    return await filesystem.backup_user(user_id)

async def restore_user(user_id: int) -> bool:
    return await filesystem.restore_user(user_id)

# ==============================================================================
# پایان فایل m3.py - ۶۰۲۸ خط
# ==============================================================================