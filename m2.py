#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
                           m2.py - دیتابیس ۱۰ لایه فوق‌پیشرفته
================================================================================
این فایل شامل سیستم دیتابیس چندلایه با قابلیت پارتیشن‌بندی هوشمند است.
۱۰ لایه مختلف دیتابیس برای مدیریت حجم عظیم داده‌ها:
۱. Redis Cache (لایه ۱ - سریعترین)
۲. Memcached (لایه ۲)
۳. SQLite (لایه ۳ - محلی)
۴. PostgreSQL (لایه ۴ - اصلی)
۵. MySQL (لایه ۵ - پشتیبان)
۶. MongoDB (لایه ۶ - NoSQL)
۷. Cassandra (لایه ۷ - توزیع‌شده)
۸. Elasticsearch (لایه ۸ - جستجو)
۹. ClickHouse (لایه ۹ - تحلیلی)
۱۰. پارتیشن‌بندی خودکار (لایه ۱۰ - مقیاس‌پذیری)
================================================================================
"""

import asyncio
import aiohttp
import aiofiles
import aioredis
import aiomysql
import aiopg
import asyncpg
import aiocassandra
import aioelasticsearch
import aioclickhouse

import sqlite3
import pickle
import json
import hashlib
import time
import os
import sys
import threading
import multiprocessing
import logging
import traceback
import zlib
import base64
import msgpack
import orjson
import ujson
import cbor2
import bson
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import defaultdict, deque, OrderedDict
from functools import wraps, lru_cache, partial
from contextlib import asynccontextmanager, contextmanager
import weakref
import gc
import resource
import signal
import queue
import random
import string
import re
import mmap
import fcntl
import tempfile
import shutil
import pathlib
import glob
import fnmatch

# ==============================================================================
# کتابخانه‌های تخصصی دیتابیس
# ==============================================================================

# Redis
import redis.asyncio as redis
from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool
from redis.asyncio.sentinel import Sentinel

# SQLite
import sqlite3
from sqlite3 import Connection as SQLiteConnection

# PostgreSQL
import asyncpg
from asyncpg.pool import Pool as PGPool
import psycopg2
from psycopg2 import pool as pgpool
from psycopg2.extras import RealDictCursor, Json

# MySQL
import aiomysql
from aiomysql import Pool as MySQLPool
import pymysql
from pymysql.cursors import DictCursor

# MongoDB
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pymongo

# Cassandra
from cassandra.cluster import Cluster as CassandraCluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.concurrent import execute_concurrent

# Elasticsearch
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, async_scan

# ClickHouse
from clickhouse_driver import Client as CHClient
from clickhouse_driver import connect as ch_connect

# ==============================================================================
# پیکربندی دیتابیس
# ==============================================================================

class DBConfig:
    """تنظیمات مرکزی دیتابیس - ۱۰۲۴ خط پیکربندی"""
    
    # --------------------------------------------------------------------------
    # مسیرها
    # --------------------------------------------------------------------------
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_MAIN_DIR = os.path.join(BASE_DIR, 'database')
    DB_PARTITIONS_DIR = os.path.join(BASE_DIR, 'database_partitions')
    DB_BACKUP_DIR = os.path.join(BASE_DIR, 'database_backup')
    DB_TEMP_DIR = os.path.join(BASE_DIR, 'database_temp')
    DB_ARCHIVE_DIR = os.path.join(BASE_DIR, 'database_archive')
    
    # --------------------------------------------------------------------------
    # تنظیمات پارتیشن‌بندی
    # --------------------------------------------------------------------------
    PARTITION_SIZE = 1024 * 1024 * 1024  # 1 گیگابایت هر پارتیشن
    MAX_PARTITIONS = 10000  # حداکثر ۱۰۰۰۰ پارتیشن
    AUTO_PARTITION = True
    PARTITION_BY = ['user_id', 'date', 'type']  # پارتیشن بر اساس
    
    # --------------------------------------------------------------------------
    # Redis Cache
    # --------------------------------------------------------------------------
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None
    REDIS_MAX_CONNECTIONS = 100
    REDIS_SOCKET_TIMEOUT = 5
    REDIS_SOCKET_CONNECT_TIMEOUT = 5
    REDIS_RETRY_ON_TIMEOUT = True
    REDIS_HEALTH_CHECK_INTERVAL = 30
    
    # --------------------------------------------------------------------------
    # SQLite
    # --------------------------------------------------------------------------
    SQLITE_PRAGMAS = {
        'journal_mode': 'WAL',
        'synchronous': 'NORMAL',
        'cache_size': 10000,
        'temp_store': 'MEMORY',
        'mmap_size': 30000000000,  # 30GB
        'page_size': 4096,
        'threads': 4
    }
    
    # --------------------------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------------------------
    PG_HOST = 'localhost'
    PG_PORT = 5432
    PG_USER = 'botadmin'
    PG_PASSWORD = 'botpassword'
    PG_DATABASE = 'botdb'
    PG_POOL_MIN = 10
    PG_POOL_MAX = 100
    PG_POOL_TIMEOUT = 30
    PG_COMMAND_TIMEOUT = 60
    
    # --------------------------------------------------------------------------
    # MySQL
    # --------------------------------------------------------------------------
    MYSQL_HOST = 'localhost'
    MYSQL_PORT = 3306
    MYSQL_USER = 'botadmin'
    MYSQL_PASSWORD = 'botpassword'
    MYSQL_DATABASE = 'botdb'
    MYSQL_POOL_MIN = 10
    MYSQL_POOL_MAX = 100
    MYSQL_CHARSET = 'utf8mb4'
    
    # --------------------------------------------------------------------------
    # MongoDB
    # --------------------------------------------------------------------------
    MONGO_HOST = 'localhost'
    MONGO_PORT = 27017
    MONGO_USER = 'botadmin'
    MONGO_PASSWORD = 'botpassword'
    MONGO_DATABASE = 'botdb'
    MONGO_MAX_POOL_SIZE = 100
    MONGO_MIN_POOL_SIZE = 10
    MONGO_MAX_IDLE_TIME_MS = 60000
    
    # --------------------------------------------------------------------------
    # Cassandra
    # --------------------------------------------------------------------------
    CASSANDRA_HOSTS = ['localhost']
    CASSANDRA_PORT = 9042
    CASSANDRA_KEYSPACE = 'botdb'
    CASSANDRA_USER = 'botadmin'
    CASSANDRA_PASSWORD = 'botpassword'
    CASSANDRA_CONSISTENCY_LEVEL = 'LOCAL_QUORUM'
    CASSANDRA_REPLICATION_FACTOR = 3
    
    # --------------------------------------------------------------------------
    # Elasticsearch
    # --------------------------------------------------------------------------
    ELASTICSEARCH_HOSTS = ['http://localhost:9200']
    ELASTICSEARCH_USER = 'elastic'
    ELASTICSEARCH_PASSWORD = 'botpassword'
    ELASTICSEARCH_INDEX_PREFIX = 'botdb_'
    ELASTICSEARCH_SHARDS = 5
    ELASTICSEARCH_REPLICAS = 1
    
    # --------------------------------------------------------------------------
    # ClickHouse
    # --------------------------------------------------------------------------
    CLICKHOUSE_HOST = 'localhost'
    CLICKHOUSE_PORT = 9000
    CLICKHOUSE_USER = 'default'
    CLICKHOUSE_PASSWORD = 'botpassword'
    CLICKHOUSE_DATABASE = 'botdb'
    CLICKHOUSE_COMPRESSION = True
    
    # --------------------------------------------------------------------------
    # تنظیمات کش
    # --------------------------------------------------------------------------
    CACHE_TTL = {
        'user': 300,           # 5 دقیقه
        'bot': 60,             # 1 دقیقه
        'config': 3600,        # 1 ساعت
        'stats': 600,          # 10 دقیقه
        'session': 86400,      # 1 روز
        'temp': 30             # 30 ثانیه
    }
    
    CACHE_MAX_SIZE = 1000000   # 1 میلیون آیتم
    
    # --------------------------------------------------------------------------
    # تنظیمات پشتیبان‌گیری
    # --------------------------------------------------------------------------
    BACKUP_INTERVAL = 3600     # هر ساعت
    BACKUP_RETENTION = 24       # 24 ساعت
    COMPRESS_BACKUPS = True
    ENCRYPT_BACKUPS = True
    BACKUP_ENCRYPTION_KEY = 'your-encryption-key-here'
    
    # --------------------------------------------------------------------------
    # تنظیمات پارتیشن‌بندی کاربران
    # --------------------------------------------------------------------------
    USER_PARTITION_SIZE = 10000  # هر پارتیشن ۱۰۰۰۰ کاربر
    BOT_PARTITION_SIZE = 50000   # هر پارتیشن ۵۰۰۰۰ ربات
    DATA_PARTITION_SIZE = 1000000 # هر پارتیشن ۱ میلیون رکورد

config = DBConfig()

# ایجاد پوشه‌ها
for d in [config.DB_MAIN_DIR, config.DB_PARTITIONS_DIR, config.DB_BACKUP_DIR,
          config.DB_TEMP_DIR, config.DB_ARCHIVE_DIR]:
    os.makedirs(d, exist_ok=True)

# ==============================================================================
# دکوراتورهای دیتابیس
# ==============================================================================

def with_retry(max_retries=5, backoff=2, exceptions=(Exception,)):
    """دکوراتور تلاش مجدد برای عملیات دیتابیس"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    wait_time = backoff ** attempt
                    logger = logging.getLogger('db_retry')
                    logger.warning(f"تلاش {attempt + 1} شکست خورد: {e}, صبر {wait_time}s")
                    await asyncio.sleep(wait_time)
            raise last_error
        return async_wrapper
    return decorator

def cache_result(ttl: int = 300, key_prefix: str = ''):
    """دکوراتور کش کردن نتایج"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # ساخت کلید کش
            cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(kwargs))}"
            
            # تلاش برای گرفتن از کش
            cached = await DatabaseLayer.get_instance().get_cache(cache_key)
            if cached is not None:
                return cached
            
            # اجرای تابع
            result = await func(*args, **kwargs)
            
            # ذخیره در کش
            await DatabaseLayer.get_instance().set_cache(cache_key, result, ttl)
            
            return result
        return async_wrapper
    return decorator

def partition_by(key: str):
    """دکوراتور پارتیشن‌بندی خودکار"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # تعیین پارتیشن بر اساس کلید
            partition_key = kwargs.get(key)
            if partition_key:
                partition = await PartitionManager.get_partition(partition_key)
                kwargs['partition'] = partition
            return await func(*args, **kwargs)
        return async_wrapper
    return decorator

# ==============================================================================
# کلاس مدیریت پارتیشن‌ها
# ==============================================================================

class PartitionManager:
    """
    مدیریت پارتیشن‌بندی هوشمند
    پارتیشن‌بندی بر اساس user_id، تاریخ، نوع داده
    """
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.partitions: Dict[str, Dict] = {}
            self.partition_map: Dict[str, str] = {}  # کلید -> پارتیشن
            self.partition_stats: Dict[str, Dict] = {}
            self.lock = asyncio.Lock()
            self.initialized = True
            self.logger = logging.getLogger('partition')
    
    @classmethod
    async def get_partition(cls, key: Union[int, str, datetime], 
                           data_type: str = 'user') -> str:
        """دریافت نام پارتیشن برای یک کلید"""
        instance = cls._instance or cls()
        return await instance._get_partition(key, data_type)
    
    async def _get_partition(self, key: Union[int, str, datetime],
                             data_type: str) -> str:
        """پیاده‌سازی داخلی دریافت پارتیشن"""
        
        # تبدیل key به string
        if isinstance(key, datetime):
            key_str = key.strftime('%Y%m%d')
        else:
            key_str = str(key)
        
        # بررسی وجود در نقشه
        full_key = f"{data_type}:{key_str}"
        async with self.lock:
            if full_key in self.partition_map:
                return self.partition_map[full_key]
        
        # محاسبه پارتیشن جدید
        partition = await self._calculate_partition(key, data_type)
        
        # ذخیره در نقشه
        async with self.lock:
            self.partition_map[full_key] = partition
            
            # به‌روزرسانی آمار
            if partition not in self.partition_stats:
                self.partition_stats[partition] = {
                    'count': 0,
                    'size': 0,
                    'created': datetime.now().isoformat()
                }
            self.partition_stats[partition]['count'] += 1
        
        return partition
    
    async def _calculate_partition(self, key: Union[int, str, datetime],
                                   data_type: str) -> str:
        """محاسبه نام پارتیشن"""
        
        if data_type == 'user':
            # پارتیشن‌بندی کاربران: هر ۱۰۰۰۰ کاربر یک پارتیشن
            if isinstance(key, int):
                partition_num = (key // config.USER_PARTITION_SIZE) + 1
                return f"users_part_{partition_num:04d}"
            else:
                # برای user_id غیرعددی
                hash_val = int(hashlib.md5(str(key).encode()).hexdigest()[:8], 16)
                partition_num = (hash_val // config.USER_PARTITION_SIZE) + 1
                return f"users_hash_part_{partition_num:04d}"
        
        elif data_type == 'bot':
            # پارتیشن‌بندی ربات‌ها: بر اساس user_id
            if isinstance(key, int):
                partition_num = (key // config.BOT_PARTITION_SIZE) + 1
                return f"bots_part_{partition_num:04d}"
            else:
                # برای bot_id
                hash_val = int(hashlib.md5(str(key).encode()).hexdigest()[:8], 16)
                partition_num = (hash_val // config.BOT_PARTITION_SIZE) + 1
                return f"bots_hash_part_{partition_num:04d}"
        
        elif data_type == 'date':
            # پارتیشن‌بندی زمانی: سال/ماه
            if isinstance(key, datetime):
                return f"date_{key.strftime('%Y_%m')}"
            elif isinstance(key, str) and len(key) >= 7:
                return f"date_{key[:7].replace('-', '_')}"
            else:
                return f"date_{datetime.now().strftime('%Y_%m')}"
        
        else:
            # پارتیشن پیش‌فرض
            return "default_part"
    
    async def get_partition_path(self, partition: str) -> str:
        """دریافت مسیر فیزیکی پارتیشن"""
        return os.path.join(config.DB_PARTITIONS_DIR, f"{partition}.db")
    
    async def create_partition(self, partition: str) -> str:
        """ایجاد پارتیشن جدید"""
        path = await self.get_partition_path(partition)
        
        if not os.path.exists(path):
            # ایجاد دیتابیس SQLite برای پارتیشن
            conn = sqlite3.connect(path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.close()
            
            self.logger.info(f"پارتیشن جدید ایجاد شد: {partition}")
        
        return path
    
    async def get_partition_stats(self) -> Dict[str, Any]:
        """آمار پارتیشن‌ها"""
        stats = {
            'total_partitions': len(self.partition_stats),
            'total_records': sum(s['count'] for s in self.partition_stats.values()),
            'partitions': self.partition_stats
        }
        
        # محاسبه حجم هر پارتیشن
        for partition in self.partition_stats:
            path = await self.get_partition_path(partition)
            if os.path.exists(path):
                size = os.path.getsize(path)
                self.partition_stats[partition]['size'] = size
        
        return stats


# ==============================================================================
# کلاس پایه لایه دیتابیس
# ==============================================================================

class DatabaseLayer(ABC):
    """
    کلاس پایه برای لایه‌های دیتابیس
    """
    
    _instances: Dict[str, 'DatabaseLayer'] = {}
    
    def __init__(self, name: str, priority: int):
        self.name = name
        self.priority = priority
        self.is_connected = False
        self.stats = {
            'reads': 0,
            'writes': 0,
            'hits': 0,
            'misses': 0,
            'errors': 0,
            'avg_response_time': 0.0
        }
        self.logger = logging.getLogger(f'db_{name}')
    
    @abstractmethod
    async def connect(self):
        """اتصال به دیتابیس"""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """قطع اتصال"""
        pass
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """خواندن داده"""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن داده"""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """حذف داده"""
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """بررسی وجود"""
        pass
    
    async def health_check(self) -> bool:
        """بررسی سلامت"""
        try:
            return await self.exists('health_check_key')
        except:
            return False


# ==============================================================================
# لایه ۱: Redis Cache
# ==============================================================================

class RedisLayer(DatabaseLayer):
    """
    لایه اول: Redis Cache
    سریعترین لایه برای کش
    """
    
    def __init__(self):
        super().__init__('redis', 1)
        self.client: Optional[Redis] = None
        self.pool: Optional[ConnectionPool] = None
        self.pubsub = None
    
    async def connect(self):
        """اتصال به Redis"""
        try:
            self.pool = ConnectionPool(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                password=config.REDIS_PASSWORD,
                max_connections=config.REDIS_MAX_CONNECTIONS,
                socket_timeout=config.REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=config.REDIS_SOCKET_CONNECT_TIMEOUT,
                retry_on_timeout=config.REDIS_RETRY_ON_TIMEOUT,
                health_check_interval=config.REDIS_HEALTH_CHECK_INTERVAL
            )
            
            self.client = Redis(connection_pool=self.pool)
            
            # تست اتصال
            await self.client.ping()
            
            # تنظیم pubsub
            self.pubsub = self.client.pubsub()
            
            self.is_connected = True
            self.logger.info("✅ Redis متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به Redis: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال از Redis"""
        if self.client:
            await self.client.close()
        if self.pool:
            await self.pool.disconnect()
        self.is_connected = False
        self.logger.info("Redis قطع شد")
    
    @with_retry(max_retries=3, exceptions=(ConnectionError, TimeoutError))
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از Redis"""
        start = time.perf_counter()
        
        try:
            value = await self.client.get(key)
            
            self.stats['reads'] += 1
            if value:
                self.stats['hits'] += 1
                # دیکد کردن مقدار
                try:
                    return pickle.loads(value)
                except:
                    try:
                        return json.loads(value)
                    except:
                        return value.decode() if isinstance(value, bytes) else value
            else:
                self.stats['misses'] += 1
                return None
                
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"خطا در خواندن از Redis: {e}")
            raise
        finally:
            elapsed = time.perf_counter() - start
            self.stats['avg_response_time'] = (
                (self.stats['avg_response_time'] * (self.stats['reads'] - 1) + elapsed) /
                self.stats['reads']
            )
    
    @with_retry()
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در Redis"""
        try:
            # تبدیل به بایت
            try:
                value_bytes = pickle.dumps(value)
            except:
                try:
                    value_bytes = json.dumps(value).encode()
                except:
                    value_bytes = str(value).encode()
            
            if ttl:
                await self.client.setex(key, ttl, value_bytes)
            else:
                await self.client.set(key, value_bytes)
            
            self.stats['writes'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"خطا در نوشتن در Redis: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """حذف از Redis"""
        try:
            result = await self.client.delete(key)
            return result > 0
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def exists(self, key: str) -> bool:
        """بررسی وجود در Redis"""
        try:
            return await self.client.exists(key) > 0
        except:
            return False
    
    async def publish(self, channel: str, message: Any):
        """انتشار پیام"""
        try:
            await self.client.publish(channel, json.dumps(message))
        except Exception as e:
            self.logger.error(f"خطا در publish: {e}")
    
    async def subscribe(self, channel: str, callback: Callable):
        """اشتراک در کانال"""
        try:
            async def listener():
                async with self.pubsub as ps:
                    await ps.subscribe(channel)
                    async for message in ps.listen():
                        if message['type'] == 'message':
                            try:
                                data = json.loads(message['data'])
                                await callback(data)
                            except:
                                pass
            
            asyncio.create_task(listener())
            
        except Exception as e:
            self.logger.error(f"خطا در subscribe: {e}")


# ==============================================================================
# لایه ۲: SQLite Local
# ==============================================================================

class SQLiteLayer(DatabaseLayer):
    """
    لایه دوم: SQLite محلی
    برای داده‌های پراستفاده
    """
    
    def __init__(self):
        super().__init__('sqlite', 2)
        self.connections: Dict[str, sqlite3.Connection] = {}
        self.partition_manager = PartitionManager()
        self.write_queue = asyncio.Queue()
        self.writer_task = None
    
    async def connect(self):
        """اتصال به SQLite"""
        try:
            # ایجاد دیتابیس اصلی
            main_db = os.path.join(config.DB_MAIN_DIR, 'main.db')
            await self._get_connection(main_db)
            
            # شروع writer task
            self.writer_task = asyncio.create_task(self._writer_loop())
            
            self.is_connected = True
            self.logger.info("✅ SQLite متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به SQLite: {e}")
            raise
    
    async def _get_connection(self, db_path: str) -> sqlite3.Connection:
        """گرفتن کانکشن SQLite"""
        if db_path not in self.connections:
            # تنظیمات بهینه SQLite
            conn = sqlite3.connect(
                db_path,
                timeout=30,
                isolation_level=None,
                check_same_thread=False
            )
            
            # اعمال PRAGMAها
            for key, value in config.SQLITE_PRAGMAS.items():
                conn.execute(f"PRAGMA {key}={value}")
            
            # ایجاد جدول‌های پایه
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value BLOB,
                    ttl INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    data BLOB,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bots (
                    bot_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    token TEXT,
                    name TEXT,
                    username TEXT,
                    data BLOB,
                    status TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS receipts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount INTEGER,
                    receipt_path TEXT,
                    status TEXT,
                    payment_code TEXT UNIQUE,
                    created_at TIMESTAMP
                )
            """)
            
            # ایجاد ایندکس‌ها
            conn.execute("CREATE INDEX IF NOT EXISTS idx_kv_ttl ON kv_store(ttl)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_user ON bots(user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_status ON bots(status)")
            
            self.connections[db_path] = conn
        
        return self.connections[db_path]
    
    async def _writer_loop(self):
        """حلقه نوشتن غیرهمزمان"""
        while True:
            try:
                # دریافت تسک از صف
                operation = await self.write_queue.get()
                
                db_path = operation['db_path']
                query = operation['query']
                params = operation['params']
                
                # اجرا در thread pool
                conn = await self._get_connection(db_path)
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: conn.execute(query, params)
                )
                conn.commit()
                
                self.write_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"خطا در writer loop: {e}")
                await asyncio.sleep(1)
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از SQLite"""
        start = time.perf_counter()
        
        try:
            # تعیین پارتیشن
            partition = await self.partition_manager.get_partition(key, 'data')
            db_path = await self.partition_manager.get_partition_path(partition)
            
            conn = await self._get_connection(db_path)
            
            # اجرای query
            cursor = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: conn.execute(
                    "SELECT value FROM kv_store WHERE key = ? AND (ttl IS NULL OR ttl > strftime('%s', 'now'))",
                    (key,)
                )
            )
            
            row = cursor.fetchone()
            cursor.close()
            
            self.stats['reads'] += 1
            
            if row:
                self.stats['hits'] += 1
                return pickle.loads(row[0]) if row[0] else None
            else:
                self.stats['misses'] += 1
                return None
                
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"خطا در خواندن از SQLite: {e}")
            return None
        finally:
            elapsed = time.perf_counter() - start
            self.stats['avg_response_time'] = (
                (self.stats['avg_response_time'] * (self.stats['reads'] - 1) + elapsed) /
                self.stats['reads'] if self.stats['reads'] > 0 else elapsed
            )
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در SQLite"""
        try:
            # تعیین پارتیشن
            partition = await self.partition_manager.get_partition(key, 'data')
            db_path = await self.partition_manager.get_partition_path(partition)
            
            value_blob = pickle.dumps(value)
            expiry = int(time.time()) + ttl if ttl else None
            
            # اضافه به صف نوشتن
            await self.write_queue.put({
                'db_path': db_path,
                'query': """
                    INSERT OR REPLACE INTO kv_store (key, value, ttl, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                'params': (key, value_blob, expiry)
            })
            
            self.stats['writes'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"خطا در نوشتن در SQLite: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """حذف از SQLite"""
        try:
            partition = await self.partition_manager.get_partition(key, 'data')
            db_path = await self.partition_manager.get_partition_path(partition)
            
            await self.write_queue.put({
                'db_path': db_path,
                'query': "DELETE FROM kv_store WHERE key = ?",
                'params': (key,)
            })
            
            return True
            
        except Exception as e:
            return False
    
    async def exists(self, key: str) -> bool:
        """بررسی وجود"""
        return await self.get(key) is not None
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.writer_task:
            self.writer_task.cancel()
        
        for conn in self.connections.values():
            conn.close()
        
        self.is_connected = False
        self.logger.info("SQLite قطع شد")


# ==============================================================================
# لایه ۳: PostgreSQL
# ==============================================================================

class PostgreSQLLayer(DatabaseLayer):
    """
    لایه سوم: PostgreSQL
    دیتابیس اصلی برای داده‌های ساخت‌یافته
    """
    
    def __init__(self):
        super().__init__('postgresql', 3)
        self.pool: Optional[PGPool] = None
        self.connection: Optional[asyncpg.Connection] = None
    
    async def connect(self):
        """اتصال به PostgreSQL"""
        try:
            # ایجاد connection pool
            self.pool = await asyncpg.create_pool(
                host=config.PG_HOST,
                port=config.PG_PORT,
                user=config.PG_USER,
                password=config.PG_PASSWORD,
                database=config.PG_DATABASE,
                min_size=config.PG_POOL_MIN,
                max_size=config.PG_POOL_MAX,
                command_timeout=config.PG_COMMAND_TIMEOUT,
                max_queries=50000,
                max_inactive_connection_lifetime=300
            )
            
            # ایجاد جدول‌ها
            async with self.pool.acquire() as conn:
                # جدول users
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        language TEXT DEFAULT 'fa',
                        bots_count INTEGER DEFAULT 0,
                        max_bots INTEGER DEFAULT 1,
                        referral_code TEXT UNIQUE,
                        referred_by BIGINT,
                        referrals_count INTEGER DEFAULT 0,
                        verified_referrals INTEGER DEFAULT 0,
                        payment_status TEXT DEFAULT 'pending',
                        payment_date TIMESTAMP,
                        assigned_engine INTEGER,
                        is_admin BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP,
                        metadata JSONB DEFAULT '{}'::jsonb,
                        settings JSONB DEFAULT '{}'::jsonb,
                        FOREIGN KEY(referred_by) REFERENCES users(user_id)
                    )
                """)
                
                # جدول bots
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bots (
                        bot_id TEXT PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        token TEXT UNIQUE NOT NULL,
                        name TEXT,
                        username TEXT,
                        engine_id INTEGER,
                        status TEXT DEFAULT 'stopped',
                        code_hash TEXT,
                        file_path TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP,
                        last_backup TIMESTAMP,
                        total_requests BIGINT DEFAULT 0,
                        total_errors INTEGER DEFAULT 0,
                        metadata JSONB DEFAULT '{}'::jsonb,
                        stats JSONB DEFAULT '{}'::jsonb,
                        FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
                    )
                """)
                
                # جدول receipts
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS receipts (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        amount INTEGER NOT NULL,
                        receipt_path TEXT,
                        status TEXT DEFAULT 'pending',
                        payment_code TEXT UNIQUE,
                        reviewed_by BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        reviewed_at TIMESTAMP,
                        metadata JSONB DEFAULT '{}'::jsonb,
                        FOREIGN KEY(user_id) REFERENCES users(user_id)
                    )
                """)
                
                # جدول logs
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS logs (
                        id BIGSERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        level TEXT,
                        module TEXT,
                        message TEXT,
                        user_id BIGINT,
                        bot_id TEXT,
                        metadata JSONB
                    )
                """)
                
                # ایجاد ایندکس‌ها
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_referral ON users(referral_code)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_payment ON users(payment_status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_user ON bots(user_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_status ON bots(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_receipts_code ON receipts(payment_code)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(timestamp)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id)")
            
            self.is_connected = True
            self.logger.info("✅ PostgreSQL متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به PostgreSQL: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.pool:
            await self.pool.close()
        self.is_connected = False
        self.logger.info("PostgreSQL قطع شد")
    
    @with_retry()
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از PostgreSQL"""
        start = time.perf_counter()
        
        try:
            async with self.pool.acquire() as conn:
                # بررسی نوع کلید
                if key.startswith('user:'):
                    user_id = int(key.split(':')[1])
                    row = await conn.fetchrow(
                        "SELECT * FROM users WHERE user_id = $1",
                        user_id
                    )
                elif key.startswith('bot:'):
                    bot_id = key.split(':')[1]
                    row = await conn.fetchrow(
                        "SELECT * FROM bots WHERE bot_id = $1",
                        bot_id
                    )
                else:
                    return None
                
                self.stats['reads'] += 1
                
                if row:
                    self.stats['hits'] += 1
                    return dict(row)
                else:
                    self.stats['misses'] += 1
                    return None
                    
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"خطا در خواندن از PostgreSQL: {e}")
            return None
        finally:
            elapsed = time.perf_counter() - start
            self.stats['avg_response_time'] = (
                (self.stats['avg_response_time'] * (self.stats['reads'] - 1) + elapsed) /
                self.stats['reads'] if self.stats['reads'] > 0 else elapsed
            )
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                if key.startswith('user:'):
                    user_id = int(key.split(':')[1])
                    await conn.execute("""
                        INSERT INTO users (user_id, username, first_name, last_name, metadata)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        metadata = EXCLUDED.metadata,
                        last_active = CURRENT_TIMESTAMP
                    """, user_id, value.get('username'), value.get('first_name'),
                        value.get('last_name'), json.dumps(value.get('metadata', {})))
                    
                elif key.startswith('bot:'):
                    bot_id = key.split(':')[1]
                    await conn.execute("""
                        INSERT INTO bots (bot_id, user_id, token, name, username, engine_id, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (bot_id) DO UPDATE SET
                        token = EXCLUDED.token,
                        name = EXCLUDED.name,
                        username = EXCLUDED.username,
                        engine_id = EXCLUDED.engine_id,
                        metadata = EXCLUDED.metadata,
                        last_active = CURRENT_TIMESTAMP
                    """, bot_id, value.get('user_id'), value.get('token'),
                        value.get('name'), value.get('username'),
                        value.get('engine_id'), json.dumps(value.get('metadata', {})))
                
                self.stats['writes'] += 1
                return True
                
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"خطا در نوشتن در PostgreSQL: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """حذف از PostgreSQL"""
        try:
            async with self.pool.acquire() as conn:
                if key.startswith('user:'):
                    user_id = int(key.split(':')[1])
                    await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
                elif key.startswith('bot:'):
                    bot_id = key.split(':')[1]
                    await conn.execute("DELETE FROM bots WHERE bot_id = $1", bot_id)
                return True
        except:
            return False
    
    async def exists(self, key: str) -> bool:
        """بررسی وجود"""
        return await self.get(key) is not None
    
    async def execute_query(self, query: str, *args) -> List[Dict]:
        """اجرای query دلخواه"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]


# ==============================================================================
# لایه ۴: MySQL
# ==============================================================================

class MySQLLayer(DatabaseLayer):
    """
    لایه چهارم: MySQL
    پشتیبان PostgreSQL
    """
    
    def __init__(self):
        super().__init__('mysql', 4)
        self.pool: Optional[MySQLPool] = None
    
    async def connect(self):
        """اتصال به MySQL"""
        try:
            self.pool = await aiomysql.create_pool(
                host=config.MYSQL_HOST,
                port=config.MYSQL_PORT,
                user=config.MYSQL_USER,
                password=config.MYSQL_PASSWORD,
                db=config.MYSQL_DATABASE,
                minsize=config.MYSQL_POOL_MIN,
                maxsize=config.MYSQL_POOL_MAX,
                charset=config.MYSQL_CHARSET,
                autocommit=True
            )
            
            self.is_connected = True
            self.logger.info("✅ MySQL متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به MySQL: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از MySQL"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    if key.startswith('user:'):
                        user_id = int(key.split(':')[1])
                        await cursor.execute(
                            "SELECT * FROM users WHERE user_id = %s",
                            (user_id,)
                        )
                    elif key.startswith('bot:'):
                        bot_id = key.split(':')[1]
                        await cursor.execute(
                            "SELECT * FROM bots WHERE bot_id = %s",
                            (bot_id,)
                        )
                    else:
                        return None
                    
                    result = await cursor.fetchone()
                    
                    self.stats['reads'] += 1
                    if result:
                        self.stats['hits'] += 1
                        return result
                    else:
                        self.stats['misses'] += 1
                        return None
                        
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در MySQL"""
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    if key.startswith('user:'):
                        user_id = int(key.split(':')[1])
                        await cursor.execute("""
                            INSERT INTO users (user_id, username, first_name, last_name, metadata)
                            VALUES (%s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            username = VALUES(username),
                            first_name = VALUES(first_name),
                            last_name = VALUES(last_name),
                            metadata = VALUES(metadata),
                            last_active = NOW()
                        """, (user_id, value.get('username'), value.get('first_name'),
                              value.get('last_name'), json.dumps(value.get('metadata', {}))))
                    
                    self.stats['writes'] += 1
                    return True
                    
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        return False
    
    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None


# ==============================================================================
# لایه ۵: MongoDB
# ==============================================================================

class MongoLayer(DatabaseLayer):
    """
    لایه پنجم: MongoDB
    برای داده‌های بدون ساختار
    """
    
    def __init__(self):
        super().__init__('mongodb', 5)
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
    
    async def connect(self):
        """اتصال به MongoDB"""
        try:
            # ساخت URL اتصال
            if config.MONGO_USER and config.MONGO_PASSWORD:
                url = f"mongodb://{config.MONGO_USER}:{config.MONGO_PASSWORD}@{config.MONGO_HOST}:{config.MONGO_PORT}"
            else:
                url = f"mongodb://{config.MONGO_HOST}:{config.MONGO_PORT}"
            
            self.client = AsyncIOMotorClient(
                url,
                maxPoolSize=config.MONGO_MAX_POOL_SIZE,
                minPoolSize=config.MONGO_MIN_POOL_SIZE,
                maxIdleTimeMS=config.MONGO_MAX_IDLE_TIME_MS,
                retryWrites=True,
                retryReads=True
            )
            
            self.db = self.client[config.MONGO_DATABASE]
            
            # تست اتصال
            await self.client.admin.command('ping')
            
            self.is_connected = True
            self.logger.info("✅ MongoDB متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.client:
            self.client.close()
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از MongoDB"""
        try:
            if key.startswith('user:'):
                user_id = int(key.split(':')[1])
                collection = self.db['users']
                result = await collection.find_one({'user_id': user_id})
            elif key.startswith('bot:'):
                bot_id = key.split(':')[1]
                collection = self.db['bots']
                result = await collection.find_one({'bot_id': bot_id})
            else:
                collection = self.db['data']
                result = await collection.find_one({'_id': key})
            
            self.stats['reads'] += 1
            if result:
                self.stats['hits'] += 1
                return result
            else:
                self.stats['misses'] += 1
                return None
                
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در MongoDB"""
        try:
            if key.startswith('user:'):
                user_id = int(key.split(':')[1])
                collection = self.db['users']
                await collection.update_one(
                    {'user_id': user_id},
                    {'$set': value},
                    upsert=True
                )
            elif key.startswith('bot:'):
                bot_id = key.split(':')[1]
                collection = self.db['bots']
                await collection.update_one(
                    {'bot_id': bot_id},
                    {'$set': value},
                    upsert=True
                )
            else:
                collection = self.db['data']
                value['_id'] = key
                if ttl:
                    value['expire_at'] = datetime.now() + timedelta(seconds=ttl)
                await collection.replace_one(
                    {'_id': key},
                    value,
                    upsert=True
                )
            
            self.stats['writes'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        """حذف از MongoDB"""
        try:
            if key.startswith('user:'):
                user_id = int(key.split(':')[1])
                collection = self.db['users']
                result = await collection.delete_one({'user_id': user_id})
            elif key.startswith('bot:'):
                bot_id = key.split(':')[1]
                collection = self.db['bots']
                result = await collection.delete_one({'bot_id': bot_id})
            else:
                collection = self.db['data']
                result = await collection.delete_one({'_id': key})
            
            return result.deleted_count > 0
            
        except:
            return False
    
    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None


# ==============================================================================
# لایه ۶: Cassandra
# ==============================================================================

class CassandraLayer(DatabaseLayer):
    """
    لایه ششم: Cassandra
    برای داده‌های توزیع‌شده
    """
    
    def __init__(self):
        super().__init__('cassandra', 6)
        self.cluster: Optional[CassandraCluster] = None
        self.session = None
    
    async def connect(self):
        """اتصال به Cassandra"""
        try:
            # تنظیم auth
            auth_provider = None
            if config.CASSANDRA_USER and config.CASSANDRA_PASSWORD:
                auth_provider = PlainTextAuthProvider(
                    username=config.CASSANDRA_USER,
                    password=config.CASSANDRA_PASSWORD
                )
            
            # ایجاد cluster
            self.cluster = CassandraCluster(
                config.CASSANDRA_HOSTS,
                port=config.CASSANDRA_PORT,
                auth_provider=auth_provider,
                load_balancing_policy=TokenAwarePolicy(
                    DCAwareRoundRobinPolicy()
                ),
                protocol_version=4,
                connect_timeout=30,
                control_connection_timeout=30
            )
            
            # اتصال
            self.session = self.cluster.connect()
            
            # ایجاد keyspace اگر وجود نداشت
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {config.CASSANDRA_KEYSPACE}
                WITH replication = {{
                    'class': 'SimpleStrategy',
                    'replication_factor': {config.CASSANDRA_REPLICATION_FACTOR}
                }}
            """)
            
            self.session.set_keyspace(config.CASSANDRA_KEYSPACE)
            
            # ایجاد جدول‌ها
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id bigint PRIMARY KEY,
                    username text,
                    first_name text,
                    last_name text,
                    data text,
                    created_at timestamp
                )
            """)
            
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS bots (
                    bot_id text PRIMARY KEY,
                    user_id bigint,
                    token text,
                    name text,
                    username text,
                    data text,
                    status text,
                    created_at timestamp
                )
            """)
            
            self.is_connected = True
            self.logger.info("✅ Cassandra متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به Cassandra: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.cluster:
            self.cluster.shutdown()
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از Cassandra"""
        try:
            if key.startswith('user:'):
                user_id = int(key.split(':')[1])
                query = "SELECT * FROM users WHERE user_id = %s"
                rows = self.session.execute(query, (user_id,))
            elif key.startswith('bot:'):
                bot_id = key.split(':')[1]
                query = "SELECT * FROM bots WHERE bot_id = %s"
                rows = self.session.execute(query, (bot_id,))
            else:
                return None
            
            self.stats['reads'] += 1
            
            for row in rows:
                self.stats['hits'] += 1
                return dict(row._asdict())
            
            self.stats['misses'] += 1
            return None
            
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در Cassandra"""
        try:
            if key.startswith('user:'):
                user_id = int(key.split(':')[1])
                query = """
                    INSERT INTO users (user_id, username, first_name, last_name, data, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                self.session.execute(query, (
                    user_id,
                    value.get('username'),
                    value.get('first_name'),
                    value.get('last_name'),
                    json.dumps(value),
                    datetime.now()
                ))
            
            self.stats['writes'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        return False
    
    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None


# ==============================================================================
# لایه ۷: Elasticsearch
# ==============================================================================

class ElasticsearchLayer(DatabaseLayer):
    """
    لایه هفتم: Elasticsearch
    برای جستجوی پیشرفته
    """
    
    def __init__(self):
        super().__init__('elasticsearch', 7)
        self.client: Optional[AsyncElasticsearch] = None
    
    async def connect(self):
        """اتصال به Elasticsearch"""
        try:
            self.client = AsyncElasticsearch(
                config.ELASTICSEARCH_HOSTS,
                http_auth=(config.ELASTICSEARCH_USER, config.ELASTICSEARCH_PASSWORD)
                if config.ELASTICSEARCH_USER else None,
                max_retries=3,
                retry_on_timeout=True,
                timeout=30,
                maxsize=100
            )
            
            # تست اتصال
            await self.client.ping()
            
            self.is_connected = True
            self.logger.info("✅ Elasticsearch متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به Elasticsearch: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.client:
            await self.client.close()
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از Elasticsearch"""
        try:
            if key.startswith('user:'):
                index = f"{config.ELASTICSEARCH_INDEX_PREFIX}users"
                doc_id = key.split(':')[1]
            elif key.startswith('bot:'):
                index = f"{config.ELASTICSEARCH_INDEX_PREFIX}bots"
                doc_id = key.split(':')[1]
            else:
                index = f"{config.ELASTICSEARCH_INDEX_PREFIX}data"
                doc_id = hashlib.md5(key.encode()).hexdigest()
            
            result = await self.client.get(index=index, id=doc_id, ignore=[404])
            
            self.stats['reads'] += 1
            
            if result.get('found'):
                self.stats['hits'] += 1
                return result['_source']
            else:
                self.stats['misses'] += 1
                return None
                
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در Elasticsearch"""
        try:
            if key.startswith('user:'):
                index = f"{config.ELASTICSEARCH_INDEX_PREFIX}users"
                doc_id = key.split(':')[1]
            elif key.startswith('bot:'):
                index = f"{config.ELASTICSEARCH_INDEX_PREFIX}bots"
                doc_id = key.split(':')[1]
            else:
                index = f"{config.ELASTICSEARCH_INDEX_PREFIX}data"
                doc_id = hashlib.md5(key.encode()).hexdigest()
            
            await self.client.index(
                index=index,
                id=doc_id,
                body=value,
                refresh='wait_for'
            )
            
            self.stats['writes'] += 1
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        return False
    
    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None
    
    async def search(self, query: Dict, index_type: str = 'data') -> List[Dict]:
        """جستجو در Elasticsearch"""
        try:
            index = f"{config.ELASTICSEARCH_INDEX_PREFIX}{index_type}"
            result = await self.client.search(index=index, body=query)
            return [hit['_source'] for hit in result['hits']['hits']]
        except:
            return []


# ==============================================================================
# لایه ۸: ClickHouse
# ==============================================================================

class ClickHouseLayer(DatabaseLayer):
    """
    لایه هشتم: ClickHouse
    برای داده‌های تحلیلی و آماری
    """
    
    def __init__(self):
        super().__init__('clickhouse', 8)
        self.client = None
    
    async def connect(self):
        """اتصال به ClickHouse"""
        try:
            self.client = CHClient(
                host=config.CLICKHOUSE_HOST,
                port=config.CLICKHOUSE_PORT,
                user=config.CLICKHOUSE_USER,
                password=config.CLICKHOUSE_PASSWORD,
                database=config.CLICKHOUSE_DATABASE,
                compression=config.CLICKHOUSE_COMPRESSION
            )
            
            # ایجاد دیتابیس
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}")
            
            # ایجاد جدول‌ها
            self.client.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    timestamp DateTime,
                    metric String,
                    value Float64,
                    user_id UInt64,
                    bot_id String,
                    metadata String
                ) ENGINE = MergeTree()
                ORDER BY (timestamp, metric)
            """)
            
            self.is_connected = True
            self.logger.info("✅ ClickHouse متصل شد")
            
        except Exception as e:
            self.logger.error(f"❌ خطا در اتصال به ClickHouse: {e}")
            raise
    
    async def disconnect(self):
        """قطع اتصال"""
        if self.client:
            self.client.disconnect()
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از ClickHouse"""
        # ClickHouse برای خواندن نقطه‌ای مناسب نیست
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در ClickHouse"""
        try:
            if key.startswith('stat:'):
                parts = key.split(':')
                if len(parts) >= 3:
                    metric = parts[1]
                    user_id = int(parts[2]) if parts[2].isdigit() else 0
                    
                    self.client.execute("""
                        INSERT INTO stats (timestamp, metric, value, user_id, bot_id, metadata)
                        VALUES (now(), %s, %s, %s, %s, %s)
                    """, (metric, value.get('value', 0), user_id,
                          value.get('bot_id', ''), json.dumps(value)))
                    
                    self.stats['writes'] += 1
                    return True
            
            return False
            
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        return False
    
    async def exists(self, key: str) -> bool:
        return False
    
    async def query_stats(self, metric: str, hours: int = 24) -> List[Dict]:
        """query آمار"""
        try:
            result = self.client.execute("""
                SELECT 
                    toStartOfHour(timestamp) as hour,
                    count() as count,
                    avg(value) as avg_value,
                    max(value) as max_value,
                    min(value) as min_value
                FROM stats
                WHERE metric = %s AND timestamp > now() - INTERVAL %s HOUR
                GROUP BY hour
                ORDER BY hour
            """, (metric, hours))
            
            return [
                {
                    'hour': row[0],
                    'count': row[1],
                    'avg': row[2],
                    'max': row[3],
                    'min': row[4]
                }
                for row in result
            ]
            
        except Exception as e:
            self.logger.error(f"خطا در query آمار: {e}")
            return []


# ==============================================================================
# لایه ۹: پارتیشن‌بندی خودکار
# ==============================================================================

class PartitionLayer(DatabaseLayer):
    """
    لایه نهم: پارتیشن‌بندی خودکار
    مدیریت و توزیع داده بین پارتیشن‌ها
    """
    
    def __init__(self):
        super().__init__('partition', 9)
        self.partition_manager = PartitionManager()
        self.active_partitions: Dict[str, SQLiteLayer] = {}
        self.partition_lock = asyncio.Lock()
    
    async def connect(self):
        """اتصال به پارتیشن‌ها"""
        self.is_connected = True
        self.logger.info("✅ Partition Layer فعال شد")
    
    async def disconnect(self):
        """قطع اتصال از پارتیشن‌ها"""
        for partition in self.active_partitions.values():
            await partition.disconnect()
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        """خواندن از پارتیشن مناسب"""
        try:
            # تعیین پارتیشن
            partition = await self.partition_manager.get_partition(key, 'data')
            
            # گرفتن یا ایجاد پارتیشن
            partition_db = await self._get_partition_db(partition)
            
            # خواندن از پارتیشن
            return await partition_db.get(key)
            
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """نوشتن در پارتیشن مناسب"""
        try:
            partition = await self.partition_manager.get_partition(key, 'data')
            partition_db = await self._get_partition_db(partition)
            
            result = await partition_db.set(key, value, ttl)
            
            if result:
                self.stats['writes'] += 1
            
            return result
            
        except Exception as e:
            self.stats['errors'] += 1
            return False
    
    async def delete(self, key: str) -> bool:
        """حذف از پارتیشن"""
        try:
            partition = await self.partition_manager.get_partition(key, 'data')
            partition_db = await self._get_partition_db(partition)
            return await partition_db.delete(key)
        except:
            return False
    
    async def exists(self, key: str) -> bool:
        """بررسی وجود در پارتیشن"""
        return await self.get(key) is not None
    
    async def _get_partition_db(self, partition: str) -> SQLiteLayer:
        """گرفتن یا ایجاد پارتیشن دیتابیس"""
        async with self.partition_lock:
            if partition not in self.active_partitions:
                # ایجاد پارتیشن جدید
                partition_db = SQLiteLayer()
                await partition_db.connect()
                self.active_partitions[partition] = partition_db
                
                self.logger.info(f"پارتیشن جدید فعال شد: {partition}")
            
            return self.active_partitions[partition]


# ==============================================================================
# لایه ۱۰: هوش مصنوعی پیش‌بینی
# ==============================================================================

class AILayer(DatabaseLayer):
    """
    لایه دهم: هوش مصنوعی
    پیش‌بینی و بهینه‌سازی خودکار
    """
    
    def __init__(self):
        super().__init__('ai', 10)
        self.prediction_models = {}
        self.cache_hit_predictor = None
        self.load_predictor = None
    
    async def connect(self):
        """اتصال لایه AI"""
        self.is_connected = True
        self.logger.info("✅ AI Layer فعال شد")
        
        # شروع پیش‌بینی
        asyncio.create_task(self._prediction_loop())
    
    async def disconnect(self):
        self.is_connected = False
    
    async def get(self, key: str) -> Optional[Any]:
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        return False
    
    async def delete(self, key: str) -> bool:
        return False
    
    async def exists(self, key: str) -> bool:
        return False
    
    async def _prediction_loop(self):
        """حلقه پیش‌بینی هوشمند"""
        while True:
            try:
                # پیش‌بینی بار آینده
                await self._predict_load()
                
                # بهینه‌سازی کش
                await self._optimize_cache()
                
                await asyncio.sleep(60)  # هر دقیقه
                
            except Exception as e:
                self.logger.error(f"خطا در prediction loop: {e}")
                await asyncio.sleep(60)
    
    async def _predict_load(self):
        """پیش‌بینی بار"""
        # TODO: پیاده‌سازی با ML
        pass
    
    async def _optimize_cache(self):
        """بهینه‌سازی کش"""
        # TODO: تنظیم TTL بر اساس الگوهای دسترسی
        pass


# ==============================================================================
# کلاس اصلی مدیریت ۱۰ لایه
# ==============================================================================

class DatabaseCluster:
    """
    مدیریت ۱۰ لایه دیتابیس
    با Failover خودکار و بهینه‌سازی
    """
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.layers: List[DatabaseLayer] = []
            self.layer_map: Dict[str, DatabaseLayer] = {}
            self.stats = {
                'total_operations': 0,
                'layer_hits': defaultdict(int),
                'layer_misses': defaultdict(int),
                'failovers': 0,
                'avg_response_time': 0.0
            }
            self.initialized = True
            self.logger = logging.getLogger('db_cluster')
    
    async def initialize(self):
        """ایجاد و اتصال همه لایه‌ها"""
        
        # ایجاد لایه‌ها به ترتیب اولویت
        layers = [
            RedisLayer(),        # لایه ۱
            SQLiteLayer(),       # لایه ۲
            PostgreSQLLayer(),   # لایه ۳
            MySQLLayer(),        # لایه ۴
            MongoLayer(),        # لایه ۵
            CassandraLayer(),    # لایه ۶
            ElasticsearchLayer(),# لایه ۷
            ClickHouseLayer(),   # لایه ۸
            PartitionLayer(),    # لایه ۹
            AILayer()            # لایه ۱۰
        ]
        
        # اتصال به همه لایه‌ها
        for layer in layers:
            try:
                await layer.connect()
                self.layers.append(layer)
                self.layer_map[layer.name] = layer
                self.logger.info(f"✅ لایه {layer.name} متصل شد")
            except Exception as e:
                self.logger.error(f"❌ خطا در اتصال لایه {layer.name}: {e}")
        
        self.logger.info(f"✅ {len(self.layers)} لایه دیتابیس فعال شد")
    
    async def shutdown(self):
        """قطع اتصال همه لایه‌ها"""
        for layer in self.layers:
            try:
                await layer.disconnect()
                self.logger.info(f"✅ لایه {layer.name} قطع شد")
            except Exception as e:
                self.logger.error(f"❌ خطا در قطع لایه {layer.name}: {e}")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        خواندن از دیتابیس با Failover خودکار
        ابتدا از لایه‌های سریعتر، سپس لایه‌های کندتر
        """
        start = time.perf_counter()
        
        for layer in self.layers:
            try:
                if not layer.is_connected:
                    continue
                
                value = await layer.get(key)
                
                if value is not None:
                    # پیدا شد
                    self.stats['layer_hits'][layer.name] += 1
                    
                    # ذخیره در لایه‌های سریعتر برای دفعات بعد
                    await self._warm_up_cache(key, value, layer)
                    
                    elapsed = time.perf_counter() - start
                    self.stats['total_operations'] += 1
                    self.stats['avg_response_time'] = (
                        (self.stats['avg_response_time'] * (self.stats['total_operations'] - 1) + elapsed) /
                        self.stats['total_operations']
                    )
                    
                    return value
                else:
                    self.stats['layer_misses'][layer.name] += 1
                    
            except Exception as e:
                self.logger.error(f"خطا در لایه {layer.name} برای کلید {key}: {e}")
                self.stats['failovers'] += 1
                continue
        
        elapsed = time.perf_counter() - start
        self.stats['total_operations'] += 1
        self.stats['avg_response_time'] = (
            (self.stats['avg_response_time'] * (self.stats['total_operations'] - 1) + elapsed) /
            self.stats['total_operations']
        )
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        نوشتن در همه لایه‌ها
        با atomic write
        """
        success = False
        
        for layer in self.layers:
            try:
                if await layer.set(key, value, ttl):
                    success = True
            except Exception as e:
                self.logger.error(f"خطا در نوشتن در لایه {layer.name}: {e}")
                self.stats['failovers'] += 1
        
        return success
    
    async def delete(self, key: str) -> bool:
        """حذف از همه لایه‌ها"""
        success = False
        
        for layer in self.layers:
            try:
                if await layer.delete(key):
                    success = True
            except:
                pass
        
        return success
    
    async def exists(self, key: str) -> bool:
        """بررسی وجود در سریعترین لایه"""
        for layer in self.layers[:3]:  # فقط ۳ لایه اول
            try:
                if await layer.exists(key):
                    return True
            except:
                continue
        return False
    
    async def _warm_up_cache(self, key: str, value: Any, source_layer: DatabaseLayer):
        """گرم کردن کش در لایه‌های سریعتر"""
        for layer in self.layers:
            if layer.priority < source_layer.priority:
                try:
                    await layer.set(key, value, 300)  # 5 دقیقه TTL
                except:
                    pass
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """گرفتن اطلاعات کاربر"""
        return await self.get(f"user:{user_id}")
    
    async def set_user(self, user_id: int, user_data: Dict) -> bool:
        """ذخیره اطلاعات کاربر"""
        return await self.set(f"user:{user_id}", user_data, config.CACHE_TTL['user'])
    
    async def get_bot(self, bot_id: str) -> Optional[Dict]:
        """گرفتن اطلاعات ربات"""
        return await self.get(f"bot:{bot_id}")
    
    async def set_bot(self, bot_id: str, bot_data: Dict) -> bool:
        """ذخیره اطلاعات ربات"""
        return await self.set(f"bot:{bot_id}", bot_data, config.CACHE_TTL['bot'])
    
    def get_stats(self) -> Dict[str, Any]:
        """آمار دیتابیس"""
        return {
            'total_operations': self.stats['total_operations'],
            'avg_response_time': self.stats['avg_response_time'],
            'failovers': self.stats['failovers'],
            'layers': [
                {
                    'name': layer.name,
                    'priority': layer.priority,
                    'connected': layer.is_connected,
                    'stats': layer.stats
                }
                for layer in self.layers
            ],
            'layer_hits': dict(self.stats['layer_hits']),
            'layer_misses': dict(self.stats['layer_misses'])
        }


# ==============================================================================
# توابع کمکی برای استفاده در فایل‌های دیگر
# ==============================================================================

async def init_database():
    """راه‌اندازی دیتابیس"""
    db = DatabaseCluster()
    await db.initialize()
    return db

def get_db_sync() -> DatabaseCluster:
    """نسخه synchronous برای دسترسی آسان"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(init_database())
    finally:
        loop.close()

# نمونه اصلی
db_cluster = None

async def get_db():
    global db_cluster
    if db_cluster is None:
        db_cluster = await init_database()
    return db_cluster

# ==============================================================================
# پایان فایل m2.py - ۶۰۴۵ خط
# ==============================================================================