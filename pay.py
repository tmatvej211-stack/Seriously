# bot.py — aiogram 3.x | Astral Team
import asyncio
import random
import string
import os
import re
import aiosqlite
import aiohttp
from datetime import datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties

# ── Конфигурация ─────────────────────────────────────────────────────────────
BOT_TOKEN   = '8664269263:AAGBj1U7zfKgyslXNmgJOTVuMbpnh-o_AJE'
OWNERS      = [488616444, 8727416659]
DB_NAME     = 'bot_database.db'
PAYOUT_CHAT_ID  = -1003644645918
PAYOUT_TOPIC_ID = 28
PAYOUT_PERCENTAGE = 0.7
CHANNEL_USERNAME  = 'AstralPerexodnik'
VIDEO_URL = 'https://github.com/luvysex/video/raw/refs/heads/main/pay.mp4'

WORK_TYPES = {
    'drainer':  {'name': 'Дрейнер', 'default_percent': 0.6},
    'otc':      {'name': 'ОТС',     'default_percent': 0.7},
    'nicegram': {'name': 'Nicegram','default_percent': 0.7},
    'guarantor':{'name': 'Гарант',  'default_percent': 0.7},
}

# ── Премиум-эмодзи ────────────────────────────────────────────────────────────
def e(emoji_id: int, fallback: str) -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

E = {
    'ok':       e(5895514131896733546, '✅'),
    'no':       e(5893163582194978381, '❌'),
    'no2':      e(5893081007153746175, '❌'),
    'clock':    e(5893102202817352158, '🕞'),
    'alarm':    e(5902050947567194830, '⏰'),
    'bolt':     e(5893450623449305489, '⚡️'),
    'gear':     e(5893161718179173515, '⚙️'),
    'gear2':    e(5902432207519093015, '⚙️'),
    'warn':     e(5904692292324692386, '⚠️'),
    'stop':     e(5960671702059848143, '⛔️'),
    'user':     e(5902335789798265487, '👤'),
    'link':     e(5902449142575141204, '🔗'),
    'rocket':   e(5195033767969839232, '🚀'),
    'star':     e(5924870095925942277, '⭐️'),
    'rocket2':  e(5258332798409783582, '🚀'),
    'n1':       e(5794164805065514131, '1⃣'),
    'n2':       e(5794085322400733645, '2⃣'),
    'n3':       e(5794280000383358988, '3⃣'),
    'n4':       e(5794241397217304511, '4⃣'),
    'n5':       e(5793985348446984682, '5⃣'),
    'n6':       e(5794324702402976226, '6⃣'),
    'n7':       e(5793942849745591465, '7⃣'),
    'n8':       e(5793926687783655907, '8⃣'),
    'n9':       e(5793979472931723221, '9⃣'),
    'gift':     e(6037175527846975726, '🎁'),
    'user2':    e(5904630315946611415, '👤'),
    'user3':    e(6032693626394382504, '👤'),
    # старые
    'warn_old': e(5447644880824181073, '⚠️'),
    'stop2':    e(5240241223632954241, '🚫'),
    'check':    e(5206607081334906820, '✅'),
    'xmark':    e(5210952531676504517, '❌'),
    'money':    e(5224257782013769471, '💰'),
    'chart':    e(5231200819986047254, '📊'),
    'cash':     e(5201691993775818138, '💸'),
    'hourglass':e(5451646226975955576, '⌛️'),
    'sparkle':  e(5325547803936572038, '✨'),
    'party':    e(5461151367559141950, '🎉'),
    'sad':      e(5386856460632201117, '😢'),
    'trophy':   e(5409008750893734809, '🏆'),
    'gold':     e(5440539497383087970, '🥇'),
    'silver':   e(5447203607294265305, '🥈'),
    'bronze':   e(5453902265922376865, '🥉'),
    'info':     e(5334544901428229844, 'ℹ️'),
    'crown':    e(5217822164362739968, '👑'),
    'bolt2':    e(5456140674028019486, '⚡️'),
    'phone':    e(5363858422590619939, '📱'),
    'cal':      e(5274055917766202507, '🗓'),
    'down':     e(5301038027601098171, '👇'),
    'mail':     e(5253742260054409879, '✉️'),
    'write':    e(5197269100878907942, '✍️'),
    'pencil':   e(5956143844457189176, '✏️'),
    'arrow':    e(6037622221625626773, '➡️'),
    'search':   e(5188217332748527444, '🔍'),
    'refresh':  e(6030657343744644592, '🔁'),
    'hat':      e(5895605369887004463, '🎩'),
    'diamond':  e(5776023601941582822, '💎'),
    'users':    e(6032609071373226027, '👥'),
    'chartup':  e(5244837092042750681, '📈'),
    'chartdn':  e(5246762912428603768, '📉'),
    'question': e(5436113877181941026, '❓'),
    'hi':       e(6041921818896372382, '👋'),
    'ban':      e(5960671702059848143, '⛔️'),
}

# ── FSM ───────────────────────────────────────────────────────────────────────
class Survey(StatesGroup):
    q1 = State()
    q2 = State()
    q3 = State()

class Payout(StatesGroup):
    deal_code  = State()
    media      = State()
    gift_link  = State()
    ton_addr   = State()

class AdminAction(StatesGroup):
    waiting_profit_add    = State()
    waiting_profit_remove = State()
    waiting_percent       = State()
    waiting_global_pct    = State()
    waiting_amount_approve= State()
    waiting_reject_reason = State()
    search_user           = State()
    add_admin             = State()
    remove_admin          = State()
    search_payout_code    = State()

# ── Инициализация ─────────────────────────────────────────────────────────────
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp  = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# Временное хранилище данных выплат и контекста одобрения
payout_temp: dict = {}       # user_id -> dict с данными выплаты
approve_ctx: dict = {}       # admin_id -> dict с данными для одобрения
reject_ctx: dict  = {}       # admin_id -> dict с данными для отклонения
payout_media_store: dict = {}  # payout_id -> list of (type, file_id)

# ── БД: пул соединений через контекстный менеджер ────────────────────────────
_db_lock = asyncio.Lock()

async def get_db() -> aiosqlite.Connection:
    """Создаёт новое соединение с БД для использования в async with."""
    conn = await aiosqlite.connect(DB_NAME)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA busy_timeout=10000")
    return conn

class _db_ctx:
    """Контекстный менеджер: открывает соединение, фиксирует lock, закрывает."""
    def __init__(self, autocommit=False):
        self._autocommit = autocommit
        self._conn = None

    async def __aenter__(self) -> aiosqlite.Connection:
        self._conn = await get_db()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        if self._conn:
            try:
                if exc_type is None and self._autocommit:
                    await self._conn.commit()
            finally:
                await self._conn.close()
                self._conn = None

def db_connect():
    return _db_ctx()

# ── БД ───────────────────────────────────────────────────────────────────────
async def init_db():
    async with db_connect() as conn:
        await conn.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, nickname TEXT,
            role TEXT DEFAULT 'worker', total_profits REAL DEFAULT 0,
            join_date TEXT, approved INTEGER DEFAULT 0)''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            payout_code TEXT, work_type TEXT, deal_code TEXT,
            gift_link TEXT, ton_address TEXT,
            status TEXT DEFAULT 'pending', created_at TEXT,
            profit_amount REAL DEFAULT 0, user_percentage REAL)''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS surveys (
            user_id INTEGER PRIMARY KEY, answer1 TEXT, answer2 TEXT,
            answer3 TEXT, status TEXT DEFAULT 'pending')''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS work_types (
            work_type TEXT PRIMARY KEY, name TEXT,
            default_percent REAL, enabled INTEGER DEFAULT 1)''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS user_work_percentages (
            user_id INTEGER, work_type TEXT, percentage REAL,
            PRIMARY KEY (user_id, work_type))''')
        try:
            await conn.execute('ALTER TABLE work_types ADD COLUMN enabled INTEGER DEFAULT 1')
        except Exception:
            pass
        await conn.execute('''CREATE TABLE IF NOT EXISTS payout_media (
            payout_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            file_id TEXT NOT NULL
        )''')
        try:
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_pm_payout ON payout_media(payout_id)')
        except Exception:
            pass
        for wt, data in WORK_TYPES.items():
            await conn.execute(
                'INSERT OR IGNORE INTO work_types (work_type, name, default_percent, enabled) VALUES (?,?,?,1)',
                (wt, data['name'], data['default_percent']))
        await conn.commit()

async def check_and_fix_database():
    print(f"{E['search']} Проверяю структуру базы данных...")
    async with db_connect() as conn:
        async with conn.execute("PRAGMA table_info(payouts)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}
        required_columns = {
            'payout_code': 'TEXT', 'work_type': 'TEXT', 'deal_code': 'TEXT',
            'gift_link': 'TEXT', 'ton_address': 'TEXT',
            'status': 'TEXT DEFAULT "pending"', 'created_at': 'TEXT',
            'profit_amount': 'REAL DEFAULT 0', 'user_percentage': 'REAL'
        }
        missing = {c: t for c, t in required_columns.items() if c not in columns}
        if not missing:
            try:
                await conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_code ON payouts(payout_code)')
            except Exception:
                await fix_payouts_table_with_data(conn, columns, required_columns)
            await conn.commit()
            return
        if 'payout_code' in missing:
            await fix_payouts_table_with_data(conn, columns, required_columns)
        else:
            for col, col_type in missing.items():
                try:
                    await conn.execute(f"ALTER TABLE payouts ADD COLUMN {col} {col_type}")
                except Exception as e:
                    print(f"❌ Ошибка при добавлении {col}: {e}")
        try:
            await conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_payout_code ON payouts(payout_code)')
        except Exception:
            pass
        await conn.commit()

async def fix_payouts_table_with_data(db, existing_columns, required_columns):
    await db.execute('DROP TABLE IF EXISTS payouts_temp')
    await db.execute('''
        CREATE TABLE payouts_temp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, payout_code TEXT, work_type TEXT, deal_code TEXT,
            gift_link TEXT, ton_address TEXT,
            status TEXT DEFAULT 'pending', created_at TEXT,
            profit_amount REAL DEFAULT 0, user_percentage REAL
        )
    ''')
    existing_cols_in_old = [c for c in [
        'id','user_id','work_type','deal_code',
        'gift_link','ton_address','status','created_at','profit_amount','user_percentage'
    ] if c in existing_columns] or ['id', 'user_id']
    async with db.execute(f"SELECT {', '.join(existing_cols_in_old)} FROM payouts") as cursor:
        old_data = await cursor.fetchall()
    for row in old_data:
        data_dict = dict(zip(existing_cols_in_old, row))
        while True:
            code = '#' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            async with db.execute("SELECT COUNT(*) FROM payouts_temp WHERE payout_code = ?", (code,)) as c:
                if (await c.fetchone())[0] == 0:
                    break
        all_cols = ['user_id','payout_code','work_type','deal_code',
                    'gift_link','ton_address','status','created_at','profit_amount','user_percentage']
        values = []
        for col in all_cols:
            if col == 'payout_code': values.append(code)
            elif col in data_dict: values.append(data_dict[col])
            else: values.append({'work_type':'drainer','status':'pending','profit_amount':0.0,'user_percentage':PAYOUT_PERCENTAGE}.get(col))
        await db.execute(f"INSERT INTO payouts_temp ({', '.join(all_cols)}) VALUES ({', '.join(['?']*len(all_cols))})", values)
    await db.execute('DROP TABLE payouts')
    await db.execute('ALTER TABLE payouts_temp RENAME TO payouts')

# ── DB-утилиты ────────────────────────────────────────────────────────────────
async def db_get_user(user_id: int):
    async with db_connect() as conn:
        async with conn.execute('SELECT * FROM users WHERE user_id=?', (user_id,)) as c:
            return await c.fetchone()

async def db_upsert_user(user_id, username, nickname):
    async with db_connect() as conn:
        await conn.execute(
            'INSERT OR IGNORE INTO users (user_id,username,nickname,role,join_date,approved) VALUES (?,?,?,"worker",?,0)',
            (user_id, username, nickname, datetime.now().strftime('%Y-%m-%d')))
        await conn.execute(
            'UPDATE users SET username=COALESCE(?,username), nickname=COALESCE(?,nickname) WHERE user_id=?',
            (username, nickname, user_id))
        await conn.commit()

async def get_approved(user_id: int):
    async with db_connect() as conn:
        async with conn.execute('SELECT approved FROM users WHERE user_id=?', (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else None

async def is_admin(user_id: int) -> bool:
    if user_id in OWNERS: return True
    async with db_connect() as conn:
        async with conn.execute('SELECT role FROM users WHERE user_id=? AND approved=1', (user_id,)) as c:
            row = await c.fetchone()
            return bool(row and row[0] == 'admin')

async def get_admins():
    admins = list(OWNERS)
    async with db_connect() as conn:
        async with conn.execute('SELECT user_id FROM users WHERE role="admin" AND approved=1') as c:
            rows = await c.fetchall()
    for row in rows:
        admins.append(row[0])
    return list(set(admins))

async def has_pending_survey(user_id: int) -> bool:
    async with db_connect() as conn:
        async with conn.execute('SELECT status FROM surveys WHERE user_id=?', (user_id,)) as c:
            row = await c.fetchone()
            return bool(row and row[0] == 'pending')

async def get_user_percentage(user_id: int, work_type: str) -> float:
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT percentage FROM user_work_percentages WHERE user_id=? AND work_type=?',
            (user_id, work_type)) as c:
            row = await c.fetchone()
            if row: return row[0]
        async with conn.execute('SELECT default_percent FROM work_types WHERE work_type=?', (work_type,)) as c:
            row = await c.fetchone()
            return row[0] if row and row[0] is not None else WORK_TYPES.get(work_type, {}).get('default_percent', PAYOUT_PERCENTAGE)

async def get_work_type_percentage(work_type: str) -> float:
    async with db_connect() as conn:
        async with conn.execute('SELECT default_percent FROM work_types WHERE work_type=?', (work_type,)) as c:
            row = await c.fetchone()
            return row[0] if row and row[0] is not None else WORK_TYPES.get(work_type, {}).get('default_percent', PAYOUT_PERCENTAGE)

async def set_user_percentage(user_id: int, work_type: str, pct: float):
    async with db_connect() as conn:
        await conn.execute(
            'INSERT OR REPLACE INTO user_work_percentages (user_id,work_type,percentage) VALUES (?,?,?)',
            (user_id, work_type, pct))
        await conn.commit()

async def set_work_type_percentage(work_type: str, pct: float):
    async with db_connect() as conn:
        await conn.execute('UPDATE work_types SET default_percent=? WHERE work_type=?', (pct, work_type))
        await conn.commit()

async def get_all_work_types():
    async with db_connect() as conn:
        async with conn.execute('SELECT work_type, name, default_percent, enabled FROM work_types') as c:
            rows = await c.fetchall()
    result = []
    for wt, name, pct, enabled in rows:
        if pct is None: pct = WORK_TYPES.get(wt, {}).get('default_percent', PAYOUT_PERCENTAGE)
        result.append({'type': wt, 'name': name, 'percent': pct, 'enabled': bool(enabled)})
    return result

async def get_enabled_work_types():
    return [w for w in await get_all_work_types() if w['enabled']]

async def toggle_work_type(work_type: str) -> bool:
    async with db_connect() as conn:
        async with conn.execute('SELECT enabled FROM work_types WHERE work_type=?', (work_type,)) as c:
            row = await c.fetchone()
        new_val = 0 if (row and row[0]) else 1
        await conn.execute('UPDATE work_types SET enabled=? WHERE work_type=?', (new_val, work_type))
        await conn.commit()
    return bool(new_val)

async def find_user(query: str):
    q = query.strip().lstrip('@')
    async with db_connect() as conn:
        if q.lstrip('-').isdigit():
            async with conn.execute(
                'SELECT user_id,nickname,username,role,total_profits,join_date,approved FROM users WHERE user_id=?',
                (int(q),)) as c:
                return await c.fetchone()
        else:
            async with conn.execute(
                'SELECT user_id,nickname,username,role,total_profits,join_date,approved FROM users WHERE username=?',
                (q,)) as c:
                return await c.fetchone()

def generate_payout_code():
    return '#' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))

def is_valid_nft_link(link: str) -> bool:
    if not link: return False
    link = link.lower().strip()
    for pat in [r'^https://t\.me/nft/', r'^t\.me/nft/', r'^https://telegram\.me/nft/']:
        if re.match(pat, link): return True
    return False

def is_valid_ton(addr: str) -> bool:
    if not addr: return False
    addr = addr.strip()
    return (addr.upper().startswith('UQ') or addr.upper().startswith('EQ')) and \
           bool(re.match(r'^[A-Za-z0-9_-]+$', addr))

async def check_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(f'@{CHANNEL_USERNAME}', user_id)
        return member.status not in ('left', 'kicked')
    except Exception:
        return False

async def download_video() -> str | None:
    path = 'pay_video.mp4'
    if os.path.exists(path): return path
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(VIDEO_URL) as r:
                if r.status == 200:
                    with open(path, 'wb') as f:
                        async for chunk in r.content.iter_chunked(8192):
                            f.write(chunk)
                    return path
    except Exception:
        return None

# ── Клавиатуры ───────────────────────────────────────────────────────────────
def kb(*rows) -> InlineKeyboardMarkup:
    """Быстрое создание клавиатуры из списков кнопок."""
    return InlineKeyboardMarkup(inline_keyboard=list(rows))

def btn(text: str, callback: str, emoji_id: int = None, style: str = None) -> InlineKeyboardButton:
    extra: dict = {}
    if emoji_id:
        extra['icon_custom_emoji_id'] = str(emoji_id)
    if style:
        extra['style'] = style
    return InlineKeyboardButton.model_construct(
        text=text,
        callback_data=callback,
        **extra
    )

def main_menu(is_admin_user=False) -> InlineKeyboardMarkup:
    rows = [
        [btn('Профиль', 'profile', 5879770735999717115), btn('Топ', 'top', 5409008750893734809)],
        [btn('Выплата', 'payout', 5201691993775818138), btn('Инфо', 'info', 5334544901428229844)],
        [btn('Мои заявки', 'my_payouts_0', 5363858422590619939)],
        [InlineKeyboardButton.model_construct(text='Переходник', url=f'https://t.me/{CHANNEL_USERNAME}', icon_custom_emoji_id='5424818078833715060')],
    ]
    if is_admin_user:
        rows.insert(2, [btn('Настройки', 'admin_settings', 5893161718179173515)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_btn() -> InlineKeyboardMarkup:
    return kb([btn('Назад', 'back_main', 5960671702059848143)])

def sub_check_kb() -> InlineKeyboardMarkup:
    return kb(
        [InlineKeyboardButton.model_construct(text='Перейти в переходник', url=f'https://t.me/{CHANNEL_USERNAME}', icon_custom_emoji_id='5424818078833715060')],
        [btn('Проверить подписку', 'check_sub', 5895514131896733546, 'success')]
    )

def survey_kb(step: int, user_id: int) -> InlineKeyboardMarkup:
    return kb([
        InlineKeyboardButton.model_construct(text='Да', callback_data=f'survey_yes_{step}_{user_id}', icon_custom_emoji_id='5895514131896733546', style='success'),
        InlineKeyboardButton.model_construct(text='Нет', callback_data=f'survey_no_{step}_{user_id}', icon_custom_emoji_id='5893163582194978381', style='danger'),
    ])

def work_type_kb(types: list) -> InlineKeyboardMarkup:
    num_ids = [5794164805065514131, 5794085322400733645, 5794280000383358988,
               5794241397217304511, 5793985348446984682, 5794324702402976226,
               5793942849745591465, 5793926687783655907, 5793979472931723221]
    rows = []
    for i, wt in enumerate(types):
        eid = num_ids[i] if i < len(num_ids) else None
        rows.append([btn(wt['name'], f'work_{wt["type"]}', eid)])
    rows.append([btn('Назад', 'back_main', 5960671702059848143)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def payout_approve_kb(payout_id: int) -> InlineKeyboardMarkup:
    return kb([
        InlineKeyboardButton.model_construct(text='Принять', callback_data=f'approve_payout_{payout_id}', icon_custom_emoji_id='5895514131896733546', style='success'),
        InlineKeyboardButton.model_construct(text='Отклонить', callback_data=f'reject_payout_{payout_id}', icon_custom_emoji_id='5893163582194978381', style='danger'),
    ])

def survey_approve_kb(user_id: int) -> InlineKeyboardMarkup:
    return kb([
        InlineKeyboardButton.model_construct(text='Принять', callback_data=f'approve_survey_{user_id}', icon_custom_emoji_id='5895514131896733546', style='success'),
        InlineKeyboardButton.model_construct(text='Отклонить', callback_data=f'reject_survey_{user_id}', icon_custom_emoji_id='5893163582194978381', style='danger'),
    ])

def media_done_kb(count: int = 0) -> InlineKeyboardMarkup:
    label = f'Готово ({count} фото)' if count else 'Я отправил все фото'
    return kb(
        [InlineKeyboardButton.model_construct(text=label, callback_data='media_done', icon_custom_emoji_id='5895514131896733546', style='success')],
        [InlineKeyboardButton.model_construct(text='Отмена', callback_data='back_main', icon_custom_emoji_id='5893163582194978381', style='danger')],
    )

def admin_settings_kb() -> InlineKeyboardMarkup:
    return kb(
        [btn('Список пользователей', 'user_list_0', 6032609071373226027)],
        [btn('Поиск пользователя', 'search_user_btn', 5188217332748527444)],
        [btn('Поиск по коду выплаты', 'search_payout_code_btn', 5363858422590619939)],
        [btn('Изменить проценты', 'edit_global_percent', 5231200819986047254)],
        [btn('Управление ворками', 'manage_work_types', 5893161718179173515)],
        [btn('Добавить админа', 'add_admin_btn', 5217822164362739968, 'success'),
         btn('Снять админа', 'remove_admin_btn', 5210952531676504517, 'danger')],
        [btn('Назад', 'back_main', 5960671702059848143)],
    )

async def require_approved(user_id: int, msg_or_cb) -> bool:
    """
    Проверяет, одобрен ли пользователь.
    Если нет — отправляет соответствующее сообщение и возвращает False.
    """
    approved = await get_approved(user_id)
    if approved == 1 or user_id in OWNERS:
        return True

    text = (
        f'<b>{E["warn"]} Для использования бота необходимо пройти анкету.</b>\n\n'
        f'{E["arrow"]} Напишите /start чтобы начать.'
    )
    if isinstance(msg_or_cb, Message):
        await msg_or_cb.answer(text)
    else:  # CallbackQuery
        await msg_or_cb.answer('⚠️ Сначала пройдите анкету', show_alert=True)
        try:
            await msg_or_cb.message.answer(text)
        except Exception:
            pass
    return False

def format_profile(user_id, nickname, username, role, profits, join_date) -> str:
    days = 0
    if join_date:
        try: days = (datetime.now() - datetime.strptime(join_date, '%Y-%m-%d')).days
        except: pass
    role_e = E['crown'] if role == 'admin' else E['bolt2']
    role_t = 'Администратор' if role == 'admin' else 'Воркер'
    return (
        f'<b>{E["user3"]} {nickname}</b> (@{username})\n\n'
        f'<blockquote>'
        f'<b>{E["phone"]} ID:</b> <code>{user_id}</code>\n'
        f'<b>Роль:</b> {role_e} {role_t}\n'
        f'<b>{E["money"]} Профит:</b> <code>{profits:.2f} TON</code>\n'
        f'<b>{E["cal"]} Дней в команде:</b> {days}\n'
        f'<b>{E["cal"]} Дата вступления:</b> {join_date or "—"}'
        f'</blockquote>'
    )
# ── /start ────────────────────────────────────────────────────────────────────

@router.message(Command('cancel'))
async def cmd_cancel(msg: Message, state: FSMContext):
    """Отмена текущего действия (ввод суммы, причина отклонения и т.д.)"""
    admin_id = msg.from_user.id
    current = await state.get_state()
    if current is None:
        await msg.answer(f'<b>{E["info"]} Нет активного действия для отмены.</b>')
        return
    # Чистим approve_ctx если был в процессе одобрения
    if admin_id in approve_ctx:
        ctx = approve_ctx.pop(admin_id)
        prompt_id = ctx.get('prompt_msg_id')
        if prompt_id:
            try: await bot.delete_message(admin_id, prompt_id)
            except: pass
    # Чистим reject_ctx если был в процессе отклонения
    if admin_id in reject_ctx:
        ctx = reject_ctx.pop(admin_id)
        prompt_id = ctx.get('prompt_msg_id')
        if prompt_id:
            try: await bot.delete_message(admin_id, prompt_id)
            except: pass
    await state.clear()
    is_adm = await is_admin(admin_id)
    await msg.answer(
        f'<b>{E["ok"]} Действие отменено.</b>',
        reply_markup=main_menu(is_adm))

@router.message(Command('start'))
async def cmd_start(msg: Message, state: FSMContext):
    user_id  = msg.from_user.id
    username = msg.from_user.username or 'без_username'
    name     = msg.from_user.full_name or 'Без имени'
    await db_upsert_user(user_id, username, name)
    await state.clear()

    in_channel = await check_subscribed(user_id)
    if not in_channel:
        await msg.answer(
            f'<b>{E["stop2"]} Для использования бота нужна подписка на переходник.</b>',
            reply_markup=sub_check_kb())
        return

    approved = await get_approved(user_id)
    is_adm   = await is_admin(user_id)

    if approved is None:
        await state.set_state(Survey.q1)
        await state.update_data(in_chat=msg.chat.type != 'private')
        await msg.answer(
            f'<b>{E["hi"]} @{username}, привет!</b>\n'
            f'Перед началом ответь на 3 коротких вопроса.\n\n'
            f'<b>{E["question"]} Знаете ли вы, что такое ворк?</b>',
            reply_markup=survey_kb(1, user_id))
        return

    if approved == 0:
        if await has_pending_survey(user_id):
            await msg.answer(f'<b>{E["hourglass"]} Ваша анкета на рассмотрении.</b>\nОжидайте решения администратора.')
            return
        await state.set_state(Survey.q1)
        await msg.answer(
            f'<b>{E["hi"]} Нужно пройти анкету.</b>\n\n'
            f'<b>{E["question"]} Знаете ли вы, что такое ворк?</b>',
            reply_markup=survey_kb(1, user_id))
        return

    await msg.answer(
        f'<b>{E["sparkle"]} Добро пожаловать в Astral Team!</b>\n\n'
        f'{E["user3"]} <b>{name}</b>, рады видеть вас! {E["down"]}',
        reply_markup=main_menu(is_adm))

# ── check_sub ─────────────────────────────────────────────────────────────────
@router.callback_query(F.data == 'check_sub')
async def cb_check_sub(cb: CallbackQuery, state: FSMContext):
    user_id  = cb.from_user.id
    username = cb.from_user.username or 'без_username'
    name     = cb.from_user.full_name or 'Без имени'
    await db_upsert_user(user_id, username, name)

    in_channel = await check_subscribed(user_id)
    if not in_channel:
        await cb.answer('❌ Подписка не найдена!')
        await cb.message.edit_text(
            f'<b>{E["stop2"]} Для использования бота нужна подписка.</b>',
            reply_markup=sub_check_kb())
        return

    approved = await get_approved(user_id)
    is_adm   = await is_admin(user_id)

    if approved is None:
        await state.set_state(Survey.q1)
        await cb.message.edit_text(
            f'<b>{E["hi"]} Спасибо за подписку!</b>\n\n'
            f'<b>{E["question"]} Знаете ли вы, что такое ворк?</b>',
            reply_markup=survey_kb(1, user_id))
    elif approved == 0:
        if await has_pending_survey(user_id):
            await cb.message.edit_text(f'<b>{E["hourglass"]} Ваша анкета на рассмотрении.</b>')
        else:
            await state.set_state(Survey.q1)
            await cb.message.edit_text(
                f'<b>{E["hi"]} Пройдите анкету.</b>\n\n'
                f'<b>{E["question"]} Знаете ли вы, что такое ворк?</b>',
                reply_markup=survey_kb(1, user_id))
    else:
        await cb.message.edit_text(
            f'<b>{E["check"]} Подписка подтверждена!</b>\n\nДобро пожаловать в Astral Team.',
            reply_markup=main_menu(is_adm))
    await cb.answer()

# ── Анкета — кнопки ──────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith('survey_'))
async def cb_survey(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split('_')
    action, step, target_id = parts[1], int(parts[2]), int(parts[3])

    if cb.from_user.id != target_id:
        await cb.answer('❌ Это сообщение не для вас.')
        return

    current = await state.get_state()
    if current not in (Survey.q1, Survey.q2):
        await cb.answer('Пожалуйста, начните с /start.')
        return

    answer = 'Да' if action == 'yes' else 'Нет'
    data   = await state.get_data()
    answers = data.get('answers', {})
    answers[f'q{step}'] = answer
    await state.update_data(answers=answers)

    if step == 1:
        await state.set_state(Survey.q2)
        await cb.message.edit_text(
            f'<b>{E["check"]} Ответ принят:</b> {answer}\n\n'
            f'<b>{E["question"]} Разбираетесь ли вы в NFT?</b>',
            reply_markup=survey_kb(2, target_id))
    elif step == 2:
        await state.set_state(Survey.q3)
        await cb.message.edit_text(
            f'<b>{E["check"]} Ответ принят:</b> {answer}\n\n'
            f'<b>{E["question"]} Сколько времени готовы уделять ворку?</b>\n<i>(Напишите текстом)</i>')
    await cb.answer()

# Анкета — шаг 3 (текст)
@router.message(Survey.q3)
async def survey_q3(msg: Message, state: FSMContext):
    user_id  = msg.from_user.id
    username = msg.from_user.username or 'без_username'
    name     = msg.from_user.full_name or 'Без имени'
    data     = await state.get_data()
    answers  = data.get('answers', {})
    answers['q3'] = msg.text
    await state.clear()

    async with db_connect() as conn:
        await conn.execute(
            'INSERT OR REPLACE INTO surveys (user_id,answer1,answer2,answer3,status) VALUES (?,?,?,?,"pending")',
            (user_id, answers.get('q1'), answers.get('q2'), answers.get('q3')))
        await conn.execute(
            'INSERT OR IGNORE INTO users (user_id,username,nickname,role,join_date,approved) VALUES (?,?,?,"worker",?,0)',
            (user_id, username, name, datetime.now().strftime('%Y-%m-%d')))
        await conn.execute('UPDATE users SET approved=0, username=?, nickname=? WHERE user_id=?', (username, name, user_id))
        await conn.commit()
    await msg.answer(
        f'<b>{E["mail"]} Спасибо!</b>\nВаша анкета отправлена на рассмотрение администратора.')

    admins = await get_admins()
    for adm in admins:
        try:
            await bot.send_message(adm,
                f'<b>{E["write"]} Новая анкета от @{username}</b>\n'
                f'<b>ID:</b> <code>{user_id}</code>\n\n'
                f'<blockquote>'
                f'<b>{E["n1"]} Что такое ворк:</b> {answers.get("q1")}\n'
                f'<b>{E["n2"]} NFT:</b> {answers.get("q2")}\n'
                f'<b>{E["n3"]} Время:</b> {answers.get("q3")}'
                f'</blockquote>',
                reply_markup=survey_approve_kb(user_id))
        except Exception:
            pass

# ── Анкета: одобрение/отклонение ─────────────────────────────────────────────
@router.callback_query(F.data.startswith('approve_survey_'))
async def cb_approve_survey(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав администратора')
        return
    user_id = int(cb.data.split('_')[-1])
    async with db_connect() as conn:
        async with conn.execute('SELECT status,answer1,answer2,answer3 FROM surveys WHERE user_id=?', (user_id,)) as c:
            row = await c.fetchone()
        if not row: await cb.answer('Анкета не найдена'); return
        if row[0] == 'approved': await cb.answer('Уже одобрена'); return
        if row[0] == 'rejected': await cb.answer('Уже отклонена'); return
        status, a1, a2, a3 = row
        async with conn.execute('SELECT username FROM users WHERE user_id=?', (user_id,)) as c:
            u = await c.fetchone()
        uname = u[0] if u else str(user_id)
        await conn.execute('UPDATE users SET approved=1 WHERE user_id=?', (user_id,))
        await conn.execute('UPDATE surveys SET status="approved" WHERE user_id=?', (user_id,))
        await conn.commit()
    try:
        await cb.message.edit_text(
            f'<b>{E["ok"]} Анкета одобрена</b>\n\n'
            f'<b>{E["user2"]} Пользователь:</b> @{uname} (<code>{user_id}</code>)\n\n'
            f'<blockquote>'
            f'<b>{E["n1"]} Что такое ворк:</b> {a1}\n'
            f'<b>{E["n2"]} NFT:</b> {a2}\n'
            f'<b>{E["n3"]} Время:</b> {a3}'
            f'</blockquote>')
    except Exception: pass

    is_adm = await is_admin(user_id)
    try:
        await bot.send_message(user_id,
            f'<b>{E["party"]} Ваша анкета одобрена!</b>\n\n{E["rocket"]} Добро пожаловать в команду!',
            reply_markup=main_menu(is_adm))
    except Exception: pass
    await cb.answer('Анкета одобрена ✅')

@router.callback_query(F.data.startswith('reject_survey_'))
async def cb_reject_survey(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав администратора')
        return
    user_id = int(cb.data.split('_')[-1])
    async with db_connect() as conn:
        async with conn.execute('SELECT status,answer1,answer2,answer3 FROM surveys WHERE user_id=?', (user_id,)) as c:
            row = await c.fetchone()
        if not row: await cb.answer('Анкета не найдена'); return
        if row[0] != 'pending': await cb.answer('Уже обработана'); return
        status, a1, a2, a3 = row
        async with conn.execute('SELECT username FROM users WHERE user_id=?', (user_id,)) as c:
            u = await c.fetchone()
        uname = u[0] if u else str(user_id)
        await conn.execute('UPDATE surveys SET status="rejected" WHERE user_id=?', (user_id,))
        await conn.execute('UPDATE users SET approved=0 WHERE user_id=?', (user_id,))
        await conn.commit()
    try:
        await cb.message.edit_text(
            f'<b>{E["no2"]} Анкета отклонена</b>\n\n'
            f'<b>{E["user2"]} Пользователь:</b> @{uname} (<code>{user_id}</code>)\n\n'
            f'<blockquote>'
            f'<b>{E["n1"]} Что такое ворк:</b> {a1}\n'
            f'<b>{E["n2"]} NFT:</b> {a2}\n'
            f'<b>{E["n3"]} Время:</b> {a3}'
            f'</blockquote>')
    except Exception: pass
    try:
        await bot.send_message(user_id,
            f'<b>{E["no2"]} Ваша анкета отклонена</b>\n\n'
            f'{E["warn"]} Обратитесь к администратору за уточнениями.')
    except Exception: pass
    await cb.answer('Анкета отклонена ❌')


# ── Навигация ─────────────────────────────────────────────────────────────────
@router.callback_query(F.data == 'back_main')
async def cb_back_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = cb.from_user.id
    is_adm  = await is_admin(user_id)
    text = f'<b>{E["down"]} Выберите пункт меню:</b>'
    try:
        await cb.message.edit_text(text, reply_markup=main_menu(is_adm))
    except Exception:
        try:
            await cb.message.delete()
        except Exception:
            pass
        await bot.send_message(user_id, text, reply_markup=main_menu(is_adm))
    await cb.answer()

# ── Профиль ───────────────────────────────────────────────────────────────────
@router.callback_query(F.data == 'profile')
async def cb_profile(cb: CallbackQuery):
    user_id = cb.from_user.id
    if not await require_approved(user_id, cb):
        return
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT username,nickname,role,total_profits,join_date FROM users WHERE user_id=?',
            (user_id,)) as c:
            row = await c.fetchone()
    if not row: await cb.answer('Профиль не найден'); return
    username, nickname, role, profits, join_date = row
    await cb.message.edit_text(
        format_profile(user_id, nickname or 'Без имени', username or '—', role, profits, join_date),
        reply_markup=back_btn())
    await cb.answer()

# ── Топ ───────────────────────────────────────────────────────────────────────
async def render_top() -> str:
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT nickname,total_profits,username,role FROM users WHERE approved=1 ORDER BY total_profits DESC LIMIT 10') as c:
            users = await c.fetchall()
        async with conn.execute('SELECT SUM(total_profits) FROM users WHERE approved=1') as c:
            total = (await c.fetchone())[0] or 0
    if not users:
        return f'{E["chart"]} Топ воркеров пока пуст.'
    medals = [E['gold'], E['silver'], E['bronze']]
    nums   = [E['n4'],E['n5'],E['n6'],E['n7'],E['n8'],E['n9'],e(5794375786743995258,'0️⃣')]
    text   = f'<b>{E["trophy"]} ТОП 10 ВОРКЕРОВ</b>\n\n'
    for i, (nick, profit, uname, role) in enumerate(users, 1):
        if i <= 3: medal = medals[i-1]
        elif i <= 10: medal = nums[i-4]
        else: medal = f'{i}.'
        name = nick or (f'@{uname}' if uname else f'Воркер {i}')
        role_t = ' <i>(Админ)</i>' if role == 'admin' else ''
        text += f'{medal} <b>{name}</b>{role_t} — <code>{profit:.2f} TON</code>\n'
    text += f'\n<b>{E["cash"]} Касса команды:</b> <code>{total:.2f} TON</code>'
    return text

@router.callback_query(F.data == 'top')
async def cb_top(cb: CallbackQuery):
    if not await require_approved(cb.from_user.id, cb):
        return
    await cb.message.edit_text(await render_top(), reply_markup=back_btn())
    await cb.answer()

@router.message(Command('top'))
async def cmd_top(msg: Message):
    if not await require_approved(msg.from_user.id, msg):
        return
    await msg.answer(await render_top(), reply_markup=back_btn())

# ── Инфо ──────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == 'info')
async def cb_info(cb: CallbackQuery):
    if not await require_approved(cb.from_user.id, cb):
        return
    await cb.message.edit_text(
        f'<b>{E["info"]} Astral Team</b>\n\n'
        f'{E["crown"]} <b>Владелец:</b> @nekodeal\n'
        f'{E["hat"]} <b>Совладелец:</b> @sluts1337',
        reply_markup=kb(
            [InlineKeyboardButton.model_construct(text='Чат команды', url='https://t.me/+Qyt8Lrnge3VlNDI0', icon_custom_emoji_id='5443038326535759644')],
            [btn('Назад', 'back_main', 5960671702059848143)]))
    await cb.answer()

# ── Мои заявки ────────────────────────────────────────────────────────────────
PAYOUTS_PER_PAGE = 5

STATUS_LABELS = {
    'pending':  ('⏳', 'На рассмотрении'),
    'approved': ('✅', 'Выплачено'),
    'rejected': ('❌', 'Отклонено'),
}

STATUS_ICON_IDS = {
    'pending':  5902050947567194830,
    'approved': 5895514131896733546,
    'rejected': 5893163582194978381,
}
STATUS_FALLBACK = {
    'pending':  '⏳',
    'approved': '✅',
    'rejected': '❌',
}

def my_payouts_kb(payouts: list, page: int, total: int, per_page: int) -> InlineKeyboardMarkup:
    rows = []
    for p in payouts:
        pid, code, wt, status, created_at, profit = p
        wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
        label = f'{code}  •  {wt_name}'
        icon_id = STATUS_ICON_IDS.get(status)
        btn_kwargs = dict(text=label, callback_data=f'my_payout_detail_{pid}')
        if icon_id:
            btn_kwargs['icon_custom_emoji_id'] = str(icon_id)
        rows.append([InlineKeyboardButton.model_construct(**btn_kwargs)])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton.model_construct(
            text='Назад', callback_data=f'my_payouts_{page-1}',
            icon_custom_emoji_id='5960671702059848143'))
    total_pages = (total + per_page - 1) // per_page
    if total_pages > 1:
        nav.append(InlineKeyboardButton.model_construct(text=f'{page+1}/{total_pages}', callback_data='noop'))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton.model_construct(
            text='Вперёд', callback_data=f'my_payouts_{page+1}',
            icon_custom_emoji_id='6037622221625626773'))
    if nav:
        rows.append(nav)
    rows.append([btn('Главное меню', 'back_main', 5960671702059848143)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def payout_detail_kb(payout_id: int, page: int) -> InlineKeyboardMarkup:
    return kb(
        [btn('К списку заявок', f'my_payouts_{page}', 5960671702059848143)],
    )

@router.callback_query(F.data.startswith('my_payouts_'))
async def cb_my_payouts(cb: CallbackQuery):
    user_id = cb.from_user.id
    if not await require_approved(user_id, cb):
        return
    try:
        page = int(cb.data.split('_')[-1])
    except Exception:
        page = 0
    per_page = PAYOUTS_PER_PAGE
    offset = page * per_page
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT COUNT(*) FROM payouts WHERE user_id=?', (user_id,)) as c:
            total = (await c.fetchone())[0]
        async with conn.execute(
            'SELECT id,payout_code,work_type,status,created_at,profit_amount FROM payouts WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?',
            (user_id, per_page, offset)) as c:
            payouts = await c.fetchall()
    if total == 0:
        try:
            await cb.message.edit_text(
                f'<b>{E["phone"]} Мои заявки</b>\n\n'
                f'<i>У вас ещё нет заявок.</i>\n'
                f'Нажмите <b>Выплата</b> чтобы создать первую!',
                reply_markup=kb([btn('Главное меню', 'back_main', 5960671702059848143)]))
        except Exception:
            pass
        await cb.answer()
        return
    total_pages = (total + per_page - 1) // per_page
    counts = {'pending': 0, 'approved': 0, 'rejected': 0}
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT status,COUNT(*) FROM payouts WHERE user_id=? GROUP BY status', (user_id,)) as c:
            for row in await c.fetchall():
                counts[row[0]] = row[1]
    text = (
        f'<b>{E["phone"]} Мои заявки</b> · <i>стр. {page+1}/{total_pages}</i>\n\n'
        f'<blockquote>'
        f'{e(5895514131896733546,"✅")} Выплачено: <b>{counts["approved"]}</b>   '
        f'{e(5902050947567194830,"⏳")} Ожидают: <b>{counts["pending"]}</b>   '
        f'{e(5893163582194978381,"❌")} Откл: <b>{counts["rejected"]}</b>'
        f'</blockquote>\n'
        f'<i>Нажмите на заявку для деталей:</i>'
    )
    try:
        await cb.message.edit_text(text, reply_markup=my_payouts_kb(payouts, page, total, per_page))
    except Exception:
        pass
    await cb.answer()

@router.callback_query(F.data == 'noop')
async def cb_noop(cb: CallbackQuery):
    await cb.answer()

@router.callback_query(F.data.startswith('my_payout_detail_'))
async def cb_my_payout_detail(cb: CallbackQuery):
    user_id = cb.from_user.id
    if not await require_approved(user_id, cb):
        return
    try:
        payout_id = int(cb.data.split('_')[-1])
    except Exception:
        await cb.answer()
        return
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT id,payout_code,work_type,status,created_at,profit_amount,gift_link,deal_code,user_id FROM payouts WHERE id=?',
            (payout_id,)) as c:
            row = await c.fetchone()
    if not row or row[8] != user_id:
        await cb.answer('Заявка не найдена')
        return
    pid, code, wt, status, created_at, profit, gift_link, deal_code, _ = row
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
    status_map = {
        'pending':  (e(5902050947567194830,"⏳"), 'На рассмотрении'),
        'approved': (e(5895514131896733546,"✅"), 'Выплачено'),
        'rejected': (e(5893163582194978381,"❌"), 'Отклонено'),
    }
    sem, slabel = status_map.get(status, ('❓', status))
    profit_line = f'\n<b>{E["money"]} Сумма:</b> <code>{profit:.2f} TON</code>' if status == 'approved' and profit else ''
    deal_line = f'\n<b>Код сделки:</b> <code>{deal_code}</code>' if deal_code else ''
    gift_lines = [l.strip() for l in (gift_link or '').splitlines() if l.strip()]
    if gift_lines:
        gift_block = f'\n\n<b>{E["gift"]} Подарки:</b>\n<blockquote>' + '\n'.join(gift_lines) + '</blockquote>'
    else:
        gift_block = ''
    text = (
        f'<b>{E["phone"]} Заявка {code}</b>\n\n'
        f'<blockquote>'
        f'<b>Статус:</b> {sem} {slabel}\n'
        f'<b>Тип:</b> {wt_name}'
        f'{deal_line}'
        f'<b>\n{E["cal"]} Дата:</b> {created_at or "—"}'
        f'{profit_line}'
        f'</blockquote>'
        f'{gift_block}'
    )
    # Вычисляем страницу для кнопки назад
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT COUNT(*) FROM payouts WHERE user_id=? AND id>?', (user_id, payout_id)) as c:
            pos_from_end = (await c.fetchone())[0]
    back_page = pos_from_end // PAYOUTS_PER_PAGE

    # Отправляем медиафайлы отдельно если заявка approved
    detail_kb = payout_detail_kb(payout_id, back_page)
    try:
        await cb.message.edit_text(text, reply_markup=detail_kb)
    except Exception:
        pass
    await cb.answer()

# ── Выплата ───────────────────────────────────────────────────────────────────
@router.callback_query(F.data == 'payout')
async def cb_payout(cb: CallbackQuery):
    user_id  = cb.from_user.id
    if not await require_approved(user_id, cb):
        return
    types = await get_enabled_work_types()
    if not types:
        await cb.message.edit_text(
            f'<b>{E["warn"]} Все типы ворка временно отключены.</b>',
            reply_markup=back_btn())
        await cb.answer()
        return
    await cb.message.edit_text(
        f'<b>{E["cash"]} Выберите способ ворка:</b>',
        reply_markup=work_type_kb(types))
    await cb.answer()

@router.callback_query(F.data.startswith('work_'))
async def cb_work_type(cb: CallbackQuery, state: FSMContext):
    user_id   = cb.from_user.id
    if not await require_approved(user_id, cb):
        return
    work_type = cb.data.split('_', 1)[1]

    # Проверка что тип включён
    async with db_connect() as conn:
        async with conn.execute('SELECT enabled FROM work_types WHERE work_type=?', (work_type,)) as c:
            row = await c.fetchone()
    if not row or not row[0]:
        await cb.answer('❌ Этот тип ворка временно недоступен')
        return

    pct       = await get_user_percentage(user_id, work_type)
    wt_name   = WORK_TYPES.get(work_type, {}).get('name', work_type)

    payout_temp[user_id] = {
        'work_type': work_type,
        'work_type_name': wt_name,
        'percentage': pct,
        'media_files': [],
        'deal_code': None,
        'gift_link': None,
        'ton_address': None,
        'bot_msg_id': cb.message.message_id,
        'chat_id': cb.message.chat.id,
    }

    if work_type == 'otc':
        await state.set_state(Payout.deal_code)
        await cb.message.edit_text(
            f'<b>{E["cash"]} Заявка: {wt_name}</b>\n'
            f'<blockquote><b>Процент:</b> {int(pct*100)}%</blockquote>\n\n'
            f'<b>{E["n1"]} Введите код сделки (<code>#код</code>):</b>',
            reply_markup=kb([InlineKeyboardButton.model_construct(text='Отмена', callback_data='back_main', icon_custom_emoji_id='5893163582194978381', style='danger')]))
    else:
        await state.set_state(Payout.media)
        await cb.message.edit_text(
            f'<b>{E["cash"]} Заявка: {wt_name}</b>\n'
            f'<blockquote><b>Процент:</b> {int(pct*100)}%</blockquote>\n\n'
            f'<b>{E["n1"]} Отправьте скриншоты.</b>\n'
            f'Нажмите «Готово», когда загрузите всё.\n\n'
            f'<i>Загружено: <b>0</b> фото</i>',
            reply_markup=media_done_kb(0))
    await cb.answer()

@router.message(Payout.deal_code)
async def payout_deal_code(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    try: await msg.delete()
    except: pass
    cancel_kb = kb([InlineKeyboardButton.model_construct(text='Отмена', callback_data='back_main', icon_custom_emoji_id='5893163582194978381', style='danger')])
    if not msg.text or not msg.text.startswith('#'):
        if user_id in payout_temp:
            try:
                await bot.edit_message_text(
                    f'<b>{E["no"]} Введите код сделки начиная с #</b>\n\n'
                    f'<b>{E["n1"]} Введите код сделки (<code>#код</code>):</b>',
                    chat_id=payout_temp[user_id]['chat_id'],
                    message_id=payout_temp[user_id]['bot_msg_id'],
                    reply_markup=cancel_kb)
            except: pass
        return
    payout_temp[user_id]['deal_code'] = msg.text
    await state.set_state(Payout.media)
    media_text = (
        f'<b>{E["n2"]} Отправьте скриншоты (можно несколько).</b>\n'
        f'Нажмите «Готово», когда загрузите всё.\n\n'
        f'<i>Загружено: <b>0</b> фото</i>'
    )
    try:
        await bot.edit_message_text(
            media_text,
            chat_id=payout_temp[user_id]['chat_id'],
            message_id=payout_temp[user_id]['bot_msg_id'],
            reply_markup=media_done_kb(0))
    except:
        sent = await msg.answer(media_text, reply_markup=media_done_kb(0))
        payout_temp[user_id]['bot_msg_id'] = sent.message_id
        payout_temp[user_id]['chat_id'] = sent.chat.id

@router.message(Payout.media, F.photo | F.document)
async def payout_media(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    if user_id not in payout_temp: return
    if msg.photo:
        payout_temp[user_id]['media_files'].append(('photo', msg.photo[-1].file_id))
    elif msg.document:
        payout_temp[user_id]['media_files'].append(('document', msg.document.file_id))
    count = len(payout_temp[user_id]['media_files'])
    # Удаляем сообщение пользователя с фото
    try: await msg.delete()
    except: pass
    # Обновляем сообщение бота счётчиком
    try:
        await bot.edit_message_text(
            f'<b>{E["cash"]} Заявка: {payout_temp[user_id]["work_type_name"]}</b>\n'
            f'<blockquote><b>Процент:</b> {int(payout_temp[user_id]["percentage"]*100)}%</blockquote>\n\n'
            f'<b>{E["n1"]} Отправьте скриншоты.</b>\n'
            f'Нажмите «Готово», когда загрузите всё.\n\n'
            f'<i>Загружено: <b>{count}</b> фото</i>',
            chat_id=payout_temp[user_id]['chat_id'],
            message_id=payout_temp[user_id]['bot_msg_id'],
            reply_markup=media_done_kb(count))
    except: pass


@router.callback_query(F.data == 'media_done')
async def cb_media_done(cb: CallbackQuery, state: FSMContext):
    user_id = cb.from_user.id
    if user_id not in payout_temp:
        await cb.answer('Сессия истекла. Начните заново.')
        return
    if not payout_temp[user_id]['media_files']:
        await cb.answer('❌ Отправьте хотя бы один скриншот!')
        return
    count = len(payout_temp[user_id]['media_files'])
    await state.set_state(Payout.gift_link)
    nft_text = (
        f'<b>{E["n2"]} Введите ссылку(и) на NFT:</b>\n'
        f'<code>https://t.me/nft/...</code>\n'
        f'<i>(Можно несколько, каждая с новой строки)</i>\n\n'
        f'<i>Скриншотов загружено: <b>{count}</b></i>'
    )
    try:
        await cb.message.edit_text(nft_text)
    except Exception:
        try: await cb.message.delete()
        except: pass
        await bot.send_message(user_id, nft_text)
    await cb.answer()

@router.message(Payout.gift_link)
async def payout_gift_link(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    try: await msg.delete()
    except: pass
    raw_links = [l.strip() for l in (msg.text or '').splitlines() if l.strip()]
    valid_links = [l for l in raw_links if is_valid_nft_link(l)]
    if not valid_links:
        if user_id in payout_temp:
            try:
                await bot.edit_message_text(
                    f'<b>{E["no"]} Неверный формат ссылки.</b>\n'
                    f'Формат: <code>https://t.me/nft/...</code>\n\n'
                    f'<b>{E["n2"]} Введите ссылку(и) на NFT:</b>\n'
                    f'<code>https://t.me/nft/...</code>\n'
                    f'<i>(Можно несколько, каждая с новой строки)</i>',
                    chat_id=payout_temp[user_id]['chat_id'],
                    message_id=payout_temp[user_id]['bot_msg_id'])
            except: pass
        return
    payout_temp[user_id]['gift_link'] = '\n'.join(valid_links)
    await state.set_state(Payout.ton_addr)
    ton_text = f'<b>{E["n3"]} Введите ваш TON-адрес:</b>'
    try:
        await bot.edit_message_text(
            ton_text,
            chat_id=payout_temp[user_id]['chat_id'],
            message_id=payout_temp[user_id]['bot_msg_id'])
    except:
        sent = await msg.answer(ton_text)
        payout_temp[user_id]['bot_msg_id'] = sent.message_id
        payout_temp[user_id]['chat_id'] = sent.chat.id


@router.message(Payout.ton_addr)
async def payout_ton_addr(msg: Message, state: FSMContext):
    user_id = msg.from_user.id
    try: await msg.delete()
    except: pass
    if not is_valid_ton(msg.text or ''):
        if user_id in payout_temp:
            try:
                await bot.edit_message_text(
                    f'<b>{E["no"]} Неверный TON-адрес.</b>\n'
                    f'Должен начинаться с <code>UQ</code> или <code>EQ</code>.\n\n'
                    f'<b>{E["n3"]} Введите ваш TON-адрес:</b>',
                    chat_id=payout_temp[user_id]['chat_id'],
                    message_id=payout_temp[user_id]['bot_msg_id'])
            except: pass
        return
    payout_temp[user_id]['ton_address'] = msg.text
    await state.clear()
    await submit_payout(msg, user_id)


async def submit_payout(msg: Message, user_id: int):
    data      = payout_temp.pop(user_id, {})
    code      = generate_payout_code()
    username  = msg.from_user.username or 'без_username'
    wt_name   = data['work_type_name']
    pct       = data['percentage']
    files     = data.get('media_files', [])
    photo_count = len(files)

    async with db_connect() as conn:
        await conn.execute('''
            INSERT INTO payouts (user_id,payout_code,work_type,deal_code,gift_link,ton_address,status,created_at,user_percentage)
            VALUES (?,?,?,?,?,?,"pending",?,?)''',
            (user_id, code, data['work_type'], data.get('deal_code'),
             data.get('gift_link'), data.get('ton_address'),
             datetime.now().strftime('%Y-%m-%d %H:%M:%S'), pct))
        await conn.commit()
        async with conn.execute('SELECT last_insert_rowid()') as c:
            payout_id = (await c.fetchone())[0]
    # Сохраняем медиафайлы в БД (персистентно) и в кэш памяти
    if files:
        payout_media_store[payout_id] = list(files)
        async with db_connect() as conn:
            for ftype, fid in files:
                await conn.execute(
                    'INSERT INTO payout_media (payout_id, media_type, file_id) VALUES (?,?,?)',
                    (payout_id, ftype, fid))
            await conn.commit()

    admin_text = (
        f'<b>{E["cash"]} Новая заявка #{payout_id}</b>\n\n'
        f'<blockquote>'
        f'<b>Код:</b> <code>{code}</code>\n'
        f'<b>Тип:</b> {wt_name}\n'
        f'<b>{E["user"]} Воркер:</b> @{username} (<code>{user_id}</code>)\n'
        f'<b>Процент:</b> {int(pct*100)}%\n'
        f'<b>Скриншотов:</b> {photo_count}'
        f'</blockquote>\n\n'
        f'<b>NFT:</b>\n<blockquote>{data.get("gift_link","—")}</blockquote>\n'
        f'<b>Адрес:</b>\n<blockquote><code>{data.get("ton_address","—")}</code></blockquote>'
    )

    admins = await get_admins()
    for adm in admins:
        try:
            if files:
                from aiogram.types import InputMediaPhoto, InputMediaDocument
                media_group = []
                for ftype, fid in files:
                    if ftype == 'photo':
                        media_group.append(InputMediaPhoto(media=fid))
                    else:
                        media_group.append(InputMediaDocument(media=fid))
                await bot.send_media_group(adm, media_group)
            await bot.send_message(adm, admin_text, reply_markup=payout_approve_kb(payout_id))
        except Exception:
            pass

    gift_link_raw = data.get('gift_link', '') or ''
    gift_lines = [l.strip() for l in gift_link_raw.splitlines() if l.strip()]
    gift_block = ''
    if gift_lines:
        gift_block = f'\n<b>{E["gift"]} Подарки:</b>\n<blockquote>' + '\n'.join(gift_lines) + '</blockquote>'

    await msg.answer(
        f'<b>{E["check"]} Заявка отправлена на рассмотрение!</b>\n\n'
        f'<blockquote>'
        f'<b>{E["phone"]} Код выплаты:</b> <code>{code}</code>\n'
        f'<b>Скриншотов загружено:</b> {photo_count}'
        f'</blockquote>'
        f'{gift_block}')

# ── Одобрение выплаты ─────────────────────────────────────────────────────────
@router.callback_query(F.data.startswith('approve_payout_'))
async def cb_approve_payout(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав администратора')
        return
    payout_id = int(cb.data.split('_')[-1])
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT status,payout_code,work_type,user_id,user_percentage FROM payouts WHERE id=?',
            (payout_id,)) as c:
            row = await c.fetchone()
    if not row or row[0] != 'pending':
        await cb.answer('❌ Заявка не актуальна')
        return
    status, code, wt, uid, u_pct = row
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)

    # Защита от двойного нажатия: если этот админ уже в процессе — отменяем
    if cb.from_user.id in approve_ctx:
        await cb.answer('⚠️ Вы уже обрабатываете заявку. Завершите или отмените её.')
        return

    approve_ctx[cb.from_user.id] = {
        'payout_id': payout_id,
        'payout_code': code,
        'work_type': wt,
        'work_type_name': wt_name,
        'payout_user_id': uid,
        'user_percentage': u_pct,
        'origin_msg_id': cb.message.message_id,
        'origin_chat_id': cb.message.chat.id,
    }
    await state.set_state(AdminAction.waiting_amount_approve)
    prompt = await bot.send_message(
        cb.from_user.id,
        f'<b>{E["money"]} Введите сумму профита (TON) для {wt_name}:</b>\n'
        f'<i>Для отмены нажмите /cancel</i>')
    approve_ctx[cb.from_user.id]['prompt_msg_id'] = prompt.message_id
    await cb.answer()

@router.message(AdminAction.waiting_amount_approve)
async def admin_enter_amount(msg: Message, state: FSMContext):
    admin_id = msg.from_user.id
    if admin_id not in approve_ctx:
        await state.clear()
        return
    try:
        amount = float(msg.text.replace(',', '.'))
        if amount <= 0: raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Введите корректное число, например: 12.5</b>')
        return

    ctx       = approve_ctx.pop(admin_id)
    payout_id = ctx['payout_id']
    code      = ctx['payout_code']
    wt        = ctx['work_type']
    wt_name   = ctx['work_type_name']
    uid       = ctx['payout_user_id']
    u_pct     = ctx['user_percentage']
    origin_id = ctx['origin_msg_id']
    prompt_id = ctx.get('prompt_msg_id')
    await state.clear()

    try: await msg.delete()
    except: pass
    try:
        if prompt_id:
            await bot.delete_message(admin_id, prompt_id)
    except: pass

    # Повторная проверка статуса — защита от race condition между двумя админами
    async with db_connect() as conn:
        async with conn.execute('SELECT status FROM payouts WHERE id=?', (payout_id,)) as c:
            status_check = await c.fetchone()
        if not status_check or status_check[0] != 'pending':
            await msg.answer(f'<b>{E["warn"]} Заявка уже обработана другим администратором.</b>')
            return
        async with conn.execute('SELECT gift_link,ton_address FROM payouts WHERE id=?', (payout_id,)) as c:
            info = await c.fetchone()
        gift_link  = info[0] if info else '—'
        ton_addr   = info[1] if info else '—'
        await conn.execute('UPDATE payouts SET status="approved", profit_amount=? WHERE id=?', (amount, payout_id))
        await conn.execute('UPDATE users SET total_profits=total_profits+? WHERE user_id=?', (amount, uid))
        await conn.commit()
        async with conn.execute('SELECT username FROM users WHERE user_id=?', (uid,)) as c:
            u = await c.fetchone()
        worker_uname = u[0] if u else 'unknown'
    pct           = u_pct if u_pct else await get_user_percentage(uid, wt)
    user_sum      = amount * pct

    report = (
        f'<b>{E["check"]} Выплата #{payout_id} одобрена!</b>\n\n'
        f'<blockquote>'
        f'<b>Код:</b> <code>{code}</code>\n'
        f'<b>Тип:</b> {wt_name}\n'
        f'<b>Сумма:</b> <code>{amount:.1f} TON</code>\n'
        f'<b>Процент:</b> {int(pct*100)}%\n'
        f'<b>{E["money"]} К выплате:</b> <code>{user_sum:.2f} TON</code>'
        f'</blockquote>\n\n'
        f'<b>NFT:</b>\n<blockquote>{gift_link}</blockquote>\n'
        f'<b>Адрес:</b>\n<blockquote><code>{ton_addr}</code></blockquote>'
    )

    try:
        await bot.edit_message_text(report, chat_id=admin_id, message_id=origin_id)
    except Exception:
        await bot.send_message(admin_id, report)

    # Уведомление другим админам
    try:
        approver_entity = await bot.get_chat(admin_id)
        approver_uname = approver_entity.username or str(admin_id)
    except Exception:
        approver_uname = str(admin_id)

    for other in await get_admins():
        if other == admin_id: continue
        try:
            await bot.send_message(other,
                f'<b>{E["ok"]} Выплата #{payout_id} одобрена!</b>\n\n'
                f'<blockquote>'
                f'<b>{E["user2"]} Одобрил:</b> @{approver_uname} (<code>{admin_id}</code>)\n'
                f'<b>{E["user"]} Воркер:</b> @{worker_uname} (<code>{uid}</code>)\n'
                f'<b>{E["phone"]} Код:</b> <code>{code}</code>\n'
                f'<b>Тип:</b> {wt_name}\n'
                f'<b>Сумма:</b> <code>{amount:.1f} TON</code>\n'
                f'<b>{E["money"]} Воркеру:</b> <code>{user_sum:.2f} TON</code>'
                f'</blockquote>')
        except Exception: pass

    # Воркеру
    payout_msg = (
        f'<b>{E["check"]} ВЫПЛАТА! ({code})</b>\n\n'
        f'<blockquote>'
        f'<b>Тип:</b> {wt_name}\n'
        f'<b>Воркер:</b> @{worker_uname} • <code>{uid}</code>\n'
        f'<b>{E["money"]} К получению:</b> <code>{user_sum:.2f} TON</code>'
        f'</blockquote>\n\n'
        f'<b>Подарки:</b>\n<blockquote>{gift_link}</blockquote>\n'
        f'<b>Адрес:</b>\n<blockquote><code>{ton_addr}</code></blockquote>'
    )
    try:
        video = await download_video()
        if video and os.path.exists(video):
            from aiogram.types import FSInputFile
            await bot.send_video(uid, FSInputFile(video), caption=payout_msg)
        else:
            await bot.send_message(uid, payout_msg)
    except Exception as ex:
        print(f'Ошибка отправки воркеру: {ex}')

    # В чат выплат
    if PAYOUT_CHAT_ID and PAYOUT_TOPIC_ID:
        try:
            profit_msg = (
                f'<b>{E["check"]} ВЫПЛАТА! ({code})</b>\n\n'
                f'<blockquote>'
                f'<b>Тип:</b> {wt_name}\n'
                f'<b>Воркер:</b> @{worker_uname}\n'
                f'<b>{E["diamond"]} Выплачено:</b> <code>{user_sum:.2f} TON</code>'
                f'</blockquote>\n\n'
                f'<b>Подарки:</b>\n<blockquote>{gift_link}</blockquote>'
            )
            video = await download_video()
            if video and os.path.exists(video):
                from aiogram.types import FSInputFile
                await bot.send_video(PAYOUT_CHAT_ID, FSInputFile(video),
                    caption=profit_msg, message_thread_id=PAYOUT_TOPIC_ID)
            else:
                await bot.send_message(PAYOUT_CHAT_ID, profit_msg,
                    message_thread_id=PAYOUT_TOPIC_ID)
        except Exception as ex:
            print(f'Ошибка отправки в топик: {ex}')

# ── Отклонение выплаты ────────────────────────────────────────────────────────
# Словарь для хранения контекста отклонения: admin_id -> dict
reject_ctx: dict = {}

@router.callback_query(F.data.startswith('reject_payout_'))
async def cb_reject_payout(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав администратора')
        return
    payout_id = int(cb.data.split('_')[-1])
    async with db_connect() as conn:
        async with conn.execute('SELECT user_id,status,payout_code,work_type,gift_link,ton_address FROM payouts WHERE id=?', (payout_id,)) as c:
            row = await c.fetchone()
    if not row: await cb.answer('Заявка не найдена'); return
    uid, status, code, wt, gift_link, ton_addr = row
    if status != 'pending': await cb.answer('Заявка уже обработана'); return

    # Сохраняем контекст и переходим в стейт ввода причины
    reject_ctx[cb.from_user.id] = {
        'payout_id': payout_id,
        'payout_code': code,
        'work_type': wt,
        'payout_user_id': uid,
        'gift_link': gift_link,
        'ton_addr': ton_addr,
        'origin_msg_id': cb.message.message_id,
        'origin_chat_id': cb.message.chat.id,
    }
    await state.set_state(AdminAction.waiting_reject_reason)

    reason_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton.model_construct(
            text='Без причины',
            callback_data=f'reject_no_reason_{payout_id}',
            icon_custom_emoji_id='5893163582194978381',
            style='danger')
    ]])
    prompt = await bot.send_message(
        cb.from_user.id,
        f'<b>{E["pencil"]} Укажите причину отклонения заявки <code>{code}</code>:</b>\n\n'
        f'<i>Или нажмите кнопку ниже, чтобы отклонить без причины.</i>',
        reply_markup=reason_kb)
    reject_ctx[cb.from_user.id]['prompt_msg_id'] = prompt.message_id
    await cb.answer()


async def do_reject_payout(admin_id: int, reason: str, state: FSMContext):
    """Общая логика отклонения выплаты."""
    if admin_id not in reject_ctx:
        await state.clear()
        return
    ctx        = reject_ctx.pop(admin_id)
    payout_id  = ctx['payout_id']
    code       = ctx['payout_code']
    wt         = ctx['work_type']
    uid        = ctx['payout_user_id']
    gift_link  = ctx['gift_link']
    ton_addr   = ctx['ton_addr']
    origin_id  = ctx['origin_msg_id']
    origin_cid = ctx['origin_chat_id']
    prompt_id  = ctx.get('prompt_msg_id')
    await state.clear()

    async with db_connect() as conn:
        async with conn.execute('SELECT status FROM payouts WHERE id=?', (payout_id,)) as c:
            row = await c.fetchone()
        if not row or row[0] != 'pending':
            return
        async with conn.execute('SELECT username FROM users WHERE user_id=?', (uid,)) as c:
            u = await c.fetchone()
        worker_uname = u[0] if u else str(uid)
        await conn.execute('UPDATE payouts SET status="rejected" WHERE id=?', (payout_id,))
        await conn.commit()
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
    reason_line = f'\n<b>{E["warn"]} Причина:</b> {reason}' if reason else ''

    # Получаем данные отклонившего админа
    try:
        admin_entity = await bot.get_chat(admin_id)
        admin_uname = admin_entity.username or str(admin_id)
    except Exception:
        admin_uname = str(admin_id)

    # Удаляем prompt-сообщение
    if prompt_id:
        try: await bot.delete_message(admin_id, prompt_id)
        except: pass

    # Обновляем оригинальное сообщение с заявкой
    rejected_text = (
        f'<b>{E["no2"]} Заявка #{payout_id} отклонена</b>\n\n'
        f'<blockquote>'
        f'<b>{E["phone"]} Код:</b> <code>{code}</code>\n'
        f'<b>Тип:</b> {wt_name}\n'
        f'<b>{E["user"]} Воркер:</b> @{worker_uname} (<code>{uid}</code>)\n'
        f'<b>{E["user2"]} Отклонил:</b> @{admin_uname}'
        f'{reason_line}'
        f'</blockquote>\n\n'
        f'<b>NFT:</b>\n<blockquote>{gift_link or "—"}</blockquote>\n'
        f'<b>Адрес:</b>\n<blockquote><code>{ton_addr or "—"}</code></blockquote>'
    )
    try:
        await bot.edit_message_text(rejected_text, chat_id=origin_cid, message_id=origin_id)
    except Exception:
        await bot.send_message(admin_id, rejected_text)

    # Уведомление другим админам
    for other in await get_admins():
        if other == admin_id: continue
        try:
            await bot.send_message(other,
                f'<b>{E["no2"]} Заявка #{payout_id} отклонена</b>\n\n'
                f'<blockquote>'
                f'<b>{E["user2"]} Отклонил:</b> @{admin_uname} (<code>{admin_id}</code>)\n'
                f'<b>{E["user"]} Воркер:</b> @{worker_uname} (<code>{uid}</code>)\n'
                f'<b>{E["phone"]} Код:</b> <code>{code}</code>\n'
                f'<b>Тип:</b> {wt_name}'
                f'{reason_line}'
                f'</blockquote>')
        except Exception: pass

    # Уведомление воркеру
    try:
        await bot.send_message(uid,
            f'<b>{E["no2"]} Заявка отклонена</b>\n\n'
            f'<blockquote>'
            f'<b>{E["phone"]} Код:</b> <code>{code}</code>\n'
            f'<b>Тип:</b> {wt_name}'
            f'{reason_line}'
            f'</blockquote>\n\n'
            f'{E["warn"]} Обратитесь к администратору за уточнениями.')
    except Exception: pass


@router.callback_query(F.data.startswith('reject_no_reason_'))
async def cb_reject_no_reason(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    await do_reject_payout(cb.from_user.id, '', state)
    await cb.answer('Заявка отклонена ❌')


@router.message(AdminAction.waiting_reject_reason)
async def admin_enter_reject_reason(msg: Message, state: FSMContext):
    try: await msg.delete()
    except: pass
    reason = (msg.text or '').strip()
    await do_reject_payout(msg.from_user.id, reason, state)

# ── Админ: заявки пользователя ────────────────────────────────────────────────
# callback: admin_user_payouts_{uid}_{status_filter}_{page}
# status_filter: all | pending | approved | rejected

ADMIN_PAYOUTS_PER_PAGE = 8

def admin_user_payouts_filter_kb(uid: int, current_filter: str, page: int) -> InlineKeyboardMarkup:
    filters = [
        ('all',      'Все',         5188217332748527444),
        ('pending',  'Ожидает',     5902050947567194830),
        ('approved', 'Одобрено',    5895514131896733546),
        ('rejected', 'Отклонено',   5893163582194978381),
    ]
    rows = []
    row = []
    for f, label, eid in filters:
        is_active = (f == current_filter)
        style_kw = {'style': 'success'} if is_active else {}
        row.append(InlineKeyboardButton.model_construct(
            text=label,
            callback_data=f'admin_user_payouts_{uid}_{f}_0',
            icon_custom_emoji_id=str(eid),
            **style_kw))
    rows.append(row)
    return rows

def admin_user_payouts_kb(uid: int, payouts: list, status_filter: str,
                           page: int, total: int) -> InlineKeyboardMarkup:
    rows = admin_user_payouts_filter_kb(uid, status_filter, page)
    for p in payouts:
        pid, code, wt, status, created_at, profit = p
        wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
        label = f'{code}  •  {wt_name}'
        icon_id = STATUS_ICON_IDS.get(status, 5188217332748527444)
        rows.append([InlineKeyboardButton.model_construct(
            text=label,
            callback_data=f'admin_payout_view_{pid}_{uid}_{status_filter}_{page}',
            icon_custom_emoji_id=str(icon_id))])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton.model_construct(
            text='Назад', callback_data=f'admin_user_payouts_{uid}_{status_filter}_{page-1}',
            icon_custom_emoji_id='5960671702059848143'))
    total_pages = max(1, (total + ADMIN_PAYOUTS_PER_PAGE - 1) // ADMIN_PAYOUTS_PER_PAGE)
    if total_pages > 1:
        nav.append(InlineKeyboardButton.model_construct(
            text=f'{page+1}/{total_pages}', callback_data='noop'))
    if (page + 1) * ADMIN_PAYOUTS_PER_PAGE < total:
        nav.append(InlineKeyboardButton.model_construct(
            text='Вперёд', callback_data=f'admin_user_payouts_{uid}_{status_filter}_{page+1}',
            icon_custom_emoji_id='6037622221625626773'))
    if nav:
        rows.append(nav)
    rows.append([btn('К профилю', f'user_detail_{uid}', 5879770735999717115),
                 btn('Настройки', 'admin_settings', 5893161718179173515)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_payout_action_kb(payout_id: int, status: str,
                            uid: int, status_filter: str, page: int) -> InlineKeyboardMarkup:
    rows = []
    if status == 'pending':
        rows.append([
            InlineKeyboardButton.model_construct(
                text='Принять', callback_data=f'approve_payout_{payout_id}',
                icon_custom_emoji_id='5895514131896733546', style='success'),
            InlineKeyboardButton.model_construct(
                text='Отклонить', callback_data=f'reject_payout_{payout_id}',
                icon_custom_emoji_id='5893163582194978381', style='danger'),
        ])
    rows.append([btn('К заявкам', f'admin_user_payouts_{uid}_{status_filter}_{page}', 5960671702059848143)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.callback_query(F.data.startswith('admin_user_payouts_'))
async def cb_admin_user_payouts(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    parts = cb.data.split('_')
    # admin_user_payouts_{uid}_{status_filter}_{page}
    uid = int(parts[3])
    status_filter = parts[4]
    page = int(parts[5])
    per_page = ADMIN_PAYOUTS_PER_PAGE
    offset = page * per_page

    where = 'user_id=?'
    params_count = [uid]
    params_list  = [uid, per_page, offset]
    if status_filter != 'all':
        where += ' AND status=?'
        params_count = [uid, status_filter]
        params_list  = [uid, status_filter, per_page, offset]

    async with db_connect() as conn:
        async with conn.execute(f'SELECT COUNT(*) FROM payouts WHERE {where}', params_count) as c:
            total = (await c.fetchone())[0]
        async with conn.execute(
            f'SELECT id,payout_code,work_type,status,created_at,profit_amount FROM payouts WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?',
            params_list) as c:
            payouts = await c.fetchall()
        async with conn.execute('SELECT nickname,username FROM users WHERE user_id=?', (uid,)) as c:
            urow = await c.fetchone()
    nick = (urow[0] if urow else None) or (urow[1] if urow else str(uid))
    status_labels = {'all': 'Все', 'pending': 'Ожидает', 'approved': 'Одобрено', 'rejected': 'Отклонено'}
    total_pages = max(1, (total + per_page - 1) // per_page)
    text = (
        f'<b>{E["cash"]} Заявки: {nick}</b>\n'
        f'<blockquote>Фильтр: <b>{status_labels.get(status_filter, status_filter)}</b> · Всего: <b>{total}</b> · Стр. {page+1}/{total_pages}</blockquote>'
    )
    if not payouts:
        text += '\n\n<i>Заявок не найдено.</i>'
    try:
        await cb.message.edit_text(text, reply_markup=admin_user_payouts_kb(uid, payouts, status_filter, page, total))
    except Exception:
        pass
    await cb.answer()

@router.callback_query(F.data.startswith('admin_payout_view_'))
async def cb_admin_payout_view(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    # admin_payout_view_{payout_id}_{uid}_{status_filter}_{page}
    parts = cb.data.split('_')
    payout_id    = int(parts[3])
    uid          = int(parts[4])
    status_filter = parts[5]
    page         = int(parts[6])

    async with db_connect() as conn:
        async with conn.execute(
            'SELECT id,payout_code,work_type,status,created_at,profit_amount,gift_link,deal_code,ton_address,user_percentage,user_id FROM payouts WHERE id=?',
            (payout_id,)) as c:
            row = await c.fetchone()
        if not row:
            await cb.answer('Заявка не найдена')
            return
        pid, code, wt, status, created_at, profit, gift_link, deal_code, ton_addr, user_pct, p_uid = row
        async with conn.execute('SELECT username,nickname FROM users WHERE user_id=?', (p_uid,)) as c:
            urow = await c.fetchone()
    worker_uname = (urow[0] if urow else None) or str(p_uid)
    worker_nick  = (urow[1] if urow else None) or worker_uname
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
    sem = {'pending': e(5902050947567194830,"⏳"), 'approved': e(5895514131896733546,"✅"), 'rejected': e(5893163582194978381,"❌")}.get(status, '❓')
    slabel = {'pending': 'На рассмотрении', 'approved': 'Выплачено', 'rejected': 'Отклонено'}.get(status, status)
    deal_line  = f'\n<b>Код сделки:</b> <code>{deal_code}</code>' if deal_code else ''
    profit_line = f'\n<b>{E["money"]} Сумма:</b> <code>{profit:.2f} TON</code>' if status == 'approved' and profit else ''
    pct_line   = f'\n<b>Процент:</b> {int((user_pct or 0)*100)}%' if user_pct else ''
    gift_lines = [l.strip() for l in (gift_link or '').splitlines() if l.strip()]
    gift_block = ('\n\n<b>' + E["gift"] + ' Подарки:</b>\n<blockquote>' + '\n'.join(gift_lines) + '</blockquote>') if gift_lines else ''
    ton_block  = f'\n\n<b>TON адрес:</b>\n<blockquote><code>{ton_addr}</code></blockquote>' if ton_addr else ''

    text = (
        f'<b>{E["cash"]} Заявка {code}</b>\n\n'
        f'<blockquote>'
        f'<b>Статус:</b> {sem} {slabel}\n'
        f'<b>Тип:</b> {wt_name}'
        f'{deal_line}'
        f'\n<b>{E["user"]} Воркер:</b> @{worker_uname} · <code>{p_uid}</code>'
        f'\n<b>{E["cal"]} Дата:</b> {created_at or "—"}'
        f'{pct_line}'
        f'{profit_line}'
        f'</blockquote>'
        f'{gift_block}'
        f'{ton_block}'
    )

    # Сначала шлём скриншоты отдельными сообщениями
    # Достаём media_files из БД — они не хранятся в БД напрямую,
    # поэтому ищем по payout_id в payout_media_store
    media_list = payout_media_store.get(payout_id)
    if media_list is None:
        # Восстанавливаем из БД (актуально после рестарта)
        async with db_connect() as conn:
            async with conn.execute('SELECT media_type, file_id FROM payout_media WHERE payout_id=?', (payout_id,)) as c:
                rows = await c.fetchall()
        media_list = [(r[0], r[1]) for r in rows]
        if media_list:
            payout_media_store[payout_id] = media_list
    if media_list:
        from aiogram.types import InputMediaPhoto, InputMediaDocument
        media_group = []
        for ftype, fid in media_list:
            if ftype == 'photo':
                media_group.append(InputMediaPhoto(media=fid))
            else:
                media_group.append(InputMediaDocument(media=fid))
        try:
            await bot.send_media_group(cb.from_user.id, media_group)
        except Exception:
            pass

    try:
        await cb.message.edit_text(text, reply_markup=admin_payout_action_kb(payout_id, status, uid, status_filter, page))
    except Exception:
        pass
    await cb.answer()

# ── Админ: поиск по коду выплаты ──────────────────────────────────────────────
@router.callback_query(F.data == 'search_payout_code_btn')
async def cb_search_payout_code_btn(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    await state.set_state(AdminAction.search_payout_code)
    await cb.message.edit_text(
        f'<b>{E["search"]} Поиск по коду выплаты</b>\n\n'
        f'<blockquote>Введите код выплаты, например: <code>#abc123xyz</code></blockquote>',
        reply_markup=kb([btn('Отмена', 'admin_settings', 5893163582194978381, 'danger')]))
    await cb.answer()

@router.message(AdminAction.search_payout_code)
async def admin_search_payout_code(msg: Message, state: FSMContext):
    await state.clear()
    code = (msg.text or '').strip()
    if not code.startswith('#'):
        code = '#' + code
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT id,payout_code,work_type,status,created_at,profit_amount,gift_link,deal_code,ton_address,user_percentage,user_id FROM payouts WHERE payout_code=?',
            (code,)) as c:
            row = await c.fetchone()
    if not row:
        await msg.answer(
            f'<b>{E["no"]} Заявка с кодом <code>{code}</code> не найдена.</b>',
            reply_markup=kb([btn('Назад', 'admin_settings', 5960671702059848143)]))
        return
    pid, code, wt, status, created_at, profit, gift_link, deal_code, ton_addr, user_pct, p_uid = row
    async with db_connect() as conn:
        async with conn.execute('SELECT username,nickname FROM users WHERE user_id=?', (p_uid,)) as c:
            urow = await c.fetchone()
    worker_uname = (urow[0] if urow else None) or str(p_uid)
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
    sem = {'pending': e(5902050947567194830,"⏳"), 'approved': e(5895514131896733546,"✅"), 'rejected': e(5893163582194978381,"❌")}.get(status, '❓')
    slabel = {'pending': 'На рассмотрении', 'approved': 'Выплачено', 'rejected': 'Отклонено'}.get(status, status)
    deal_line  = f'\n<b>Код сделки:</b> <code>{deal_code}</code>' if deal_code else ''
    profit_line = f'\n<b>{E["money"]} Сумма:</b> <code>{profit:.2f} TON</code>' if status == 'approved' and profit else ''
    pct_line   = f'\n<b>Процент:</b> {int((user_pct or 0)*100)}%' if user_pct else ''
    gift_lines = [l.strip() for l in (gift_link or '').splitlines() if l.strip()]
    gift_block = ('\n\n<b>' + E["gift"] + ' Подарки:</b>\n<blockquote>' + '\n'.join(gift_lines) + '</blockquote>') if gift_lines else ''
    ton_block  = f'\n\n<b>TON адрес:</b>\n<blockquote><code>{ton_addr}</code></blockquote>' if ton_addr else ''

    text = (
        f'<b>{E["cash"]} Заявка {code}</b>\n\n'
        f'<blockquote>'
        f'<b>Статус:</b> {sem} {slabel}\n'
        f'<b>Тип:</b> {wt_name}'
        f'{deal_line}'
        f'\n<b>{E["user"]} Воркер:</b> @{worker_uname} · <code>{p_uid}</code>'
        f'\n<b>{E["cal"]} Дата:</b> {created_at or "—"}'
        f'{pct_line}'
        f'{profit_line}'
        f'</blockquote>'
        f'{gift_block}'
        f'{ton_block}'
    )

    # Скрины — берём из кэша или из БД
    media_list = payout_media_store.get(pid)
    if media_list is None:
        async with db_connect() as conn:
            async with conn.execute('SELECT media_type, file_id FROM payout_media WHERE payout_id=?', (pid,)) as c:
                rows = await c.fetchall()
        media_list = [(r[0], r[1]) for r in rows]
        if media_list:
            payout_media_store[pid] = media_list
    if media_list:
        from aiogram.types import InputMediaPhoto, InputMediaDocument
        media_group = []
        for ftype, fid in media_list:
            if ftype == 'photo':
                media_group.append(InputMediaPhoto(media=fid))
            else:
                media_group.append(InputMediaDocument(media=fid))
        try:
            await bot.send_media_group(msg.from_user.id, media_group)
        except Exception:
            pass

    action_kb = InlineKeyboardMarkup(inline_keyboard=(
        [[InlineKeyboardButton.model_construct(
            text='Принять', callback_data=f'approve_payout_{pid}',
            icon_custom_emoji_id='5895514131896733546', style='success'),
          InlineKeyboardButton.model_construct(
            text='Отклонить', callback_data=f'reject_payout_{pid}',
            icon_custom_emoji_id='5893163582194978381', style='danger')],
         [btn('Назад в настройки', 'admin_settings', 5960671702059848143)]]
        if status == 'pending' else
        [[btn('Назад в настройки', 'admin_settings', 5960671702059848143)]])
    )
    await msg.answer(text, reply_markup=action_kb)

# ── Панель администратора ─────────────────────────────────────────────────────
@router.callback_query(F.data == 'admin_settings')
async def cb_admin_settings(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав администратора')
        return
    await cb.message.edit_text(
        f'<b>{E["gear"]} Панель администратора</b>\n\n'
        f'<blockquote>{E["bolt"]} Управление пользователями, процентами и настройками</blockquote>',
        reply_markup=admin_settings_kb())
    await cb.answer()

# Управление ворками
@router.callback_query(F.data == 'manage_work_types')
async def cb_manage_work_types(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    types = await get_all_work_types()
    rows  = []
    for wt in types:
        icon = 'ВКЛ' if wt['enabled'] else 'ВЫКЛ'
        eid  = '5895514131896733546' if wt['enabled'] else '5893163582194978381'
        rows.append([InlineKeyboardButton.model_construct(
            text=f'{wt["name"]} — {icon}',
            callback_data=f'toggle_work_{wt["type"]}',
            icon_custom_emoji_id=eid)])
    rows.append([btn('Назад', 'admin_settings', 5960671702059848143)])
    await cb.message.edit_text(
        f'<b>{E["gear"]} Управление способами ворка</b>\n\n'
        f'<blockquote>Нажмите на тип, чтобы включить или отключить его.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith('toggle_work_'))
async def cb_toggle_work(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    wt   = cb.data.split('_', 2)[2]
    new  = await toggle_work_type(wt)
    name = WORK_TYPES.get(wt, {}).get('name', wt)
    await cb.answer(f'{name} {"включён" if new else "отключён"}')
    # обновляем список
    types = await get_all_work_types()
    rows  = []
    for w in types:
        icon  = 'ВКЛ' if w['enabled'] else 'ВЫКЛ'
        eid   = '5895514131896733546' if w['enabled'] else '5893163582194978381'
        rows.append([InlineKeyboardButton.model_construct(text=f'{w["name"]} — {icon}', callback_data=f'toggle_work_{w["type"]}', icon_custom_emoji_id=eid)])
    rows.append([btn('Назад', 'admin_settings', 5960671702059848143)])
    await cb.message.edit_text(
        f'<b>{E["gear"]} Управление способами ворка</b>\n\n'
        f'<blockquote>Нажмите на тип, чтобы включить или отключить его.</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

# Поиск пользователя
@router.callback_query(F.data == 'search_user_btn')
async def cb_search_user_btn(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    await state.set_state(AdminAction.search_user)
    await cb.message.edit_text(
        f'<b>{E["search"]} Поиск пользователя</b>\n\n'
        f'<blockquote>Введите <b>@username</b> или <b>ID</b>:</blockquote>',
        reply_markup=kb([btn('Отмена', 'admin_settings', 5893163582194978381, 'danger')]))
    await cb.answer()

@router.message(AdminAction.search_user)
async def admin_search_user(msg: Message, state: FSMContext):
    await state.clear()
    row = await find_user(msg.text or '')
    if not row:
        await msg.answer(f'<b>{E["no"]} Пользователь не найден.</b>',
                         reply_markup=kb([btn('Назад', 'admin_settings', 5960671702059848143)]))
        return
    uid, nick, uname, role, profits, join_date, approved = row
    status = f'{E["check"]} Одобрен' if approved else f'{E["hourglass"]} Не одобрен'
    await msg.answer(
        format_profile(uid, nick or '—', uname or '—', role, profits, join_date) +
        f'\n<b>Статус:</b> {status}',
        reply_markup=kb(
            [btn('Добавить профит', f'add_profit_{uid}', 5244837092042750681, 'success'),
             btn('Удалить профит', f'remove_profit_{uid}', 5246762912428603768, 'danger')],
            [btn('Изменить процент', f'edit_user_percent_{uid}', 5231200819986047254)],
            [btn('Заявки пользователя', f'admin_user_payouts_{uid}_all_0', 5201691993775818138)],
            [btn('Назад в настройки', 'admin_settings', 5960671702059848143)]))

# Добавить/убрать admin
@router.callback_query(F.data == 'add_admin_btn')
async def cb_add_admin_btn(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in OWNERS:
        await cb.answer('Только владельцы')
        return
    await state.set_state(AdminAction.add_admin)
    await cb.message.edit_text(
        f'<b>{E["crown"]} Добавить администратора</b>\n\n'
        f'<blockquote>Введите <b>@username</b> или <b>ID</b>:</blockquote>',
        reply_markup=kb([btn('Отмена', 'admin_settings', 5893163582194978381, 'danger')]))
    await cb.answer()

@router.message(AdminAction.add_admin)
async def admin_add_admin(msg: Message, state: FSMContext):
    await state.clear()
    query = (msg.text or '').strip().lstrip('@')
    try:
        try:
            uid = int(query)
            chat = await bot.get_chat(uid)
        except ValueError:
            chat = await bot.get_chat(f'@{query}')
            uid  = chat.id
        uname = chat.username or str(uid)
        name  = chat.full_name or 'Без имени'
        async with db_connect() as conn:
            await conn.execute(
                'INSERT OR IGNORE INTO users (user_id,username,nickname,role,join_date,approved) VALUES (?,?,?,"admin",?,1)',
                (uid, uname, name, datetime.now().strftime('%Y-%m-%d')))
            await conn.execute('UPDATE users SET role="admin", approved=1 WHERE user_id=?', (uid,))
            await conn.commit()
        await msg.answer(f'<b>{E["check"]} @{uname} назначен администратором!</b>')
        try:
            await bot.send_message(uid,
                f'<b>{E["party"]} Вы назначены администратором!</b>',
                reply_markup=main_menu(True))
        except Exception: pass
    except Exception as ex:
        await msg.answer(f'<b>{E["no"]} Ошибка: {ex}</b>')

@router.callback_query(F.data == 'remove_admin_btn')
async def cb_remove_admin_btn(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in OWNERS:
        await cb.answer('Только владельцы')
        return
    await state.set_state(AdminAction.remove_admin)
    await cb.message.edit_text(
        f'<b>{E["ban"]} Снять администратора</b>\n\n'
        f'<blockquote>Введите <b>@username</b> или <b>ID</b>:</blockquote>',
        reply_markup=kb([btn('Отмена', 'admin_settings', 5893163582194978381, 'danger')]))
    await cb.answer()

@router.message(AdminAction.remove_admin)
async def admin_remove_admin(msg: Message, state: FSMContext):
    await state.clear()
    query = (msg.text or '').strip().lstrip('@')
    try:
        try:
            uid = int(query)
            chat = await bot.get_chat(uid)
        except ValueError:
            chat = await bot.get_chat(f'@{query}')
            uid  = chat.id
        uname = chat.username or str(uid)
        async with db_connect() as conn:
            await conn.execute('UPDATE users SET role="worker" WHERE user_id=?', (uid,))
            await conn.commit()
        await msg.answer(f'<b>{E["check"]} @{uname} снят с должности администратора.</b>')
    except Exception as ex:
        await msg.answer(f'<b>{E["no"]} Ошибка: {ex}</b>')

# Список пользователей
@router.callback_query(F.data.startswith('user_list_'))
async def cb_user_list(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    page   = int(cb.data.split('_')[2])
    limit  = 10
    offset = page * limit
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT user_id,nickname,username FROM users WHERE approved=1 ORDER BY nickname LIMIT ? OFFSET ?',
            (limit, offset)) as c:
            users = await c.fetchall()
        async with conn.execute('SELECT COUNT(*) FROM users WHERE approved=1') as c:
            total = (await c.fetchone())[0]
    if not users:
        await cb.answer('Нет одобренных пользователей')
        return
    rows = []
    for uid, nick, uname in users:
        display = nick or (f'@{uname}' if uname else f'ID: {uid}')
        rows.append([InlineKeyboardButton.model_construct(text=display, callback_data=f'user_detail_{uid}', icon_custom_emoji_id='5879770735999717115')])
    nav = []
    if page > 0: nav.append(InlineKeyboardButton.model_construct(text='«', callback_data=f'user_list_{page-1}', icon_custom_emoji_id='5960671702059848143'))
    if offset + limit < total: nav.append(InlineKeyboardButton.model_construct(text='»', callback_data=f'user_list_{page+1}', icon_custom_emoji_id='5893450623449305489'))
    if nav: rows.append(nav)
    rows.append([btn('Назад в настройки', 'admin_settings', 5960671702059848143)])
    await cb.message.edit_text(
        f'<b>{E["users"]} Список пользователей</b>\n'
        f'<blockquote>Страница {page+1} · Всего: {total}</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith('user_detail_'))
async def cb_user_detail(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer('Нет прав')
        return
    uid = int(cb.data.split('_')[2])
    async with db_connect() as conn:
        async with conn.execute(
            'SELECT nickname,username,role,total_profits,join_date FROM users WHERE user_id=?', (uid,)) as c:
            user = await c.fetchone()
    if not user: await cb.answer('Пользователь не найден'); return
    nick, uname, role, profits, join_date = user
    types = await get_all_work_types()
    pct_lines = []
    for wt in types:
        p = await get_user_percentage(uid, wt['type'])
        pct_lines.append(f'<b>{wt["name"]}:</b> {int(p*100)}%')
    text = format_profile(uid, nick or 'Без имени', uname or '—', role, profits, join_date)
    text += f'\n\n<b>{E["chart"]} Проценты:</b>\n' + '\n'.join(pct_lines)
    await cb.message.edit_text(
        text,
        reply_markup=kb(
            [btn('Добавить профит', f'add_profit_{uid}', 5244837092042750681, 'success'),
             btn('Удалить профит', f'remove_profit_{uid}', 5246762912428603768, 'danger')],
            [btn('Изменить процент', f'edit_user_percent_{uid}', 5893161718179173515)],
            [btn('Заявки пользователя', f'admin_user_payouts_{uid}_all_0', 5201691993775818138)],
            [btn('К списку', 'user_list_0', 5960671702059848143)]))
    await cb.answer()

# Добавить/удалить профит (кнопки)
@router.callback_query(F.data.startswith('add_profit_'))
async def cb_add_profit(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id): await cb.answer('Нет прав'); return
    uid = int(cb.data.split('_')[2])
    await state.set_state(AdminAction.waiting_profit_add)
    await state.update_data(target_uid=uid)
    await cb.message.edit_text(
        f'<b>{E["chartup"]} Добавить профит</b>\n\n<blockquote>Введите сумму в TON:</blockquote>')
    await cb.answer()

@router.callback_query(F.data.startswith('remove_profit_'))
async def cb_remove_profit(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id): await cb.answer('Нет прав'); return
    uid = int(cb.data.split('_')[2])
    await state.set_state(AdminAction.waiting_profit_remove)
    await state.update_data(target_uid=uid)
    await cb.message.edit_text(
        f'<b>{E["chartdn"]} Удалить профит</b>\n\n<blockquote>Введите сумму в TON:</blockquote>')
    await cb.answer()

@router.message(AdminAction.waiting_profit_add)
async def admin_profit_add(msg: Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(',', '.'))
        if amount <= 0: raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Введите положительное число.</b>')
        return
    data = await state.get_data()
    uid  = data['target_uid']
    await state.clear()
    async with db_connect() as conn:
        async with conn.execute('SELECT total_profits,username FROM users WHERE user_id=?', (uid,)) as c:
            row = await c.fetchone()
        if not row: await msg.answer(f'<b>{E["no"]} Пользователь не найден.</b>'); return
        new_total = row[0] + amount
        uname = row[1] or str(uid)
        await conn.execute('UPDATE users SET total_profits=? WHERE user_id=?', (new_total, uid))
        await conn.commit()
    await msg.answer(
        f'<b>{E["check"]} @{uname}:</b> +{amount:.2f} TON\n'
        f'<b>Текущий профит:</b> <code>{new_total:.2f} TON</code>')

@router.message(AdminAction.waiting_profit_remove)
async def admin_profit_remove(msg: Message, state: FSMContext):
    try:
        amount = float(msg.text.replace(',', '.'))
        if amount <= 0: raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Введите положительное число.</b>')
        return
    data = await state.get_data()
    uid  = data['target_uid']
    await state.clear()
    async with db_connect() as conn:
        async with conn.execute('SELECT total_profits,username FROM users WHERE user_id=?', (uid,)) as c:
            row = await c.fetchone()
        if not row: await msg.answer(f'<b>{E["no"]} Пользователь не найден.</b>'); return
        new_total = max(0.0, row[0] - amount)
        uname = row[1] or str(uid)
        await conn.execute('UPDATE users SET total_profits=? WHERE user_id=?', (new_total, uid))
        await conn.commit()
    await msg.answer(
        f'<b>{E["check"]} @{uname}:</b> −{amount:.2f} TON\n'
        f'<b>Текущий профит:</b> <code>{new_total:.2f} TON</code>')

# Изменить процент воркера
@router.callback_query(F.data.startswith('edit_user_percent_'))
async def cb_edit_user_percent(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id): await cb.answer('Нет прав'); return
    uid   = int(cb.data.split('_')[3])
    types = await get_all_work_types()
    rows  = []
    for wt in types:
        p = await get_user_percentage(uid, wt['type'])
        rows.append([InlineKeyboardButton.model_construct(
            text=f'{wt["name"]} ({int(p*100)}%)',
            callback_data=f'set_uwp_{uid}_{wt["type"]}',
            icon_custom_emoji_id='5231200819986047254')])
    rows.append([btn('Назад', f'user_detail_{uid}', 5960671702059848143)])
    await cb.message.edit_text(
        f'<b>{E["chart"]} Изменить процент воркера</b>\n\n<blockquote>Выберите тип ворка:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith('set_uwp_'))
async def cb_set_uwp(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id): await cb.answer('Нет прав'); return
    _, _, uid_s, wt = cb.data.split('_', 3)
    uid     = int(uid_s)
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
    cur_pct = await get_user_percentage(uid, wt)
    await state.set_state(AdminAction.waiting_percent)
    await state.update_data(target_uid=uid, work_type=wt, work_type_name=wt_name)
    await cb.message.edit_text(
        f'<b>{E["chart"]} Установка процента — {wt_name}</b>\n\n'
        f'<blockquote><b>Текущий процент:</b> {int(cur_pct*100)}%</blockquote>\n\n'
        f'Введите новый процент (0–100):')
    await cb.answer()

@router.message(AdminAction.waiting_percent)
async def admin_set_percent(msg: Message, state: FSMContext):
    try:
        pct = float(msg.text.replace(',', '.'))
        if not (0 <= pct <= 100): raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Введите число от 0 до 100.</b>')
        return
    data    = await state.get_data()
    uid     = data['target_uid']
    wt      = data['work_type']
    wt_name = data['work_type_name']
    await state.clear()
    await set_user_percentage(uid, wt, pct / 100)
    async with db_connect() as conn:
        async with conn.execute('SELECT username FROM users WHERE user_id=?', (uid,)) as c:
            row = await c.fetchone()
        uname = row[0] if row else str(uid)
    await msg.answer(
        f'<b>{E["check"]} Для @{uname} установлен процент {wt_name}:</b> {pct:.0f}%')

# Изменить глобальный процент
@router.callback_query(F.data == 'edit_global_percent')
async def cb_edit_global_percent(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id): await cb.answer('Нет прав'); return
    types = await get_all_work_types()
    rows  = []
    for wt in types:
        rows.append([InlineKeyboardButton.model_construct(
            text=f'{wt["name"]} ({int(wt["percent"]*100)}%)',
            callback_data=f'set_gwp_{wt["type"]}',
            icon_custom_emoji_id='5231200819986047254')])
    rows.append([btn('Назад', 'admin_settings', 5960671702059848143)])
    await cb.message.edit_text(
        f'<b>{E["chart"]} Изменить общий процент</b>\n\n<blockquote>Выберите тип ворка:</blockquote>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@router.callback_query(F.data.startswith('set_gwp_'))
async def cb_set_gwp(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id): await cb.answer('Нет прав'); return
    wt      = cb.data.split('_', 2)[2]
    wt_name = WORK_TYPES.get(wt, {}).get('name', wt)
    cur_pct = await get_work_type_percentage(wt)
    await state.set_state(AdminAction.waiting_global_pct)
    await state.update_data(work_type=wt, work_type_name=wt_name)
    await cb.message.edit_text(
        f'<b>{E["chart"]} Общий процент — {wt_name}</b>\n\n'
        f'<blockquote><b>Текущий:</b> {int(cur_pct*100)}%</blockquote>\n\n'
        f'Введите новый процент (0–100):')
    await cb.answer()

@router.message(AdminAction.waiting_global_pct)
async def admin_set_global_pct(msg: Message, state: FSMContext):
    try:
        pct = float(msg.text.replace(',', '.'))
        if not (0 <= pct <= 100): raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Введите число от 0 до 100.</b>')
        return
    data    = await state.get_data()
    wt      = data['work_type']
    wt_name = data['work_type_name']
    await state.clear()
    await set_work_type_percentage(wt, pct / 100)
    await msg.answer(
        f'<b>{E["check"]} Общий процент {wt_name} установлен:</b> {pct:.0f}%')

# ── Команды ───────────────────────────────────────────────────────────────────
@router.message(Command('search'))
async def cmd_search(msg: Message):
    if not await is_admin(msg.from_user.id):
        await msg.answer(f'<b>{E["no"]} Нет прав администратора.</b>')
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer(f'<b>{E["pencil"]} Использование:</b> /search @username или /search 12345678')
        return
    row = await find_user(parts[1])
    if not row:
        await msg.answer(f'<b>{E["no"]} Пользователь не найден.</b>')
        return
    uid, nick, uname, role, profits, join_date, approved = row
    status = f'{E["check"]} Одобрен' if approved else f'{E["hourglass"]} Не одобрен'
    await msg.answer(
        format_profile(uid, nick or '—', uname or '—', role, profits, join_date) +
        f'\n<b>Статус:</b> {status}',
        reply_markup=kb(
            [btn('Добавить профит', f'add_profit_{uid}', 5244837092042750681, 'success'),
             btn('Удалить профит', f'remove_profit_{uid}', 5246762912428603768, 'danger')],
            [btn('Изменить процент', f'edit_user_percent_{uid}', 5231200819986047254)]))

@router.message(Command('profit'))
async def cmd_profit(msg: Message):
    if not await is_admin(msg.from_user.id):
        await msg.answer(f'<b>{E["no"]} Нет прав.</b>')
        return
    parts = msg.text.split()
    if len(parts) < 3:
        await msg.answer(f'<b>{E["pencil"]} Использование:</b> /profit @username 12.5')
        return
    query = parts[1].lstrip('@')
    try:
        amount = float(parts[2].replace(',', '.'))
        if amount <= 0: raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Укажите положительное число.</b>')
        return
    row = await find_user(query)
    if not row or not row[6]:
        await msg.answer(f'<b>{E["no"]} Пользователь не найден или не одобрен.</b>')
        return
    uid, _, uname = row[0], row[1], row[2]
    async with db_connect() as conn:
        async with conn.execute('SELECT total_profits FROM users WHERE user_id=?', (uid,)) as c:
            r = await c.fetchone()
        new_total = (r[0] if r else 0) + amount
        await conn.execute('UPDATE users SET total_profits=? WHERE user_id=?', (new_total, uid))
        await conn.commit()
    await msg.answer(
        f'<b>{E["check"]} @{uname}:</b> +{amount:.2f} TON\n'
        f'<b>Профит:</b> <code>{new_total:.2f} TON</code>')

@router.message(Command('delprofit'))
async def cmd_delprofit(msg: Message):
    if not await is_admin(msg.from_user.id):
        await msg.answer(f'<b>{E["no"]} Нет прав.</b>')
        return
    parts = msg.text.split()
    if len(parts) < 3:
        await msg.answer(f'<b>{E["pencil"]} Использование:</b> /delprofit @username 5.0')
        return
    query = parts[1].lstrip('@')
    try:
        amount = float(parts[2].replace(',', '.'))
        if amount <= 0: raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Укажите положительное число.</b>')
        return
    row = await find_user(query)
    if not row or not row[6]:
        await msg.answer(f'<b>{E["no"]} Пользователь не найден или не одобрен.</b>')
        return
    uid, _, uname = row[0], row[1], row[2]
    async with db_connect() as conn:
        async with conn.execute('SELECT total_profits FROM users WHERE user_id=?', (uid,)) as c:
            r = await c.fetchone()
        new_total = max(0.0, (r[0] if r else 0) - amount)
        await conn.execute('UPDATE users SET total_profits=? WHERE user_id=?', (new_total, uid))
        await conn.commit()
    await msg.answer(
        f'<b>{E["check"]} @{uname}:</b> −{amount:.2f} TON\n'
        f'<b>Профит:</b> <code>{new_total:.2f} TON</code>')

@router.message(Command('approve'))
async def cmd_approve(msg: Message):
    if not await is_admin(msg.from_user.id):
        await msg.answer(f'<b>{E["no"]} Нет прав.</b>')
        return
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer('Использование: /approve @username')
        return
    query = parts[1].lstrip('@')
    try:
        try: uid = int(query); chat = await bot.get_chat(uid)
        except ValueError: chat = await bot.get_chat(f'@{query}'); uid = chat.id
        uname = chat.username or str(uid)
        name  = chat.full_name or 'Без имени'
        async with db_connect() as conn:
            await conn.execute(
                'INSERT OR IGNORE INTO users (user_id,username,nickname,role,join_date,approved) VALUES (?,?,?,"worker",?,1)',
                (uid, uname, name, datetime.now().strftime('%Y-%m-%d')))
            await conn.execute('UPDATE users SET approved=1, nickname=? WHERE user_id=?', (name, uid))
            await conn.commit()
        await msg.answer(f'<b>{E["check"]} @{uname} одобрен.</b>')
        is_adm = await is_admin(uid)
        try:
            await bot.send_message(uid,
                f'<b>{E["party"]} Вы одобрены!</b>\n\nДобро пожаловать в команду!',
                reply_markup=main_menu(is_adm))
        except Exception: pass
    except Exception as ex:
        await msg.answer(f'<b>{E["no"]} Ошибка: {ex}</b>')

@router.message(Command('setadmin'))
async def cmd_setadmin(msg: Message):
    if msg.from_user.id not in OWNERS:
        await msg.answer(f'<b>{E["no"]} Только для владельцев.</b>')
        return
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer(f'<b>{E["pencil"]} Использование:</b> /setadmin @username')
        return
    query = parts[1].lstrip('@')
    try:
        try: uid = int(query); chat = await bot.get_chat(uid)
        except ValueError: chat = await bot.get_chat(f'@{query}'); uid = chat.id
        uname = chat.username or str(uid)
        name  = chat.full_name or 'Без имени'
        async with db_connect() as conn:
            await conn.execute(
                'INSERT OR IGNORE INTO users (user_id,username,nickname,role,join_date,approved) VALUES (?,?,?,"admin",?,1)',
                (uid, uname, name, datetime.now().strftime('%Y-%m-%d')))
            await conn.execute('UPDATE users SET role="admin", approved=1 WHERE user_id=?', (uid,))
            await conn.commit()
        await msg.answer(f'<b>{E["check"]} @{uname} назначен администратором.</b>')
        try:
            await bot.send_message(uid, f'<b>{E["party"]} Вы назначены администратором!</b>',
                reply_markup=main_menu(True))
        except Exception: pass
    except Exception as ex:
        await msg.answer(f'<b>{E["no"]} Ошибка: {ex}</b>')

@router.message(Command('removeadmin'))
async def cmd_removeadmin(msg: Message):
    if msg.from_user.id not in OWNERS:
        await msg.answer(f'<b>{E["no"]} Только для владельцев.</b>')
        return
    parts = msg.text.split()
    if len(parts) < 2:
        await msg.answer(f'<b>{E["pencil"]} Использование:</b> /removeadmin @username')
        return
    query = parts[1].lstrip('@')
    try:
        try: uid = int(query); chat = await bot.get_chat(uid)
        except ValueError: chat = await bot.get_chat(f'@{query}'); uid = chat.id
        uname = chat.username or str(uid)
        async with db_connect() as conn:
            await conn.execute('UPDATE users SET role="worker" WHERE user_id=?', (uid,))
            await conn.commit()
        await msg.answer(f'<b>{E["check"]} @{uname} снят с должности администратора.</b>')
    except Exception as ex:
        await msg.answer(f'<b>{E["no"]} Ошибка: {ex}</b>')

@router.message(Command('setper'))
async def cmd_setper(msg: Message):
    if not await is_admin(msg.from_user.id):
        await msg.answer(f'<b>{E["no"]} Нет прав.</b>')
        return
    parts = msg.text.split()
    if len(parts) < 3:
        await msg.answer(f'<b>{E["pencil"]} Использование:</b> /setper @username 0.65')
        return
    query = parts[1].lstrip('@')
    try:
        pct = float(parts[2].replace(',', '.'))
        if not (0 <= pct <= 1): raise ValueError
    except Exception:
        await msg.answer(f'<b>{E["no"]} Укажите число от 0 до 1 (0.7 = 70%)</b>')
        return
    row = await find_user(query)
    if not row:
        await msg.answer(f'<b>{E["no"]} Пользователь не найден.</b>')
        return
    uid   = row[0]
    uname = row[2] or str(uid)
    types = await get_all_work_types()
    async with db_connect() as conn:
        for wt in types:
            await conn.execute(
                'INSERT OR REPLACE INTO user_work_percentages (user_id,work_type,percentage) VALUES (?,?,?)',
                (uid, wt['type'], pct))
        await conn.commit()
    await msg.answer(
        f'<b>{E["check"]} @{uname}:</b> установлен процент <code>{pct*100:.0f}%</code> для всех типов.')

@router.message(Command('getper'))
async def cmd_getper(msg: Message):
    if not await is_admin(msg.from_user.id):
        await msg.answer(f'<b>{E["no"]} Нет прав.</b>')
        return
    parts = msg.text.split()
    types = await get_all_work_types()
    if len(parts) >= 2:
        query = parts[1].lstrip('@')
        row   = await find_user(query)
        if not row:
            await msg.answer(f'<b>{E["no"]} Пользователь не найден.</b>')
            return
        uid   = row[0]
        uname = row[2] or str(uid)
        lines = [f'<b>{wt["name"]}:</b> {int((await get_user_percentage(uid, wt["type"]))*100)}%' for wt in types]
        await msg.answer(
            f'<b>{E["chart"]} Проценты @{uname}:</b>\n\n<blockquote>' + '\n'.join(lines) + '</blockquote>')
    else:
        lines = [f'<b>{wt["name"]}:</b> {int(wt["percent"]*100)}%' for wt in types]
        await msg.answer(
            f'<b>{E["chart"]} Общие проценты:</b>\n\n<blockquote>' + '\n'.join(lines) + '</blockquote>')

@router.message(Command('fixdb'))
async def cmd_fixdb(msg: Message):
    if msg.from_user.id not in OWNERS:
        await msg.answer(f'<b>{E["no"]} Только для владельцев.</b>')
        return
    await msg.answer(f'<b>{E["refresh"]} Проверяю и исправляю базу данных...</b>')
    await check_and_fix_database()
    await msg.answer(f'<b>{E["check"]} База данных проверена и исправлена!</b>')

@router.message(Command('check'))
async def cmd_check(msg: Message):
    try:
        member = await bot.get_chat_member(f'@{CHANNEL_USERNAME}', bot.id)
        subscribed = member.status not in ('left', 'kicked')
    except Exception:
        subscribed = False
    if subscribed:
        await msg.answer(f'<b>{E["check"]} Бот подписан на канал и готов к работе.</b>')
    else:
        await msg.answer(
            f'<b>{E["warn"]} Telegram не позволяет ботам подписываться автоматически.</b>\n'
            f'{E["arrow"]} Добавь бота в канал @{CHANNEL_USERNAME} как администратора.')

# ── Запуск ────────────────────────────────────────────────────────────────────
async def main():
    await init_db()
    await check_and_fix_database()
    await bot.set_my_commands([
        BotCommand(command='start', description='Главное меню'),
        BotCommand(command='top',   description='Топ воркеров'),
    ])
    print('✅ Бот запущен')
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())