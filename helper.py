import json
import os
import asyncio
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# === НАСТРОЙКИ ===
BOT_TOKEN = "8664269263:AAGBj1U7zfKgyslXNmgJOTVuMbpnh-o_AJE"
ADMIN_IDS = [488616444, 8727416659]

# === ССЫЛКИ НА МАНУАЛЫ ===
MANUAL_LINK = "https://telegra.ph/Manual-po-vorku-v-Astral-Team-10-21"
NICEGRAM_LINK = "https://telegra.ph/Nicegram-vork--Manual-11-15"
DRAIN_LINK = "https://telegra.ph/Manual-po-drejneru-12-07"

# === ПУТИ К ФАЙЛАМ ===
MENTORS_FILE = "mentors.json"
GARANTS_FILE  = "garants.json"
ADMINS_FILE   = "admins.json"

# === ЭМОДЗИ ===
E_BOOK    = '<tg-emoji emoji-id="5226512880362332956">📖</tg-emoji>'
E_USERS   = '<tg-emoji emoji-id="6032609071373226027">👥</tg-emoji>'
E_GEM     = '<tg-emoji emoji-id="5836907383292436018">💎</tg-emoji>'
E_PHONE   = '<tg-emoji emoji-id="5895652322469482989">📱</tg-emoji>'
E_PENCIL  = '<tg-emoji emoji-id="5197269100878907942">✍️</tg-emoji>'
E_CROSS   = '<tg-emoji emoji-id="5774077015388852135">❌</tg-emoji>'
E_NOTE    = '<tg-emoji emoji-id="5778299625370817409">📝</tg-emoji>'
E_WARN    = '<tg-emoji emoji-id="5904692292324692386">⚠️</tg-emoji>'
E_OK      = '<tg-emoji emoji-id="5938252440926163756">✅</tg-emoji>'
E_TRASH   = '<tg-emoji emoji-id="5904542823167824187">🗑</tg-emoji>'
E_FLEUR   = '⚜️'
E_POINT   = '<tg-emoji emoji-id="5471978009449731768">👉</tg-emoji>'
E_CAMERA  = '<tg-emoji emoji-id="5881806211195605908">📸</tg-emoji>'
E_ROCKET  = '<tg-emoji emoji-id="6041705726206808304">🚀</tg-emoji>'
E_BUBBLE  = '<tg-emoji emoji-id="5904248647972820334">💭</tg-emoji>'
E_MONEY   = '<tg-emoji emoji-id="5902206159095339799">🤑</tg-emoji>'
E_WRENCH  = '<tg-emoji emoji-id="5962952497197748583">🔧</tg-emoji>'
E_CHECK   = '<tg-emoji emoji-id="5774022692642492953">✅</tg-emoji>'

# === ЧИСЛА-ЭМОДЗИ ===
NUM_EMOJI = [
    '<tg-emoji emoji-id="5794164805065514131">1⃣</tg-emoji>',
    '<tg-emoji emoji-id="5794085322400733645">2⃣</tg-emoji>',
    '<tg-emoji emoji-id="5794280000383358988">3⃣</tg-emoji>',
    '<tg-emoji emoji-id="5794241397217304511">4⃣</tg-emoji>',
    '<tg-emoji emoji-id="5793985348446984682">5⃣</tg-emoji>',
    '<tg-emoji emoji-id="5794324702402976226">6⃣</tg-emoji>',
    '<tg-emoji emoji-id="5793942849745591465">7⃣</tg-emoji>',
    '<tg-emoji emoji-id="5793926687783655907">8⃣</tg-emoji>',
    '<tg-emoji emoji-id="5793979472931723221">9⃣</tg-emoji>',
]
E_TEN = '<tg-emoji emoji-id="5794375786743995258">🔟</tg-emoji>'

def num_to_emoji(n: int) -> str:
    """1–9 кастомные, 10 — отдельный эмодзи, выше — обычный текст."""
    if 1 <= n <= 9:
        return NUM_EMOJI[n - 1]
    if n == 10:
        return E_TEN
    return str(n)

# === ЛОГИРОВАНИЕ ===
logging.basicConfig(level=logging.INFO)

# === ФУНКЦИИ ДЛЯ JSON ===
def load_json(filename: str, default):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Конвертируем старый формат (список) в словарь
            if filename == MENTORS_FILE and isinstance(data, list):
                data = {username: 5 for username in data}
                save_json(filename, data)
            return data
    return default

def save_json(filename: str, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# === ДАННЫЕ ===
mentors: dict = load_json(MENTORS_FILE, {})
garants: dict = load_json(GARANTS_FILE, {})
dynamic_admins: list = load_json(ADMINS_FILE, [])  # список ID, выданных через .админ

# === ПРОВЕРКА ПРАВ ===
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS or user_id in dynamic_admins

def is_superadmin(user_id: int) -> bool:
    """Только хардкод-админы могут выдавать/забирать права."""
    return user_id in ADMIN_IDS

# === ХЕЛПЕРЫ ===
async def reply(message: Message, text: str, disable_web_page_preview: bool = True):
    await message.reply(text, parse_mode=ParseMode.HTML, disable_web_page_preview=disable_web_page_preview)


# =========================
# === /HELP ===
# =========================
async def cmd_help(message: Message):
    text = (
        f"{E_BOOK} <b>СПРАВКА ПО КОМАНДАМ</b>\n\n"

        f"<blockquote>{E_USERS} <b>Наставники</b>\n"
        f"{E_POINT} <code>/list</code> — список наставников\n"
        f"{E_POINT} <code>/addnast @user 5</code> — добавить наставника <i>(только админ)</i>\n"
        f"{E_POINT} <code>/delnast 1</code> — удалить по номеру <i>(только админ)</i>\n"
        f"{E_POINT} <code>/delnast @user</code> — удалить по нику <i>(только админ)</i></blockquote>\n\n"

        f"<blockquote>{E_GEM} <b>Гаранты</b>\n"
        f"{E_POINT} <code>гарант</code> / <code>гаранты</code> — список гарантов\n"
        f"{E_POINT} <code>/addgarant @user 5</code> — добавить гаранта <i>(только админ)</i>\n"
        f"{E_POINT} <code>/delgarant @user</code> — удалить гаранта <i>(только админ)</i></blockquote>\n\n"

        f"<blockquote>{E_PHONE} <b>Мануалы</b>\n"
        f"{E_POINT} <code>мануал</code> — инструкция для воркеров\n"
        f"{E_POINT} <code>найсграм</code> — мануал по Nicegram\n"
        f"{E_POINT} <code>дрейн</code> / <code>дрейнер</code> — мануал по дрейн-ворку</blockquote>\n\n"

        f"<blockquote>{E_CHECK} <b>Управление админами</b> <i>(только суперадмин)</i>\n"
        f"{E_POINT} <code>.админ</code> в ответ на сообщение — выдать права\n"
        f"{E_POINT} <code>.деладмин</code> в ответ на сообщение — забрать права</blockquote>"
    )
    await reply(message, text)


# =========================
# === НАСТАВНИКИ ===
# =========================
async def cmd_list(message: Message):
    if not mentors:
        await reply(message, f"{E_NOTE} <i>Список наставников пока пуст.</i>")
        return

    rows = "\n".join(
        f"  {num_to_emoji(i)} <b>{username}</b> — {percent}%"
        for i, (username, percent) in enumerate(mentors.items(), 1)
    )
    text = f"{E_USERS} <b>Список наставников:</b>\n\n<blockquote>{rows}</blockquote>"
    await reply(message, text)


async def cmd_addnast(message: Message):
    if not is_admin(message.from_user.id):
        await reply(message, f"{E_CROSS} <b>У тебя нет прав на добавление наставников.</b>")
        return

    parts = message.text.strip().split()
    if len(parts) != 3:
        await reply(message, f"{E_PENCIL} <b>Использование:</b> <code>/addnast @user 5</code>")
        return

    username = parts[1]
    try:
        percent = int(parts[2])
    except ValueError:
        await reply(message, f"{E_WARN} Процент должен быть <b>числом</b>.")
        return

    if username in mentors:
        await reply(message, f"{E_WARN} Наставник <code>{username}</code> уже есть в списке.")
        return

    mentors[username] = percent
    save_json(MENTORS_FILE, mentors)
    await reply(message, f"{E_OK} Добавлен наставник <code>{username}</code> с процентом <b>{percent}%</b>.")


async def cmd_delnast(message: Message):
    if not is_admin(message.from_user.id):
        await reply(message, f"{E_CROSS} <b>У тебя нет прав на удаление наставников.</b>")
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await reply(message, f"{E_PENCIL} <b>Использование:</b> <code>/delnast @user</code> или <code>/delnast 1</code>")
        return

    arg = parts[1].strip()

    if arg.isdigit():
        index = int(arg) - 1
        if 0 <= index < len(mentors):
            username = list(mentors.keys())[index]
            mentors.pop(username)
            save_json(MENTORS_FILE, mentors)
            await reply(message, f"{E_TRASH} Удалён наставник: <code>{username}</code>")
        else:
            await reply(message, f"{E_WARN} Наставник с номером <b>{arg}</b> не найден.")
    elif arg in mentors:
        mentors.pop(arg)
        save_json(MENTORS_FILE, mentors)
        await reply(message, f"{E_TRASH} Удалён наставник: <code>{arg}</code>")
    else:
        await reply(message, f"{E_WARN} Наставник <code>{arg}</code> не найден.")


# =========================
# === ГАРАНТЫ ===
# =========================
async def cmd_garants(message: Message):
    if not garants:
        await reply(message, f"{E_NOTE} <i>Список гарантов пуст.</i>")
        return

    rows = "\n".join(
        f'  <tg-emoji emoji-id="5879807599704020061">👻</tg-emoji> <b>{username}</b> — {percent}%'
        for username, percent in garants.items()
    )
    text = f"{E_GEM} <b>Гаранты:</b>\n\n<blockquote>{rows}</blockquote>"
    await reply(message, text)


async def cmd_addgarant(message: Message):
    if not is_admin(message.from_user.id):
        await reply(message, f"{E_CROSS} <b>У тебя нет прав на добавление гарантов.</b>")
        return

    parts = message.text.strip().split()
    if len(parts) != 3:
        await reply(message, f"{E_PENCIL} <b>Использование:</b> <code>/addgarant @user 5</code>")
        return

    username = parts[1]
    try:
        percent = int(parts[2])
    except ValueError:
        await reply(message, f"{E_WARN} Процент должен быть <b>числом</b>.")
        return

    if username in garants:
        await reply(message, f"{E_WARN} Гарант <code>{username}</code> уже есть в списке.")
        return

    garants[username] = percent
    save_json(GARANTS_FILE, garants)
    await reply(message, f"{E_CHECK} Добавлен гарант <code>{username}</code> с комиссией <b>{percent}%</b>.")


async def cmd_delgarant(message: Message):
    if not is_admin(message.from_user.id):
        await reply(message, f"{E_CROSS} <b>У тебя нет прав на удаление гарантов.</b>")
        return

    parts = message.text.strip().split(maxsplit=1)
    if len(parts) < 2:
        await reply(message, f"{E_PENCIL} <b>Использование:</b> <code>/delgarant @user</code>")
        return

    username = parts[1].strip()

    if username not in garants:
        await reply(message, f"{E_WARN} Гарант <code>{username}</code> не найден в списке.")
        return

    del garants[username]
    save_json(GARANTS_FILE, garants)
    await reply(message, f"{E_FLEUR} Гарант <code>{username}</code> удалён из списка.")


# =========================
# === МАНУАЛЫ ===
# =========================
async def cmd_manual(message: Message):
    text = (
        f"{E_BOOK} <b>МАНУАЛ ДЛЯ ВОРКЕРОВ</b>\n\n"
        f"<blockquote>{E_POINT} Ознакомься с инструкцией:\n"
        f"<a href=\"{MANUAL_LINK}\">{MANUAL_LINK}</a></blockquote>\n\n"
        f"{E_BUBBLE} <i>Если что-то непонятно — обратись к наставнику.</i>"
    )
    await reply(message, text)


async def cmd_nicegram(message: Message):
    text = (
        f"{E_PHONE} <b>МАНУАЛ ПО НАЙСГРАМ</b>\n\n"
        f"<blockquote>{E_ROCKET} Инструкция по ворку и использованию:\n"
        f"<a href=\"{NICEGRAM_LINK}\">{NICEGRAM_LINK}</a></blockquote>\n\n"
        f"{E_BUBBLE} <i>Если что-то не понял — пиши наставникам.</i>"
    )
    await reply(message, text)


async def cmd_drain(message: Message):
    text = (
        f"{E_MONEY} <b>МАНУАЛ ПО ДРЕЙНУ</b>\n\n"
        f"<blockquote>{E_WRENCH} Инструкция по дрейнеру:\n"
        f"<a href=\"{DRAIN_LINK}\">{DRAIN_LINK}</a></blockquote>\n\n"
        f"{E_WARN} <i>Внимательно изучи инструкцию перед началом работы.</i>"
    )
    await reply(message, text)


# =========================
# === УПРАВЛЕНИЕ АДМИНАМИ ===
# =========================
async def cmd_add_admin(message: Message):
    if not is_superadmin(message.from_user.id):
        await reply(message, f"{E_CROSS} <b>Только суперадмины могут выдавать права.</b>")
        return

    if not message.reply_to_message:
        await reply(message, f"{E_WARN} Ответь на сообщение пользователя, которому хочешь выдать права.")
        return

    target = message.reply_to_message.from_user
    if target.id in ADMIN_IDS:
        await reply(message, f"{E_WARN} <code>{target.full_name}</code> уже является суперадмином.")
        return
    if target.id in dynamic_admins:
        await reply(message, f"{E_WARN} <code>{target.full_name}</code> уже имеет права администратора.")
        return

    dynamic_admins.append(target.id)
    save_json(ADMINS_FILE, dynamic_admins)

    mention = f'<a href="tg://user?id={target.id}">{target.full_name}</a>'
    await reply(message, f"{E_OK} {mention} получил права администратора.")


async def cmd_del_admin(message: Message):
    if not is_superadmin(message.from_user.id):
        await reply(message, f"{E_CROSS} <b>Только суперадмины могут забирать права.</b>")
        return

    if not message.reply_to_message:
        await reply(message, f"{E_WARN} Ответь на сообщение пользователя, у которого хочешь забрать права.")
        return


    target = message.reply_to_message.from_user
    if target.id in ADMIN_IDS:
        await reply(message, f"{E_CROSS} Нельзя забрать права у суперадмина.")
        return
    if target.id not in dynamic_admins:
        await reply(message, f"{E_WARN} <code>{target.full_name}</code> не имеет выданных прав.")
        return

    dynamic_admins.remove(target.id)
    save_json(ADMINS_FILE, dynamic_admins)

    mention = f'<a href="tg://user?id={target.id}">{target.full_name}</a>'
    await reply(message, f"{E_TRASH} У {mention} забраны права администратора.")



async def main():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    # /help
    dp.message.register(cmd_help, Command("help"))

    # Наставники
    dp.message.register(cmd_list, Command("list"))
    dp.message.register(cmd_list, F.text.lower() == "наставники")
    dp.message.register(cmd_addnast, Command("addnast"))
    dp.message.register(cmd_delnast, Command("delnast"))

    # Гаранты
    dp.message.register(cmd_garants, F.text.lower().in_({"гарант", "гаранты"}))
    dp.message.register(cmd_addgarant, Command("addgarant"))
    dp.message.register(cmd_delgarant, Command("delgarant"))

    # Мануалы
    dp.message.register(cmd_manual, F.text.lower() == "мануал")
    dp.message.register(cmd_nicegram, F.text.lower() == "найсграм")
    dp.message.register(cmd_drain, F.text.lower().in_({"дрейн", "дрейнер"}))

    # Управление админами
    dp.message.register(cmd_add_admin, F.text == ".админ")
    dp.message.register(cmd_del_admin, F.text == ".деладмин")

    print(f"{E_OK} Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())