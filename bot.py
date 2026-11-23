#!/usr/bin/env python3
"""Bot Telegram kiểm tra thông tin kênh TikTok.

Gửi username (hoặc link) TikTok -> bot trả về báo cáo giống mẫu:
thống kê kênh + chi tiết tài khoản, kèm các nút: Thu Gọn Lại,
Gỡ Liên Kết Ngoài, Định Giá Kênh, Nhóm Chat Tiktok.

Chạy 24/7 trên Botkeep (long polling). Token đọc từ biến môi trường BOT_TOKEN.
"""
import html
import asyncio
import json
import logging
import os
import re
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
log = logging.getLogger("tiktok-checker")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
GROUP_URL = os.environ.get("GROUP_URL", "https://t.me/").strip()
# --- watchlist (theo dõi LIVE/DIE) ---
WATCH_FILE = os.environ.get(
    "WATCHLIST_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchlist.json"),
)
WATCH_INTERVAL_SEC = max(60, int(os.environ.get("WATCH_INTERVAL_MIN", "30")) * 60)
WATCH_GAP_SEC = 3  # giãn cách giữa 2 lần check trong 1 lượt quét watchlist
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

COOLDOWN_SEC = 5
_last_check: dict = {}

URL_RE = re.compile(r"https?://[^\s)>\]]+", re.I)
TIKTOK_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.|vm\.|vt\.)?tiktok\.com/@([A-Za-z0-9._]{2,24})", re.I
)
AT_RE = re.compile(r"@([A-Za-z0-9._]{2,24})")

FLAGS = {
    "VN": "🇻🇳", "US": "🇺🇸", "CN": "🇨🇳", "TH": "🇹🇭", "ID": "🇮🇩",
    "MY": "🇲🇾", "PH": "🇵🇭", "SG": "🇸🇬", "KR": "🇰🇷", "JP": "🇯🇵",
    "TW": "🇹🇼", "HK": "🇭🇰", "GB": "🇬🇧", "DE": "🇩🇪", "FR": "🇫🇷",
}
COUNTRIES = {
    "VN": "Vietnam", "US": "United States", "CN": "China", "TH": "Thailand",
    "ID": "Indonesia", "MY": "Malaysia", "PH": "Philippines", "SG": "Singapore",
    "KR": "Korea", "JP": "Japan", "TW": "Taiwan", "HK": "Hong Kong",
}


# ---------- helpers ----------
def pick(d, *keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, ""):
            return d[k]
    return None


def _trim(s: str) -> str:
    return s[:-2] if s.endswith(".0") else s


def fmt_num(n) -> str:
    try:
        n = int(n)
    except (TypeError, ValueError):
        return "0"
    if n >= 1_000_000_000:
        return _trim(f"{n / 1_000_000_000:.1f}") + "B"
    if n >= 1_000_000:
        return _trim(f"{n / 1_000_000:.1f}") + "M"
    if n >= 1_000:
        return _trim(f"{n / 1_000:.1f}") + "K"
    return str(n)


def fmt_vnd(n: int) -> str:
    return f"{int(n):,}".replace(",", ".") + "đ"


def domain_of(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc or url
    except Exception:
        return url


def fmt_time(ts) -> str | None:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=VN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def extract_username(text: str) -> str | None:
    text = (text or "").strip()
    m = TIKTOK_URL_RE.search(text)
    if m:
        return m.group(1)
    m = AT_RE.search(text)
    if m:
        return m.group(1)
    t = text.lstrip("@")
    if re.fullmatch(r"[A-Za-z0-9._]{2,24}", t):
        return t
    return None


# ---------- TikTok data ----------
async def fetch_tiktok_user(unique_id: str) -> dict:
    """Lấy thông tin public của kênh TikTok bằng cách đọc trực tiếp trang
    profile (dữ liệu JSON nhúng trong HTML), không qua API trung gian
    (các API trung gian như tikwm chặn IP máy chủ)."""
    url = f"https://www.tiktok.com/@{unique_id}"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient(
        timeout=25, headers=headers, follow_redirects=True
    ) as client:
        r = await client.get(url)
        if r.status_code != 200:
            raise ValueError(f"http {r.status_code}")
        page = r.text
    m = re.search(
        r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">(.*?)</script>',
        page,
        re.S,
    )
    if not m:
        raise ValueError("no embedded data")
    data = json.loads(m.group(1))
    scope = data.get("__DEFAULT_SCOPE__", {})
    detail = scope.get("webapp.user-detail", {})
    info = detail.get("userInfo", {}) or {}
    user = info.get("user", {}) or {}
    stats = info.get("stats", {}) or {}
    if not user.get("uniqueId"):
        raise ValueError("user not found")
    bio_link = user.get("bioLink")
    if isinstance(bio_link, dict):
        bio_link = bio_link.get("link")
    # Chuẩn hoá về dict phẳng để build_report dùng chung
    return {
        "nickname": user.get("nickname"),
        "unique_id": user.get("uniqueId"),
        "id": user.get("id"),
        "verified": user.get("verified"),
        "private_account": user.get("privateAccount"),
        "create_time": user.get("createTime"),
        "region": user.get("region"),
        "signature": user.get("signature"),
        "bio_link": bio_link,
        "follower_count": stats.get("followerCount"),
        "following_count": stats.get("followingCount"),
        "friend_count": stats.get("friendCount"),
        "heart_count": stats.get("heartCount"),
        "video_count": stats.get("videoCount"),
    }


# ---------- report ----------
def build_report(username: str, user: dict, hide_external: bool = False):
    nickname = pick(user, "nickname") or username
    verified = pick(user, "verified")
    nick_line = f"👤 Nickname: {html.escape(str(nickname))}"
    if verified in (True, 1, "1", "true"):
        nick_line += " ✅"
    uid = pick(user, "id", "uid") or "Không rõ"
    private = pick(user, "private_account", "privateAccount")
    private_txt = "CÓ" if private in (True, 1, "1", "true") else "KHÔNG"
    created = fmt_time(pick(user, "create_time", "createTime")) or "Không rõ"

    region = str(pick(user, "region", "account_region") or "").upper()
    if region:
        country = f"{FLAGS.get(region, '🌍')} {region} ({COUNTRIES.get(region, region)})"
    else:
        country = "Không rõ"

    followers = int(pick(user, "follower_count", "followerCount") or 0)
    following = int(pick(user, "following_count", "followingCount") or 0)
    friends = int(pick(user, "friend_count", "friendCount") or 0)
    hearts = int(pick(user, "heart_count", "heartCount", "total_favorited") or 0)
    videos = int(pick(user, "video_count", "videoCount", "aweme_count") or 0)

    sig = pick(user, "signature") or ""
    hidden_links = URL_RE.findall(str(sig))

    bio_link = pick(user, "bio_link", "bioLink")
    external = None
    if isinstance(bio_link, dict):
        external = bio_link.get("link") or bio_link.get("url")
    elif isinstance(bio_link, str):
        external = bio_link
    if hide_external:
        external = None

    lines = [f"🧐 <b>Kết quả kiểm tra</b> @{html.escape(username)}", ""]
    if hidden_links:
        lines.append(f"🔴 Liên kết ẩn: {html.escape(', '.join(hidden_links[:3]))}")
    else:
        lines.append("✅ Không có liên kết ẩn.")
    if external:
        lines.append(f"🔴 Liên kết ngoài: {html.escape(domain_of(external))}")
    else:
        lines.append("✅ Không có liên kết ngoài.")
    lines += [
        "",
        "📊 <b>THỐNG KÊ KÊNH</b>",
        f"👥 Người theo dõi: {followers}",
        f"👥 Tiếp theo (Đang theo dõi): {following}",
        f"❤️ Lượt thích: {fmt_num(hearts)}",
        f"🎥 Video: {videos}",
        f"👥 Bạn bè: {friends}",
        "",
        "📝 <b>CHI TIẾT</b>",
        nick_line,
        f"🆔 Mã người dùng: {html.escape(str(uid))}",
        f"🔒 Riêng tư: {private_txt}",
        f"📅 Đã tạo: {created}",
    ]
    nick_time = fmt_time(pick(user, "nickname_modify_time", "nicknameModifyTime"))
    if nick_time:
        lines.append(f"🕐 Biệt danh sửa lúc: {nick_time}")
    uname_time = fmt_time(pick(user, "unique_id_modify_time", "uniqueIdModifyTime"))
    lines.append(
        f"👤 Tên người dùng đổi lúc: {uname_time if uname_time else 'Không áp dụng'}"
    )
    lines.append(f"🌍 Quốc gia: {country}")

    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔼 Thu Gọn Lại", callback_data=f"collapse:{username}")],
            [
                InlineKeyboardButton("Gỡ Liên Kết Ngoài", callback_data=f"unlink:{username}"),
                InlineKeyboardButton("💰 Định Giá Kênh", callback_data=f"price:{username}"),
            ],
            [InlineKeyboardButton("💬 Nhóm Chat Tiktok", url=GROUP_URL)],
        ]
    )
    return "\n".join(lines), kb


def estimate_price(user: dict) -> tuple:
    followers = int(pick(user, "follower_count", "followerCount") or 0)
    hearts = int(pick(user, "heart_count", "heartCount") or 0)
    videos = int(pick(user, "video_count", "videoCount") or 0)
    low = followers * 80 + videos * 500
    high = followers * 250 + hearts + videos * 1500
    return max(low, 0), max(high, low)


# ---------- health check (cho Render free tier) ----------
def _start_health_server():
    """Web server nhỏ để nền tảng hosting kiểm tra service còn sống.
    Render free yêu cầu service mở 1 cổng HTTP, nếu không sẽ báo deploy lỗi."""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write("tiktok-check-bot is running".encode())

        def log_message(self, *args):
            pass

    port = int(os.environ.get("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("Health server listening on port %s", port)


# ---------- handlers ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Chào sếp! Gửi cho tôi <b>username</b> hoặc <b>link TikTok</b> "
        "(vd: @phucduynguyen497 hoặc https://www.tiktok.com/@phucduynguyen497) "
        "để check thông tin kênh.\n\nLệnh:\n"
        "/check &lt;username&gt; — xem báo cáo chi tiết (LIVE thì ra đủ thông tin, DIE thì báo DIE)\n"
        "/livedie &lt;user1&gt; &lt;user2&gt;... — check acc LIVE hay DIE (tối đa 20 acc/lần)\n"
        "/checklive &lt;user&gt; — check LIVE/DIE ngay (LIVE ra đủ thông tin) + tự theo dõi, đổi trạng thái là báo\n"
        "/watch &lt;user1&gt; &lt;user2&gt;... — theo dõi acc, đổi trạng thái LIVE/DIE là bot báo ngay\n"
        "/unwatch &lt;user1&gt;... — bỏ theo dõi\n"
        "/watchlist — xem danh sách đang theo dõi",
        parse_mode=ParseMode.HTML,
    )


async def do_check(message, username: str):
    await message.chat.send_action("typing")
    try:
        user = await fetch_tiktok_user(username)
    except Exception as e:
        log.warning("fetch failed for %s: %s", username, e)
        if _is_not_found_error(e):
            await message.reply_text(
                f"❌ <b>DIE</b> — @{html.escape(username)} không tồn tại "
                "(đã bị khóa / đã xóa / sai username), không có thông tin kênh.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await message.reply_text(
                f"⚠️ Không check được @{html.escape(username)} lúc này "
                "(lỗi tạm thời / API bận). Thử lại sau nhé sếp.",
                parse_mode=ParseMode.HTML,
            )
        return
    text, kb = build_report(username, user)
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = extract_username(" ".join(context.args))
    if not username:
        await update.message.reply_text("Dùng: /check @username (vd: /check phucduynguyen497)")
        return
    await do_check(update.message, username)


def parse_usernames(text: str, limit: int = 20) -> list:
    """Tách nhiều username từ text (hỗ trợ @user, link tiktok, cách nhau bởi
    khoảng trắng/phẩy/xuống dòng). Trả về list đã khử trùng, giữ thứ tự."""
    found: list = []
    seen = set()
    for token in re.split(r"[\s,;]+", text or ""):
        token = token.strip()
        if not token:
            continue
        u = extract_username(token)
        if u and u.lower() not in seen:
            seen.add(u.lower())
            found.append(u)
        if len(found) >= limit:
            break
    return found


def _is_not_found_error(e: Exception) -> bool:
    """Lỗi này có nghĩa TikTok xác nhận tài khoản KHÔNG tồn tại (DIE),
    phân biệt với lỗi tạm thời (rate limit, lỗi mạng, TikTok đổi cấu trúc trang)."""
    msg = str(e).lower()
    return "user not found" in msg or "http 404" in msg


async def probe_live(username: str) -> tuple:
    """Trả về (status, is_private, nickname) với status là 'live' | 'die' | 'unknown'.
    - live: lấy được dữ liệu profile.
    - die: TikTok báo không tìm thấy tài khoản (404 / user not found).
    - unknown: lỗi tạm thời (rate limit, mạng, cấu trúc trang) — chưa chắc chắn.
    Thử lại 1 lần nếu lỗi tạm thời."""
    last_err = None
    for _ in range(2):
        try:
            user = await fetch_tiktok_user(username)
            return "live", bool(user.get("private_account")), str(user.get("nickname") or "")
        except Exception as e:
            last_err = e
            log.warning("livedie probe failed for %s: %s", username, e)
            if _is_not_found_error(e):
                return "die", False, ""
            await asyncio.sleep(1)
    return "unknown", False, ""


# ---------- watchlist: theo dõi LIVE/DIE, báo khi đổi trạng thái ----------
_watch_lock = threading.Lock()


def _watch_load() -> dict:
    try:
        with open(WATCH_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _watch_save(data: dict):
    tmp = WATCH_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, WATCH_FILE)


def _watch_now() -> str:
    return datetime.now(tz=VN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _watch_upsert(username: str, status: str, nickname: str, chat_id: int):
    with _watch_lock:
        data = _watch_load()
        key = username.lower()
        entry = data.get(key, {})
        entry.update(
            {
                "username": username,
                "status": status,
                "nickname": nickname or entry.get("nickname", ""),
                "last_checked": _watch_now(),
                "chat_id": chat_id,
            }
        )
        data[key] = entry
        _watch_save(data)


def _status_tag(status: str, is_private: bool = False) -> str:
    if status == "live":
        return "🔒 LIVE (riêng tư)" if is_private else "✅ LIVE"
    if status == "die":
        return "❌ DIE"
    return "❓ CHƯA CHẮC"


async def _notify_status_change(bot, entry: dict, old: str, new: str, nickname: str):
    username = entry["username"]
    chat_id = entry.get("chat_id")
    if not chat_id:
        return
    nick = f" ({html.escape(nickname)})" if nickname else ""
    try:
        if new == "live":
            head = (
                f"🟢 <b>@{html.escape(username)}</b>{nick} vừa <b>LIVE</b> trở lại "
                f"(trước đó: {_status_tag(old)})!\n\n"
            )
            try:
                user = await fetch_tiktok_user(username)
                text, kb = build_report(username, user)
                await bot.send_message(
                    chat_id, head + text, parse_mode=ParseMode.HTML, reply_markup=kb
                )
            except Exception:
                await bot.send_message(chat_id, head.rstrip(), parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(
                chat_id,
                f"🔴 <b>@{html.escape(username)}</b>{nick} vừa <b>DIE</b> "
                f"(trước đó: {_status_tag(old)}) — tài khoản không còn truy cập được.",
                parse_mode=ParseMode.HTML,
            )
    except Exception as e:
        log.warning("watch notify failed for %s: %s", username, e)


async def _watch_check_once(app):
    """Quét toàn bộ watchlist 1 lượt; chỉ báo khi trạng thái đổi LIVE<->DIE."""
    with _watch_lock:
        data = _watch_load()
    if not data:
        return
    changed = False
    for i, (key, entry) in enumerate(list(data.items())):
        username = entry.get("username") or key
        old = entry.get("status", "unknown")
        status, is_private, nickname = await probe_live(username)
        entry["last_checked"] = _watch_now()
        if nickname:
            entry["nickname"] = nickname
        changed = True
        if status in ("live", "die") and old in ("live", "die") and status != old:
            entry["status"] = status
            await _notify_status_change(app.bot, entry, old, status, nickname)
        elif status in ("live", "die"):
            entry["status"] = status
        # status == "unknown": giữ nguyên trạng thái cũ, không báo
        if i < len(data) - 1:
            await asyncio.sleep(WATCH_GAP_SEC)
    if changed:
        with _watch_lock:
            _watch_save(data)


async def _watch_job(context: ContextTypes.DEFAULT_TYPE):
    await _watch_check_once(context.application)


async def _watch_loop(app):
    while True:
        await asyncio.sleep(WATCH_INTERVAL_SEC)
        try:
            await _watch_check_once(app)
        except Exception as e:
            log.warning("watch loop error: %s", e)


async def _post_init(app):
    if app.job_queue is not None:
        app.job_queue.run_repeating(
            _watch_job, interval=WATCH_INTERVAL_SEC, first=WATCH_INTERVAL_SEC
        )
        log.info("Watchlist job queue started, interval=%ss", WATCH_INTERVAL_SEC)
    else:
        asyncio.create_task(_watch_loop(app))
        log.info("Watchlist asyncio loop started, interval=%ss", WATCH_INTERVAL_SEC)


async def cmd_checklive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check LIVE/DIE 1 lần + tự thêm vào watchlist để báo khi đổi trạng thái.
    LIVE -> trả báo cáo đầy đủ; DIE -> báo DIE. Cả 2 đều được theo dõi."""
    usernames = parse_usernames(" ".join(context.args))
    if not usernames:
        await update.message.reply_text(
            "Dùng: /checklive @user — check LIVE/DIE ngay và tự theo dõi, "
            "đổi trạng thái là bot báo. Bỏ theo dõi: /unwatch @user"
        )
        return
    chat_id = update.effective_chat.id
    for i, u in enumerate(usernames):
        status, is_private, nickname = await probe_live(u)
        _watch_upsert(u, status, nickname, chat_id)
        if status == "live":
            try:
                user = await fetch_tiktok_user(u)
                text, kb = build_report(u, user)
                await update.message.reply_text(
                    text
                    + "\n\n👁️ <i>Đã đưa vào danh sách theo dõi — đổi trạng thái là báo ngay.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
            except Exception:
                await update.message.reply_text(
                    f"✅ <b>LIVE</b> — @{html.escape(u)} (chưa lấy được báo cáo chi tiết, thử /check sau)\n"
                    "👁️ Đã đưa vào danh sách theo dõi.",
                    parse_mode=ParseMode.HTML,
                )
        elif status == "die":
            await update.message.reply_text(
                f"❌ <b>DIE</b> — @{html.escape(u)} không tồn tại (đã bị khóa / đã xóa / sai username).\n"
                "👁️ Đã đưa vào danh sách theo dõi — nếu acc LIVE trở lại bot sẽ báo kèm thông tin kênh.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"❓ <b>CHƯA CHẮC</b> — @{html.escape(u)} lỗi tạm thời, chưa kết luận được.\n"
                "👁️ Đã đưa vào danh sách theo dõi, bot sẽ check lại định kỳ.",
                parse_mode=ParseMode.HTML,
            )
        if i < len(usernames) - 1:
            await asyncio.sleep(0.6)


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    usernames = parse_usernames(" ".join(context.args))
    if not usernames:
        await update.message.reply_text(
            "Dùng: /watch @user1 @user2 ... — bot sẽ theo dõi và báo ngay khi acc đổi LIVE/DIE."
        )
        return
    chat_id = update.effective_chat.id
    lines = []
    for i, u in enumerate(usernames):
        status, is_private, nickname = await probe_live(u)
        _watch_upsert(u, status, nickname, chat_id)
        nick = f" ({html.escape(nickname)})" if nickname else ""
        lines.append(f"👁️ Đang theo dõi @{html.escape(u)}{nick} — hiện tại: {_status_tag(status, is_private)}")
        if i < len(usernames) - 1:
            await asyncio.sleep(0.6)
    await update.message.reply_text(
        "\n".join(lines)
        + f"\n\nBot check lại mỗi {WATCH_INTERVAL_SEC // 60} phút, đổi trạng thái là báo ngay. "
        "Bỏ theo dõi: /unwatch @user · Xem danh sách: /watchlist",
        parse_mode=ParseMode.HTML,
    )


async def cmd_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    usernames = parse_usernames(" ".join(context.args))
    if not usernames:
        await update.message.reply_text("Dùng: /unwatch @user1 @user2 ...")
        return
    removed, missing = [], []
    with _watch_lock:
        data = _watch_load()
        for u in usernames:
            if data.pop(u.lower(), None) is not None:
                removed.append(u)
            else:
                missing.append(u)
        _watch_save(data)
    lines = []
    if removed:
        lines.append("🗑️ Đã bỏ theo dõi: " + ", ".join(f"@{html.escape(u)}" for u in removed))
    if missing:
        lines.append("ℹ️ Không có trong danh sách: " + ", ".join(f"@{html.escape(u)}" for u in missing))
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with _watch_lock:
        data = _watch_load()
    if not data:
        await update.message.reply_text(
            "📋 Chưa theo dõi acc nào. Thêm bằng: /watch @user"
        )
        return
    lines = [f"📋 <b>DANH SÁCH THEO DÕI</b> ({len(data)} acc)"]
    for entry in sorted(data.values(), key=lambda e: e.get("username", "").lower()):
        nick = f" ({html.escape(entry['nickname'])})" if entry.get("nickname") else ""
        lines.append(
            f"{_status_tag(entry.get('status', 'unknown'))} — @{html.escape(entry.get('username', '?'))}{nick}"
            f" · check cuối: {html.escape(str(entry.get('last_checked', '?')))}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_livedie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    now = datetime.now().timestamp()
    if now - _last_check.get(("livedie", uid), 0) < 15:
        await update.message.reply_text("⏳ Chậm thôi sếp ơi, chờ vài giây rồi check tiếp nhé.")
        return
    usernames = parse_usernames(" ".join(context.args))
    if not usernames:
        await update.message.reply_text(
            "Dùng: /livedie @user1 @user2 ... (vd: /livedie tiktok charlidamelio)\n"
            "Tối đa 20 acc một lần nhé sếp."
        )
        return
    _last_check[("livedie", uid)] = now
    status_msg = await update.message.reply_text(
        f"⏳ Đang check {len(usernames)} acc, sếp chờ xíu..."
    )
    lines = []
    n_live = n_die = n_unknown = 0
    for i, u in enumerate(usernames, 1):
        status, is_private, nickname = await probe_live(u)
        nick = f" ({html.escape(nickname)})" if nickname else ""
        if status == "live":
            n_live += 1
            tag = "🔒 LIVE (riêng tư)" if is_private else "✅ LIVE"
        elif status == "die":
            n_die += 1
            tag = "❌ DIE"
        else:
            n_unknown += 1
            tag = "❓ CHƯA CHẮC (lỗi tạm thời, thử lại sau)"
        lines.append(f"{tag} — @{html.escape(u)}{nick}")
        if i < len(usernames):
            await asyncio.sleep(0.6)
            if i % 5 == 0:
                try:
                    await status_msg.edit_text(f"⏳ Đang check {i}/{len(usernames)} acc...")
                except Exception:
                    pass
    text = (
        f"⚡ <b>KẾT QUẢ LIVE/DIE</b> ({len(usernames)} acc)\n"
        f"✅ LIVE: <b>{n_live}</b> · ❌ DIE: <b>{n_die}</b> · ❓ CHƯA CHẮC: <b>{n_unknown}</b>\n\n"
        + "\n".join(lines)
    )
    await status_msg.edit_text(text, parse_mode=ParseMode.HTML)


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    now = datetime.now().timestamp()
    if now - _last_check.get(uid, 0) < COOLDOWN_SEC:
        await update.message.reply_text("⏳ Chậm thôi sếp ơi, chờ vài giây rồi check tiếp nhé.")
        return
    username = extract_username(update.message.text or "")
    if not username:
        await update.message.reply_text(
            "Gửi tôi username hoặc link TikTok nhé sếp (vd: @phucduynguyen497)."
        )
        return
    _last_check[uid] = now
    await do_check(update.message, username)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    action, _, username = (q.data or "").partition(":")
    if not username:
        return
    try:
        user = await fetch_tiktok_user(username)
    except Exception:
        await q.edit_message_text("⚠️ Không lấy được dữ liệu, thử lại sau nhé sếp.")
        return

    if action == "collapse":
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔽 Mở Rộng", callback_data=f"expand:{username}")]]
        )
        await q.edit_message_text(
            f"🧐 Kênh @{html.escape(username)} — đã thu gọn.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
    elif action == "expand":
        text, kb = build_report(username, user)
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    elif action == "unlink":
        text, kb = build_report(username, user, hide_external=True)
        await q.edit_message_text(
            text + "\n\n<i>Đã gỡ liên kết ngoài khỏi báo cáo.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
    elif action == "price":
        low, high = estimate_price(user)
        await q.edit_message_text(
            f"💰 <b>Định giá kênh</b> @{html.escape(username)}\n\n"
            f"Ước tính tham khảo: <b>{fmt_vnd(low)} – {fmt_vnd(high)}</b>\n"
            f"<i>(dựa trên lượt follow, lượt thích và số video; "
            f"giá thực tế do 2 bên thỏa thuận)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Quay Lại", callback_data=f"expand:{username}")]]
            ),
        )


def main():
    if not BOT_TOKEN:
        raise SystemExit("Thiếu BOT_TOKEN — set biến môi trường BOT_TOKEN rồi chạy lại.")
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler(["check", "id"], cmd_check))
    app.add_handler(CommandHandler("livedie", cmd_livedie))
    app.add_handler(CommandHandler("checklive", cmd_checklive))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("unwatch", cmd_unwatch))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    _start_health_server()
    log.info("bot version: v2.3-watchlist (/livedie 3-state + /watch /unwatch /watchlist, bao khi doi LIVE/DIE)")
    log.info("Bot đang chạy, chờ tin nhắn...")
    app.run_polling()


if __name__ == "__main__":
    main()
