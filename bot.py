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
        "/check &lt;username&gt; — xem báo cáo chi tiết\n"
        "/livedie &lt;user1&gt; &lt;user2&gt;... — check acc LIVE hay DIE (tối đa 20 acc/lần)",
        parse_mode=ParseMode.HTML,
    )


async def do_check(message, username: str):
    await message.chat.send_action("typing")
    try:
        user = await fetch_tiktok_user(username)
    except Exception as e:
        log.warning("fetch failed for %s: %s", username, e)
        await message.reply_text(
            f"⚠️ Không check được @{html.escape(username)} lúc này "
            "(kênh không tồn tại / riêng tư / API bận). Thử lại sau nhé sếp.",
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


async def probe_live(username: str) -> tuple:
    """Trả về (is_live, is_private, nickname). Thử lại 1 lần nếu lỗi mạng."""
    user = None
    for _ in range(2):
        try:
            user = await fetch_tiktok_user(username)
            break
        except Exception as e:
            log.warning("livedie probe failed for %s: %s", username, e)
            await asyncio.sleep(1)
    if not user:
        return False, False, ""
    return True, bool(user.get("private_account")), str(user.get("nickname") or "")


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
    n_live = 0
    for i, u in enumerate(usernames, 1):
        is_live, is_private, nickname = await probe_live(u)
        nick = f" ({html.escape(nickname)})" if nickname else ""
        if is_live:
            n_live += 1
            tag = "🔒 LIVE (riêng tư)" if is_private else "✅ LIVE"
        else:
            tag = "❌ DIE"
        lines.append(f"{tag} — @{html.escape(u)}{nick}")
        if i < len(usernames):
            await asyncio.sleep(0.6)
            if i % 5 == 0:
                try:
                    await status_msg.edit_text(f"⏳ Đang check {i}/{len(usernames)} acc...")
                except Exception:
                    pass
    n_die = len(usernames) - n_live
    text = (
        f"⚡ <b>KẾT QUẢ LIVE/DIE</b> ({len(usernames)} acc)\n"
        f"✅ LIVE: <b>{n_live}</b> · ❌ DIE: <b>{n_die}</b>\n\n" + "\n".join(lines)
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
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler(["check", "id"], cmd_check))
    app.add_handler(CommandHandler("livedie", cmd_livedie))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    _start_health_server()
    log.info("bot version: v2.1-livedie (them lenh /livedie check LIVE/DIE)")
    log.info("Bot đang chạy, chờ tin nhắn...")
    app.run_polling()


if __name__ == "__main__":
    main()
