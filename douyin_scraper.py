from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

from playwright.async_api import BrowserContext, Page, Response, async_playwright

try:
    import browser_cookie3
except ImportError:  # pragma: no cover
    browser_cookie3 = None


DEFAULT_KEYWORDS = [
    "工地机器人",
    "施工机器人",
    "建筑机器人",
    "建造机器人",
    "机械臂建造/施工",
    "3D打印建造/施工",
    "无人机建造/施工",
    "无人塔机/塔吊",
    "智能塔机/塔吊",
    "自动化建造/施工",
    "造楼机",
    "智能施工装备",
]

SEARCH_RESPONSE_HINTS = (
    "/aweme/v1/web/general/search/",
    "/aweme/v1/web/search/",
)

COMMENT_RESPONSE_HINTS = (
    "/aweme/v1/web/comment/list/",
    "/aweme/v1/web/comment/list/reply/",
)

COMMENT_SELECTORS = [
    "[data-e2e='comment-list']",
    "[class*='comment-list']",
    "[class*='CommentList']",
    "[class*='commentList']",
    "[class*='panel'][class*='comment']",
]

LOGGER = logging.getLogger("douyin_scraper")

CHROME_CANDIDATES = [
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
]
CHROME_USER_DATA_DIR = Path.home() / "Library/Application Support/Google/Chrome"


@dataclass(slots=True)
class PostRecord:
    keyword: str
    aweme_id: str
    user_id: str
    location: str
    published_at: str
    text: str
    post_url: str


@dataclass(slots=True)
class CommentRecord:
    keyword: str
    aweme_id: str
    post_user_id: str
    user_id: str
    location: str
    published_at: str
    text: str
    comment_id: str


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Douyin posts and comments for a keyword list."
    )
    parser.add_argument(
        "--keywords-file",
        type=Path,
        default=Path("keywords.txt"),
        help="Text file with one keyword per line.",
    )
    parser.add_argument(
        "--keyword",
        action="append",
        dest="keywords",
        help="Repeatable single keyword. Overrides keywords.txt when provided.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for exported CSV and JSONL files.",
    )
    parser.add_argument(
        "--profile-dir",
        type=Path,
        default=Path(".playwright-profile"),
        help="Persistent browser profile directory for Douyin login state.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode. First run is usually easier without this flag.",
    )
    parser.add_argument(
        "--max-posts-per-keyword",
        type=int,
        default=15,
        help="Maximum number of posts to keep per keyword.",
    )
    parser.add_argument(
        "--max-comments-per-post",
        type=int,
        default=50,
        help="Maximum number of comments to keep per post.",
    )
    parser.add_argument(
        "--max-search-scrolls",
        type=int,
        default=18,
        help="Maximum scroll rounds on each search result page.",
    )
    parser.add_argument(
        "--max-comment-scrolls",
        type=int,
        default=20,
        help="Maximum scroll rounds on each post detail page.",
    )
    parser.add_argument(
        "--scroll-wait-ms",
        type=int,
        default=1800,
        help="Wait time after each scroll operation.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    parser.add_argument(
        "--chrome-profile-name",
        default=None,
        help="Chrome profile folder name used to import existing Douyin cookies.",
    )
    parser.add_argument(
        "--skip-chrome-cookies",
        action="store_true",
        help="Do not import Douyin cookies from local Chrome profile.",
    )
    return parser.parse_args()


def read_keywords(args: argparse.Namespace) -> list[str]:
    if args.keywords:
        keywords = [item.strip() for item in args.keywords if item and item.strip()]
        if keywords:
            return keywords

    if args.keywords_file.exists():
        lines = args.keywords_file.read_text(encoding="utf-8").splitlines()
        keywords = [line.strip() for line in lines if line.strip()]
        if keywords:
            return keywords

    return list(DEFAULT_KEYWORDS)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\u200b", " ").split())


def first_non_empty(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def format_timestamp(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        number = int(value)
    except (TypeError, ValueError):
        return clean_text(value)

    if number > 10**12:
        number = number // 1000
    try:
        dt = datetime.fromtimestamp(number, tz=timezone.utc).astimezone()
    except (OverflowError, OSError, ValueError):
        return str(value)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def pick_user_id(user: dict[str, Any] | None) -> str:
    if not isinstance(user, dict):
        return ""
    return first_non_empty(
        user.get("unique_id"),
        user.get("short_id"),
        user.get("sec_uid"),
        user.get("uid"),
    )


def extract_post_location(aweme: dict[str, Any]) -> str:
    poi_info = aweme.get("poi_info") or {}
    text_extra = aweme.get("text_extra") or []
    poi_name = poi_info.get("poi_name") if isinstance(poi_info, dict) else ""
    poi_city = poi_info.get("city") if isinstance(poi_info, dict) else ""
    hashtag_location = ""
    if isinstance(text_extra, list):
        for item in text_extra:
            if not isinstance(item, dict):
                continue
            hashtag_location = first_non_empty(
                item.get("hashtag_name"),
                item.get("hashtag_info", {}).get("hashtag_name")
                if isinstance(item.get("hashtag_info"), dict)
                else "",
            )
            if hashtag_location and any(mark in hashtag_location for mark in ("省", "市", "区", "县")):
                break
            hashtag_location = ""
    return first_non_empty(
        aweme.get("ip_label"),
        aweme.get("region"),
        aweme.get("location"),
        poi_name,
        poi_city,
        hashtag_location,
    )


def extract_comment_location(comment: dict[str, Any]) -> str:
    user = comment.get("user") or {}
    return first_non_empty(
        comment.get("ip_label"),
        comment.get("ip_label_text"),
        comment.get("label_text"),
        comment.get("location"),
        user.get("city"),
        user.get("region"),
    )


def build_search_url(keyword: str) -> str:
    encoded = quote(keyword, safe="")
    return (
        f"https://www.douyin.com/search/{encoded}"
        "?source=switch_tab&type=video&publish_time=0&sort_type=0"
    )


def build_post_url(aweme_id: str) -> str:
    return f"https://www.douyin.com/video/{aweme_id}"


def resolve_browser_launch_options() -> dict[str, Any]:
    for candidate in CHROME_CANDIDATES:
        if candidate.exists():
            LOGGER.info("使用系统浏览器: %s", candidate)
            return {"executable_path": str(candidate)}
    LOGGER.info("未发现系统 Chrome，改用 Playwright 自带 Chromium")
    return {}


def resolve_active_chrome_profile() -> str:
    local_state = CHROME_USER_DATA_DIR / "Local State"
    if local_state.exists():
        try:
            data = json.loads(local_state.read_text(encoding="utf-8"))
            profile = data.get("profile") or {}
            last_active = profile.get("last_active_profiles") or []
            if isinstance(last_active, list) and last_active:
                profile_name = clean_text(last_active[0])
                if profile_name:
                    return profile_name
        except Exception:
            LOGGER.debug("读取 Chrome Local State 失败", exc_info=True)
    return "Default"


def resolve_chrome_cookie_db(profile_name: str) -> Path:
    return CHROME_USER_DATA_DIR / profile_name / "Cookies"


def load_chrome_douyin_cookies(profile_name: str) -> list[dict[str, Any]]:
    if browser_cookie3 is None:
        LOGGER.warning("未安装 browser-cookie3，跳过 Chrome cookies 导入")
        return []

    cookie_db = resolve_chrome_cookie_db(profile_name)
    if not cookie_db.exists():
        LOGGER.warning("未找到 Chrome cookies 数据库: %s", cookie_db)
        return []

    try:
        cookie_jar = browser_cookie3.chrome(
            cookie_file=str(cookie_db),
            domain_name=".douyin.com",
        )
    except Exception:
        LOGGER.warning("读取 Chrome cookies 失败", exc_info=True)
        return []

    cookies: list[dict[str, Any]] = []
    for cookie in cookie_jar:
        name = clean_text(cookie.name)
        value = clean_text(cookie.value)
        domain = clean_text(cookie.domain)
        path = clean_text(cookie.path) or "/"
        if not name or not value or not domain:
            continue

        item: dict[str, Any] = {
            "name": name,
            "value": value,
            "domain": domain,
            "path": path,
            "secure": bool(cookie.secure),
            "httpOnly": bool(getattr(cookie, "_rest", {}).get("HttpOnly")),
        }
        if getattr(cookie, "expires", None):
            try:
                item["expires"] = int(cookie.expires)
            except (TypeError, ValueError):
                pass
        cookies.append(item)
    return cookies


def is_aweme_candidate(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    aweme_id = payload.get("aweme_id") or payload.get("group_id")
    if not aweme_id:
        return False
    return any(
        key in payload
        for key in ("desc", "author", "create_time", "statistics", "aweme_type")
    )


def extract_aweme_objects(payload: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    stack = [payload]

    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            aweme_info = current.get("aweme_info")
            if isinstance(aweme_info, dict):
                stack.append(aweme_info)

            mix_items = current.get("mix_items")
            if isinstance(mix_items, list):
                stack.extend(mix_items)

            if is_aweme_candidate(current):
                aweme_id = str(current.get("aweme_id") or current.get("group_id"))
                if aweme_id not in seen_ids:
                    seen_ids.add(aweme_id)
                    found.append(current)

            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)

    return found


def extract_comment_objects(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    comment_list = payload.get("comments") or payload.get("comment_list") or []
    if not isinstance(comment_list, list):
        return []

    found: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for comment in comment_list:
        if not isinstance(comment, dict):
            continue
        comment_id = clean_text(comment.get("cid"))
        if not comment_id or comment_id in seen_ids:
            continue
        seen_ids.add(comment_id)
        found.append(comment)

    return found


def normalize_post(keyword: str, aweme: dict[str, Any]) -> PostRecord | None:
    aweme_id = clean_text(aweme.get("aweme_id") or aweme.get("group_id"))
    if not aweme_id:
        return None

    text = first_non_empty(
        aweme.get("desc"),
        aweme.get("share_info", {}).get("share_desc")
        if isinstance(aweme.get("share_info"), dict)
        else "",
    )
    if not text:
        text = "[无正文]"

    return PostRecord(
        keyword=keyword,
        aweme_id=aweme_id,
        user_id=pick_user_id(aweme.get("author")),
        location=extract_post_location(aweme),
        published_at=format_timestamp(aweme.get("create_time")),
        text=text,
        post_url=build_post_url(aweme_id),
    )


def normalize_comment(
    keyword: str,
    aweme_id: str,
    post_user_id: str,
    comment: dict[str, Any],
) -> CommentRecord | None:
    comment_id = clean_text(comment.get("cid"))
    text = clean_text(comment.get("text"))
    if not comment_id or not text:
        return None

    return CommentRecord(
        keyword=keyword,
        aweme_id=aweme_id,
        post_user_id=post_user_id,
        user_id=pick_user_id(comment.get("user")),
        location=extract_comment_location(comment),
        published_at=format_timestamp(comment.get("create_time")),
        text=text,
        comment_id=comment_id,
    )


class NetworkCollector:
    def __init__(self) -> None:
        self.search_payloads: list[dict[str, Any]] = []
        self.comment_payloads: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._pending_tasks: set[asyncio.Task[Any]] = set()

    def attach(self, page: Page) -> None:
        page.on("response", self._handle_response)

    def clear_search(self) -> None:
        self.search_payloads.clear()

    def clear_comments(self, aweme_id: str | None = None) -> None:
        if aweme_id is None:
            self.comment_payloads.clear()
            return
        self.comment_payloads.pop(aweme_id, None)

    async def wait_pending(self) -> None:
        if not self._pending_tasks:
            return
        await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)

    def _handle_response(self, response: Response) -> None:
        task = asyncio.create_task(self._capture_response(response))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _capture_response(self, response: Response) -> None:
        url = response.url
        if response.status != 200:
            return
        if any(marker in url for marker in SEARCH_RESPONSE_HINTS):
            payload = await self._safe_json(response)
            if isinstance(payload, dict):
                self.search_payloads.append(payload)
            return

        if any(marker in url for marker in COMMENT_RESPONSE_HINTS):
            payload = await self._safe_json(response)
            if not isinstance(payload, dict):
                return
            aweme_id = self._extract_aweme_id(url, payload)
            if aweme_id:
                self.comment_payloads[aweme_id].append(payload)

    async def _safe_json(self, response: Response) -> dict[str, Any] | None:
        try:
            payload = await response.json()
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _extract_aweme_id(url: str, payload: dict[str, Any]) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        aweme_id = clean_text(query.get("aweme_id", [""])[0])
        if aweme_id:
            return aweme_id
        return clean_text(payload.get("aweme_id"))

    def collect_posts(self, keyword: str) -> list[PostRecord]:
        posts_by_id: dict[str, PostRecord] = {}
        for payload in self.search_payloads:
            for aweme in extract_aweme_objects(payload):
                record = normalize_post(keyword, aweme)
                if record and record.aweme_id not in posts_by_id:
                    posts_by_id[record.aweme_id] = record
        return list(posts_by_id.values())

    def collect_comments(
        self,
        keyword: str,
        aweme_id: str,
        post_user_id: str,
    ) -> list[CommentRecord]:
        comments_by_id: dict[str, CommentRecord] = {}
        for payload in self.comment_payloads.get(aweme_id, []):
            for comment in extract_comment_objects(payload):
                record = normalize_comment(keyword, aweme_id, post_user_id, comment)
                if record and record.comment_id not in comments_by_id:
                    comments_by_id[record.comment_id] = record
        return list(comments_by_id.values())


class DouyinScraper:
    def __init__(self, args: argparse.Namespace, keywords: list[str]) -> None:
        self.args = args
        self.keywords = keywords
        self.collector = NetworkCollector()

    async def run(self) -> tuple[list[PostRecord], list[CommentRecord]]:
        async with async_playwright() as playwright:
            launch_options = resolve_browser_launch_options()
            context = await playwright.chromium.launch_persistent_context(
                user_data_dir=str(self.args.profile_dir),
                headless=self.args.headless,
                viewport={"width": 1440, "height": 1000},
                locale="zh-CN",
                args=["--disable-blink-features=AutomationControlled"],
                **launch_options,
            )
            try:
                await self._bootstrap_login_state(context)
                await context.route("**/*", self._route_request)
                page = context.pages[0] if context.pages else await context.new_page()
                self.collector.attach(page)
                await self._open_home(page)

                all_posts: list[PostRecord] = []
                all_comments: list[CommentRecord] = []

                for keyword in self.keywords:
                    posts = await self._scrape_keyword(page, keyword)
                    all_posts.extend(posts)

                    for post in posts:
                        comments = await self._scrape_comments(page, post)
                        all_comments.extend(comments)

                return all_posts, all_comments
            finally:
                await context.close()

    async def _bootstrap_login_state(self, context: BrowserContext) -> None:
        if self.args.skip_chrome_cookies:
            LOGGER.info("已禁用 Chrome cookies 导入")
            return
        profile_name = self.args.chrome_profile_name or resolve_active_chrome_profile()
        cookies = load_chrome_douyin_cookies(profile_name)
        if not cookies:
            LOGGER.info("未导入到任何 Chrome Douyin cookies")
            return
        await context.add_cookies(cookies)
        LOGGER.info("已从 Chrome profile %s 导入 %s 个 Douyin cookies", profile_name, len(cookies))

    async def _route_request(self, route) -> None:  # type: ignore[no-untyped-def]
        if route.request.resource_type == "media":
            await route.abort()
            return
        await route.continue_()

    async def _open_home(self, page: Page) -> None:
        await page.goto("https://www.douyin.com/", wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)

    async def _scrape_keyword(self, page: Page, keyword: str) -> list[PostRecord]:
        LOGGER.info("采集关键词: %s", keyword)
        self.collector.clear_search()
        await page.goto(build_search_url(keyword), wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        await self.collector.wait_pending()

        posts = self.collector.collect_posts(keyword)
        needs_intervention = await self._needs_manual_intervention(page)
        if not posts and self.args.headless and needs_intervention:
            raise RuntimeError(
                "抖音返回了登录/验证码页面。请先去掉 --headless 运行一次，"
                "在浏览器中完成登录或验证，然后再重试。"
            )
        if not posts and not self.args.headless:
            await self._prompt_manual_login(page, keyword)
            posts = self.collector.collect_posts(keyword)

        stagnant_rounds = 0
        for _ in range(self.args.max_search_scrolls):
            before = len(posts)
            if before >= self.args.max_posts_per_keyword:
                break
            await page.mouse.move(720, 860)
            await page.mouse.wheel(0, 3200)
            await page.wait_for_timeout(self.args.scroll_wait_ms)
            await self.collector.wait_pending()
            posts = self.collector.collect_posts(keyword)
            if len(posts) == before:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            if stagnant_rounds >= 3:
                break

        posts = posts[: self.args.max_posts_per_keyword]
        LOGGER.info("关键词 %s 采集到帖子 %s 条", keyword, len(posts))
        return posts

    async def _prompt_manual_login(self, page: Page, keyword: str) -> None:
        print()
        print("未能立即拿到搜索结果，浏览器可能需要登录或过风控。")
        print("请在打开的 Chromium 窗口中完成登录，然后回到终端按回车继续。")
        await asyncio.to_thread(input, "")
        self.collector.clear_search()
        await page.goto(build_search_url(keyword), wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        await self.collector.wait_pending()

    async def _needs_manual_intervention(self, page: Page) -> bool:
        title = clean_text(await page.title())
        if any(marker in title for marker in ("验证码", "登录", "访问受限")):
            return True
        try:
            body_text = clean_text(await page.locator("body").inner_text(timeout=3000))
        except Exception:
            return False
        return any(marker in body_text for marker in ("验证码", "扫码登录", "访问受限"))

    async def _scrape_comments(self, page: Page, post: PostRecord) -> list[CommentRecord]:
        LOGGER.info("采集评论: %s", post.post_url)
        self.collector.clear_comments(post.aweme_id)
        await page.goto(post.post_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        await self.collector.wait_pending()

        comments = self.collector.collect_comments(post.keyword, post.aweme_id, post.user_id)
        stagnant_rounds = 0

        for _ in range(self.args.max_comment_scrolls):
            before = len(comments)
            if before >= self.args.max_comments_per_post:
                break
            await self._scroll_comment_panel(page)
            await page.wait_for_timeout(self.args.scroll_wait_ms)
            await self.collector.wait_pending()
            comments = self.collector.collect_comments(post.keyword, post.aweme_id, post.user_id)
            if len(comments) == before:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            if stagnant_rounds >= 3:
                break

        comments = comments[: self.args.max_comments_per_post]
        LOGGER.info("帖子 %s 采集到评论 %s 条", post.aweme_id, len(comments))
        return comments

    async def _scroll_comment_panel(self, page: Page) -> None:
        for selector in COMMENT_SELECTORS:
            locator = page.locator(selector).first
            try:
                count = await locator.count()
                if count == 0:
                    continue
                box = await locator.bounding_box()
            except Exception:
                continue
            if not box:
                continue
            x = box["x"] + box["width"] / 2
            y = box["y"] + min(box["height"] / 2, 180)
            await page.mouse.move(x, y)
            await page.mouse.wheel(0, 2200)
            return

        await page.mouse.move(1180, 480)
        await page.mouse.wheel(0, 2200)


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_results(output_dir: Path, posts: list[PostRecord], comments: list[CommentRecord]) -> None:
    post_rows = [asdict(item) for item in posts]
    comment_rows = [asdict(item) for item in comments]

    write_csv(
        output_dir / "posts.csv",
        post_rows,
        ["keyword", "aweme_id", "user_id", "location", "published_at", "text", "post_url"],
    )
    write_csv(
        output_dir / "comments.csv",
        comment_rows,
        [
            "keyword",
            "aweme_id",
            "post_user_id",
            "user_id",
            "location",
            "published_at",
            "text",
            "comment_id",
        ],
    )
    write_jsonl(output_dir / "posts.jsonl", post_rows)
    write_jsonl(output_dir / "comments.jsonl", comment_rows)


async def async_main() -> int:
    args = parse_args()
    configure_logging(args.verbose)
    keywords = read_keywords(args)
    LOGGER.info("本次共处理关键词 %s 个", len(keywords))
    scraper = DouyinScraper(args, keywords)
    posts, comments = await scraper.run()
    save_results(args.output_dir, posts, comments)
    LOGGER.info(
        "采集完成: 帖子 %s 条, 评论 %s 条, 输出目录 %s",
        len(posts),
        len(comments),
        args.output_dir,
    )
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main())
    except RuntimeError as exc:
        LOGGER.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("用户中断执行")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
