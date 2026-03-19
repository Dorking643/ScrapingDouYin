# Douyin Scraper

使用 Playwright 打开抖音网页，按关键词采集帖子与评论，并分别导出为独立文件。

## 抓取字段

`posts.csv`

- `keyword`
- `aweme_id`
- `user_id`
- `location`
- `published_at`
- `text`
- `post_url`

`comments.csv`

- `keyword`
- `aweme_id`
- `post_user_id`
- `user_id`
- `location`
- `published_at`
- `text`

## 安装

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

如果你本机没有安装 Chrome，再执行：

```bash
./.venv/bin/playwright install chromium
```

## 运行

第一次建议非无头运行，方便扫码登录：

```bash
./.venv/bin/python douyin_scraper.py
```

如果已经完成过登录并生成了 `.playwright-profile/`，可以使用无头模式：

```bash
./.venv/bin/python douyin_scraper.py --headless
```

常用参数：

```bash
./.venv/bin/python douyin_scraper.py \
  --headless \
  --max-posts-per-keyword 20 \
  --max-comments-per-post 50 \
  --output-dir output
```

## 说明

- 脚本默认读取当前目录的 `keywords.txt`。
- 脚本会优先复用系统已安装的 Chrome；找不到时才使用 Playwright 自带 Chromium。
- 脚本默认会读取本机当前活跃的 Chrome profile，并导入其中的 `douyin.com` cookies。
- 浏览器登录态保存在 `.playwright-profile/`，后续运行会复用。
- 抖音网页端在无头模式下很容易进入验证码页；首次运行建议不要带 `--headless`。
- 如果触发登录、验证码或风控，脚本会提示你在浏览器里手动完成，再继续执行。
- 导出文件默认保存在 `output/posts.csv` 和 `output/comments.csv`。
