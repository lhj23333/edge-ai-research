import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".work"
URL = "http://127.0.0.1:18765"
PAGES = ("/", "/updates", "/map", "/companies", "/compare", "/evidence", "/questions", "/research",
         "/personal", "/watchlist", "/settings", "/tasks", "/topics", "/library",
         "/admin/sources", "/admin/funnel", "/admin/eval")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--paths", nargs="*", default=PAGES)
    arguments = parser.parse_args()
    WORK.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="b-", dir=WORK) as directory:
        os.environ["TMPDIR"] = directory
        environment = dict(os.environ, RESEARCHER_DATA_DIR=str(Path(directory) / "data"), PYTHONDONTWRITEBYTECODE="1")
        check_browser(arguments, environment)


def check_browser(arguments, environment):
    base_url = "http://127.0.0.1:8765" if arguments.live else URL
    server = None if arguments.live else subprocess.Popen([sys.executable, "-m", "researcher.cli", "run", "--port", "18765"], cwd=ROOT, env=environment)
    try:
        with httpx.Client(trust_env=False) as client:
            for _ in range(60):
                if server and server.poll() is not None:
                    raise RuntimeError("网站验证服务提前退出")
                try:
                    response = client.get(base_url + "/health")
                    if response.status_code == 200 and response.json()["worker"] == "running":
                        break
                except httpx.ConnectError:
                    pass
                time.sleep(0.25)
            else:
                raise RuntimeError("网站启动超时")
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(executable_path=os.environ.get("RESEARCHER_CHROME", "/usr/bin/google-chrome"), headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(8000)
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            if arguments.live:
                # 关闭生产首页脚本以保留阅读进度。
                home_context = browser.new_context(java_script_enabled=False)
                home_page = home_context.new_page()
                for width in (1440, 390):
                    page.set_viewport_size({"width": width, "height": 900})
                    home_page.set_viewport_size({"width": width, "height": 900})
                    for path in arguments.paths:
                        active_page = home_page if path == "/" else page
                        response = active_page.goto(base_url + path)
                        assert response.status == 200, path
                        assert active_page.locator("main h1").count() == 1, path
                        assert active_page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), (width, path)
                        if path == "/map":
                            graph = page.request.get(base_url + "/map/data").json()
                            if graph["entities"]:
                                page.locator("#knowledge-graph svg").wait_for()
                                assert page.locator("#knowledge-graph svg a").count() == len(graph["entities"])
                        if path.startswith("/questions?") and "action=" in path:
                            assert page.locator("textarea[name='main_question']").input_value()
                            assert page.locator("input[name='note_id']:checked").count() == 0
                assert errors == [], errors
                home_context.close()
                browser.close()
                print("真实研究内容页面验证通过：", arguments.paths, flush=True)
                return
            page.goto(URL)
            assert page.get_by_role("heading", name="今日简报").count() == 1
            assert page.get_by_text("手动维护中 · 自动更新待配置").count() >= 1
            page.goto(URL + "/watchlist")
            page.locator("form[action='/watchlist/add'] select[name='kind']").select_option("专题")
            page.locator("form[action='/watchlist/add'] input[name='name']").fill("独立 AI 加速器领域概览")
            page.locator("form[action='/watchlist/add'] textarea[name='question']").fill("梳理端侧推理独立加速器的厂商、产品、软件与应用场景。")
            page.get_by_role("button", name="加入关注").click()
            assert page.get_by_text("专题 · 独立 AI 加速器领域概览").count() >= 1
            page.goto(URL + "/map")
            page.locator("a[href^='/knowledge/']").first.click()
            page.get_by_text("写个人批注").click()
            page.locator("form[action='/annotations'] textarea[name='body']").fill("希望持续核查软件迁移对产品竞争力的影响。")
            page.get_by_role("button", name="保存批注").click()
            assert page.get_by_text("批注已保存到个人区域").count() == 1
            page.goto(URL + "/personal")
            assert page.get_by_text("希望持续核查软件迁移对产品竞争力的影响。").count() >= 1
            page.goto(URL + "/updates")
            page.get_by_role("button", name="采集启用信源").click()
            expect(page.get_by_role("alert")).to_have_text("请在信源管理中启用采集源，或提交一批待处理条目")
            check_event_and_map(page)
            page.keyboard.press("Control+k")
            expect(page.get_by_role("dialog", name="快速跳转")).to_be_visible()
            page.locator("[data-command-input]").fill("Hailo")
            page.locator("[data-command-search='/evidence']").click()
            page.wait_for_url("**/evidence?q=Hailo")
            expect(page.get_by_role("heading", name="证据管理")).to_be_visible()
            page.goto(URL + "/settings")
            select = page.locator("[data-provider-select]")
            select.select_option("deepseek")
            assert page.locator("input[name='base_url']").input_value() == "https://api.deepseek.com/v1"
            page.goto(URL + "/library")
            page.locator("input[name='q']").fill("独立 AI 加速器")
            with page.expect_response(lambda response: "/library?q=" in response.url) as fragment:
                page.get_by_role("button", name="搜索", exact=True).click()
            page.wait_for_url("**/library?q=*")
            assert fragment.value.status == 200
            assert fragment.value.request.header_value("hx-request") == "true"
            assert fragment.value.request.header_value("hx-target") == "library-results"
            expect(page.locator("#library-results").get_by_role("heading", name="没有找到相关成果")).to_be_visible()
            assert page.locator("main").count() == 1
            check_admin(page)
            for width in (1440, 390):
                page.set_viewport_size({"width": width, "height": 900})
                for path in arguments.paths:
                    response = page.goto(URL + path)
                    assert response.status == 200, path
                    assert page.locator("main h1").count() == 1, path
                    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), (width, path)
            assert errors == [], errors
            browser.close()
        print(f"Chromium 验收通过。{len(arguments.paths)} 个页面通过桌面和移动检查。专题、事件、认知地图、批注、采集提示、命令面板、HTMX 和三个后台均通过。JS pageerror {len(errors)} 个。")
    finally:
        if server:
            server.terminate()
            server.wait(timeout=25)


def check_event_and_map(page):
    page.get_by_text("人工补充行业动态", exact=True).click()
    manual = page.locator("form[action='/updates/manual']")
    original = "https://www.raspberrypi.com/news/introducing-the-raspberry-pi-ai-hat-plus-2-generative-ai-on-raspberry-pi-5/"
    values = {
        "company": "Hailo",
        "title": "Raspberry Pi AI HAT+ 2",
        "what_happened": "Raspberry Pi 发布搭载 Hailo-10H 的 AI HAT+ 2。",
        "why_important": "板卡支持在 Raspberry Pi 5 上运行生成式模型。",
        "published_at": "2026-01-15",
        "url": original,
        "excerpt": "Featuring the new Hailo-10H neural network accelerator, the Raspberry Pi AI HAT+ 2 delivers 40 TOPS (INT4) of inferencing performance",
    }
    for name, value in values.items():
        manual.locator(f"[name='{name}']").fill(value)
    manual.get_by_role("button", name="保存为待核实事件").click()
    page.wait_for_url("**/events/*")
    verify = page.get_by_role("button", name="针对本事件发起 LangGraph 深度核验")
    with page.expect_response(lambda response: response.url.endswith("/research") and response.request.method == "POST") as submitted:
        verify.click()
    assert submitted.value.status == 303
    expect(page.get_by_role("alert")).to_have_text("请在设置中添加并选择模型配置")

    page.goto(URL + "/map")
    graph = page.request.get(URL + "/map/data")
    assert graph.ok
    expect(page.locator("#knowledge-graph svg a")).to_have_count(len(graph.json()["entities"]))
    page.locator("#knowledge-graph svg a").filter(has_text="Hailo").click()
    expect(page.get_by_role("heading", name="Hailo", exact=True)).to_be_visible()

    page.goto(URL + "/topics")
    page.get_by_role("link", name="独立 AI 加速器领域概览").click()
    expect(page.get_by_text("历史定时计划已停用", exact=False)).to_be_visible()
    page.get_by_role("button", name="保存计划", exact=True).click()
    expect(page.get_by_role("status")).to_contain_text("专题定向更新计划已保存")
    page.get_by_role("button", name="启用计划", exact=True).click()
    expect(page.get_by_role("button", name="停用计划", exact=True)).to_be_visible()
    page.get_by_role("button", name="停用计划", exact=True).click()
    expect(page.get_by_role("status")).to_contain_text("专题定向更新计划已停用")
    page.get_by_role("button", name="发起专题深度研究").click()
    expect(page.get_by_role("alert")).to_have_text("请在设置中添加并选择模型配置")


def check_admin(page):
    page.goto(URL + "/admin/sources")
    source = page.locator("form[action='/admin/sources']")
    for name, value in {"key": "browser-source", "name": "浏览器预览", "publisher": "browser-source"}.items():
        source.locator(f"[name='{name}']").fill(value)
    source.locator("[name='kind']").select_option("external")
    source.get_by_text("列表解析与推送预览", exact=True).click()
    source.locator("[name='entries']").fill(json.dumps([{
        "url": "https://www.raspberrypi.com/news/introducing-the-raspberry-pi-ai-hat-plus-2-generative-ai-on-raspberry-pi-5/",
        "title": "Introducing the Raspberry Pi AI HAT+ 2",
        "published_at": "2026-01-15",
    }]))
    with page.expect_response(lambda response: response.url.endswith("/admin/sources/preview") and response.request.method == "POST") as submitted:
        source.get_by_role("button", name="预览前 20 条").click()
    assert submitted.value.status == 200
    preview = page.locator("#preview")
    expect(preview.get_by_role("heading", name="预览结果")).to_be_visible()
    expect(preview.get_by_role("link", name="Introducing the Raspberry Pi AI HAT+ 2")).to_be_visible()
    preview_time = datetime(2026, 1, 15, tzinfo=timezone.utc)
    assert preview_time.date().isoformat() in preview.inner_text()
    page.get_by_role("link", name="漏斗诊断").first.click()
    expect(page.get_by_role("heading", name="漏斗诊断")).to_be_visible()
    page.goto(URL + "/admin/eval")
    expect(page.get_by_role("heading", name="开发集")).to_be_visible()
    expect(page.get_by_role("heading", name="留出集")).to_be_visible()
    page.get_by_text("已标注样本", exact=True).first.click()
    expect(page.get_by_role("link", name="Hailo-10H AI Accelerator", exact=True)).to_be_visible()


if __name__ == "__main__":
    main()
