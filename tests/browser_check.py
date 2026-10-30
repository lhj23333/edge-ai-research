import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".work" / "browser"
WORK.mkdir(parents=True, exist_ok=True)
os.environ["RESEARCHER_DATA_DIR"] = str(WORK / "data")
os.environ["TMPDIR"] = str(WORK)
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
URL = "http://127.0.0.1:18765"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--paths", nargs="*", default=["/", "/updates", "/map", "/companies", "/compare", "/evidence", "/questions", "/personal", "/watchlist", "/settings", "/tasks", "/library"])
    arguments = parser.parse_args()
    base_url = "http://127.0.0.1:8765" if arguments.live else URL
    server = None if arguments.live else subprocess.Popen([sys.executable, "-m", "researcher.cli", "run", "--port", "18765"], cwd=ROOT, env=os.environ.copy())
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
                # 生产首页检查只读取页面，保留用户的阅读进度。
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
            page.get_by_role("button", name="检查关注范围").click()
            assert page.get_by_role("alert").inner_text() == "请在设置中添加并选择模型配置"
            page.goto(URL + "/settings")
            select = page.locator("[data-provider-select]")
            select.select_option("deepseek")
            assert page.locator("input[name='base_url']").input_value() == "https://api.deepseek.com/v1"
            page.goto(URL + "/library")
            page.locator("input[name='q']").fill("独立 AI 加速器")
            page.get_by_role("button", name="搜索", exact=False).click()
            page.wait_for_url("**/library?q=*")
            assert page.get_by_role("heading", name="没有找到相关成果").count() == 1
            for width in (1440, 390):
                page.set_viewport_size({"width": width, "height": 900})
                for path in ("/", "/updates", "/map", "/companies", "/compare", "/evidence", "/questions", "/personal", "/watchlist", "/settings", "/tasks", "/library"):
                    page.goto(URL + path)
                    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), (width, path)
            assert errors == [], errors
            browser.close()
        print("真实浏览器验证通过：关注专题、认知地图、个人批注、采集配置提示、服务商预设及桌面和移动页面布局。")
    finally:
        if server:
            server.terminate()
            server.wait(timeout=25)


if __name__ == "__main__":
    main()
