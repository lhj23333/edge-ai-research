document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("[data-provider-select]").forEach((select) => {
    select.addEventListener("change", () => {
      const option = select.selectedOptions[0];
      const form = select.closest("form");
      form.elements.base_url.value = option.dataset.base;
      form.elements.model.value = option.dataset.model;
    });
  });
  document.querySelectorAll("form[method='post']").forEach((form) => {
    form.addEventListener("submit", () => {
      form.querySelectorAll("button[type='submit']").forEach((button) => {
        button.disabled = true;
        button.setAttribute("aria-busy", "true");
      });
    });
  });
  // 浏览器后退恢复缓存页面时，提交中的按钮需要重新可用
  window.addEventListener("pageshow", (event) => {
    if (!event.persisted) return;
    document.querySelectorAll("button[aria-busy='true']").forEach((button) => {
      button.disabled = false;
      button.removeAttribute("aria-busy");
    });
  });
  const panel = document.querySelector("[data-task-events]");
  if (panel) {
    const events = new EventSource(panel.dataset.taskEvents);
    let completed = false;
    events.addEventListener("task", (event) => {
      const task = JSON.parse(event.data);
      document.getElementById("task-progress").textContent = task.progress;
      if (!["queued", "running"].includes(task.status) && !completed) {
        completed = true;
        events.close();
        window.location.reload();
      }
    });
    window.addEventListener("pagehide", () => events.close());
  }

  const root = document.documentElement;
  document.querySelectorAll("[data-sidebar-toggle]").forEach((button) => {
    const sync = () => {
      const collapsed = root.classList.contains("sidebar-collapsed");
      const label = collapsed ? "展开侧边栏" : "收起侧边栏";
      button.setAttribute("aria-label", label);
      button.setAttribute("aria-expanded", String(!collapsed));
      button.title = label;
    };
    sync();
    button.addEventListener("click", () => {
      const collapsed = root.classList.toggle("sidebar-collapsed");
      localStorage.setItem("researcher.sidebar", collapsed ? "collapsed" : "expanded");
      sync();
    });
  });

  const palette = document.getElementById("command-palette");
  if (palette) {
    const input = palette.querySelector("[data-command-input]");
    const pages = [...palette.querySelectorAll("[data-command-item]")];
    const scopes = [...palette.querySelectorAll("[data-command-search]")];
    const pageSection = palette.querySelector("[data-command-section='pages']");
    const scopeSection = palette.querySelector("[data-command-section='scopes']");
    const emptyMessage = palette.querySelector("[data-command-empty]");
    let active = null;
    const visibleItems = () => [...palette.querySelectorAll(".command-item:not([hidden])")];
    const setActive = (item, reveal) => {
      active?.classList.remove("is-active");
      active = item || null;
      if (!active) return;
      active.classList.add("is-active");
      if (reveal) active.scrollIntoView({block: "nearest"});
    };
    const update = () => {
      const term = input.value.trim();
      const needle = term.toLowerCase();
      let matches = 0;
      for (const item of pages) {
        const show = !needle || item.dataset.keywords.toLowerCase().includes(needle);
        item.hidden = !show;
        if (show) matches += 1;
      }
      pageSection.hidden = matches === 0;
      for (const item of scopes) {
        item.hidden = !term;
        item.href = `${item.dataset.commandSearch}?q=${encodeURIComponent(term)}`;
        item.querySelector("[data-command-term]").textContent = term;
      }
      scopeSection.hidden = !term;
      const items = visibleItems();
      emptyMessage.hidden = items.length > 0;
      setActive(items[0], true);
    };
    const open = () => {
      if (palette.open) return;
      input.value = "";
      update();
      palette.showModal();
      input.focus();
    };
    document.querySelectorAll("[data-command-open]").forEach((button) => button.addEventListener("click", open));
    document.addEventListener("keydown", (event) => {
      if (event.isComposing) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        if (palette.open) palette.close();
        else open();
        return;
      }
      const target = event.target;
      const typing = target instanceof Element && target.closest("input, textarea, select, [contenteditable='true']");
      if (event.key === "/" && !palette.open && !typing && !event.ctrlKey && !event.metaKey && !event.altKey) {
        event.preventDefault();
        open();
      }
    });
    input.addEventListener("input", update);
    input.addEventListener("keydown", (event) => {
      if (event.isComposing) return;
      const items = visibleItems();
      if ((event.key === "ArrowDown" || event.key === "ArrowUp") && items.length) {
        event.preventDefault();
        const step = event.key === "ArrowDown" ? 1 : -1;
        setActive(items[(items.indexOf(active) + step + items.length) % items.length], true);
      } else if (event.key === "Enter" && active) {
        event.preventDefault();
        active.click();
      }
    });
    palette.addEventListener("pointermove", (event) => {
      const item = event.target.closest(".command-item");
      if (item && item !== active) setActive(item, false);
    });
    palette.addEventListener("click", (event) => {
      if (event.target === palette) palette.close();
    });
  }

  document.querySelectorAll("table[data-sortable]").forEach((table) => {
    const body = table.tBodies[0];
    table.querySelectorAll("th[data-sort]").forEach((header) => {
      const index = header.cellIndex;
      const numeric = header.dataset.sort === "number";
      const button = document.createElement("button");
      button.type = "button";
      button.className = "th-sort";
      button.append(...header.childNodes);
      header.append(button);
      const value = (row) => {
        const cell = row.cells[index];
        return cell.dataset.value ?? cell.textContent.trim();
      };
      button.addEventListener("click", () => {
        const direction = header.getAttribute("aria-sort") === "ascending" ? "descending" : "ascending";
        table.querySelectorAll("th[aria-sort]").forEach((other) => other.removeAttribute("aria-sort"));
        header.setAttribute("aria-sort", direction);
        const factor = direction === "ascending" ? 1 : -1;
        const rows = [...body.rows].filter((row) => row.cells.length > index);
        rows.sort((a, b) => factor * (numeric ? Number(value(a)) - Number(value(b)) : value(a).localeCompare(value(b), "zh-CN")));
        body.append(...rows);
      });
    });
  });

  document.querySelectorAll("[data-table-filter]").forEach((input) => {
    const table = document.getElementById(input.dataset.tableFilter);
    const group = document.querySelector(`[data-table-status="${table.id}"]`);
    const empty = document.querySelector(`[data-filter-empty="${table.id}"]`);
    let status = "";
    const apply = () => {
      const needle = input.value.trim().toLowerCase();
      let shown = 0;
      for (const row of table.tBodies[0].rows) {
        const match = (!status || row.dataset.status === status) && (!needle || row.textContent.toLowerCase().includes(needle));
        row.hidden = !match;
        if (match) shown += 1;
      }
      empty.hidden = shown > 0;
    };
    input.addEventListener("input", apply);
    group?.querySelectorAll("button[data-value]").forEach((button) => {
      button.addEventListener("click", () => {
        status = button.dataset.value;
        group.querySelectorAll("button[data-value]").forEach((other) => other.setAttribute("aria-pressed", String(other === button)));
        apply();
      });
    });
  });

  document.querySelectorAll("[data-compare-picker]").forEach((form) => {
    const boxes = [...form.querySelectorAll("input[name='id']")];
    const counter = form.querySelector("[data-compare-count]");
    const sync = () => {
      const count = boxes.filter((box) => box.checked).length;
      counter.textContent = `已选择 ${count} / 3`;
      for (const box of boxes) box.disabled = !box.checked && count >= 3;
    };
    boxes.forEach((box) => box.addEventListener("change", sync));
    sync();
  });
});
