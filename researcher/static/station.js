document.addEventListener("DOMContentLoaded", () => {
  const visit = document.getElementById("reading-visit");
  if (visit) {
    fetch(visit.action, {method: "POST", body: new FormData(visit), credentials: "same-origin"})
      .then((response) => {
        if (!response.ok) throw new Error("阅读进度保存失败");
      }).catch(() => {
        const notice = document.createElement("p");
        notice.setAttribute("role", "status");
        notice.textContent = "本次阅读进度未能保存，补读入口将继续保留。";
        visit.insertAdjacentElement("beforebegin", notice);
      });
  }

  const targetKind = document.getElementById("question-target-kind");
  if (targetKind) {
    const target = targetKind.form.elements.target_id;
    const updateTargets = () => {
      for (const option of target.options) {
        if (option.value) option.hidden = option.dataset.kind !== targetKind.value;
      }
      if (target.selectedOptions[0]?.hidden) target.value = "";
    };
    targetKind.addEventListener("change", updateTargets);
    updateTargets();
  }

  const container = document.getElementById("knowledge-graph");
  if (!container) return;
  fetch("/map/data").then((response) => response.json()).then(({entities, relations}) => {
    if (!entities.length) {
      container.textContent = "尚无关系节点。确认关注名单并完成资料采集后会显示。";
      return;
    }
    const groups = ["应用场景", "厂商", "产品", "技术路线", "软件生态"];
    const positions = new Map();
    const counts = new Map(groups.map((name) => [name, 0]));
    const width = 1120;
    for (const item of entities) {
      const column = Math.max(0, groups.indexOf(item.kind));
      const row = counts.get(groups[column]);
      counts.set(groups[column], row + 1);
      positions.set(item.id, {x: 28 + column * 220, y: 82 + row * 62});
    }
    const height = Math.max(220, 130 + Math.max(...counts.values()) * 62);
    const ns = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(ns, "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("width", width);
    svg.setAttribute("height", height);
    for (let i = 0; i < groups.length; i++) {
      const heading = document.createElementNS(ns, "text");
      heading.setAttribute("x", 28 + i * 220);
      heading.setAttribute("y", 32);
      heading.setAttribute("class", "map-heading");
      heading.textContent = groups[i];
      svg.append(heading);
    }
    for (const relation of relations) {
      const from = positions.get(relation.from_entity_id);
      const to = positions.get(relation.to_entity_id);
      if (!from || !to) continue;
      const line = document.createElementNS(ns, "path");
      line.setAttribute("d", `M ${from.x + 170} ${from.y + 21} C ${(from.x + to.x) / 2 + 100} ${from.y + 21}, ${(from.x + to.x) / 2 + 100} ${to.y + 21}, ${to.x} ${to.y + 21}`);
      line.setAttribute("fill", "none");
      line.setAttribute("stroke", "#9cbdac");
      line.setAttribute("stroke-width", "2");
      const title = document.createElementNS(ns, "title");
      title.textContent = `${relation.relation_type}：${relation.explanation}`;
      line.append(title);
      svg.append(line);
    }
    for (const item of entities) {
      const point = positions.get(item.id);
      const link = document.createElementNS(ns, "a");
      link.setAttribute("href", `/entities/${item.id}`);
      const rect = document.createElementNS(ns, "rect");
      for (const [key, value] of Object.entries({x: point.x, y: point.y, width: 175, height: 43, rx: 10})) rect.setAttribute(key, value);
      rect.setAttribute("fill", "#fff");
      rect.setAttribute("stroke", "#b9d0bf");
      const label = document.createElementNS(ns, "text");
      label.setAttribute("x", point.x + 10);
      label.setAttribute("y", point.y + 27);
      label.setAttribute("class", "map-label");
      label.textContent = item.name.length > 14 ? item.name.slice(0, 13) + "…" : item.name;
      const title = document.createElementNS(ns, "title");
      title.textContent = `${item.kind}：${item.name}。${item.summary || "点击阅读档案与资料"}`;
      link.append(rect, label, title);
      svg.append(link);
    }
    container.append(svg);
  }).catch(() => {
    container.textContent = "关系图暂时无法读取，请使用下方文字列表。";
  });
});
