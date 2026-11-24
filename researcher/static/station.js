document.addEventListener("DOMContentLoaded", () => {
  const visit = document.getElementById("reading-visit");
  if (visit) {
    fetch(visit.action, {method: "POST", body: new FormData(visit), credentials: "same-origin"})
      .then((response) => {
        if (!response.ok) throw new Error("阅读进度保存失败");
      }).catch(() => {
        const notice = document.createElement("p");
        notice.setAttribute("role", "status");
        notice.className = "alert tone-warning";
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
  const showMessage = (text) => {
    const message = document.createElement("p");
    message.className = "graph-empty";
    message.textContent = text;
    container.replaceChildren(message);
  };
  fetch("/map/data").then((response) => response.json()).then(({entities, relations}) => {
    if (!entities.length) {
      showMessage("尚无关系节点。深度研究保存有原文依据的关系后会显示。");
      return;
    }
    const groups = ["应用场景", "厂商", "产品", "技术路线", "软件生态"];
    const kindKeys = {"应用场景": "scenario", "厂商": "company", "产品": "product", "技术路线": "route", "软件生态": "software"};
    const column = 220;
    const nodeWidth = 176;
    const nodeHeight = 44;
    const rowHeight = 60;
    const top = 64;
    const positions = new Map();
    const counts = new Map(groups.map((name) => [name, 0]));
    for (const item of entities) {
      const index = Math.max(0, groups.indexOf(item.kind));
      const row = counts.get(groups[index]);
      counts.set(groups[index], row + 1);
      positions.set(item.id, {x: 24 + index * column, y: top + row * rowHeight});
    }
    const width = 24 * 2 + (groups.length - 1) * column + nodeWidth;
    const height = Math.max(220, top + Math.max(...counts.values()) * rowHeight + 12);
    const ns = "http://www.w3.org/2000/svg";
    const create = (name, attributes) => {
      const element = document.createElementNS(ns, name);
      for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
      return element;
    };
    const svg = create("svg", {viewBox: `0 0 ${width} ${height}`, width, height, class: "graph-canvas"});
    groups.forEach((name, index) => {
      const x = 24 + index * column;
      svg.append(create("rect", {x: x - 10, y: 14, width: nodeWidth + 20, height: height - 22, rx: 12, class: "graph-lane"}));
      const heading = create("text", {x, y: 40, class: "map-heading"});
      heading.textContent = `${name} · ${counts.get(name)}`;
      svg.append(heading);
    });
    const curve = (from, to) => {
      const fromY = from.y + nodeHeight / 2;
      const toY = to.y + nodeHeight / 2;
      if (from.x === to.x) {
        const x = from.x + nodeWidth;
        return `M ${x} ${fromY} C ${x + 38} ${fromY}, ${x + 38} ${toY}, ${x} ${toY}`;
      }
      const forward = to.x > from.x;
      const startX = forward ? from.x + nodeWidth : from.x;
      const endX = forward ? to.x : to.x + nodeWidth;
      const middle = (startX + endX) / 2;
      return `M ${startX} ${fromY} C ${middle} ${fromY}, ${middle} ${toY}, ${endX} ${toY}`;
    };
    const edges = [];
    for (const relation of relations) {
      const from = positions.get(relation.from_entity_id);
      const to = positions.get(relation.to_entity_id);
      if (!from || !to) continue;
      const line = create("path", {d: curve(from, to), pathLength: 1, class: "graph-edge", "data-from": relation.from_entity_id, "data-to": relation.to_entity_id});
      const title = document.createElementNS(ns, "title");
      title.textContent = `${relation.relation_type}：${relation.explanation}`;
      line.append(title);
      edges.push(line);
      svg.append(line);
    }
    const nodes = [];
    for (const item of entities) {
      const point = positions.get(item.id);
      const link = create("a", {href: `/entities/${item.id}`, class: `graph-node kind-${kindKeys[item.kind] || "other"}`, "data-id": item.id});
      const box = create("rect", {x: point.x, y: point.y, width: nodeWidth, height: nodeHeight, rx: 10, class: "graph-node-box"});
      const marker = create("circle", {cx: point.x + 16, cy: point.y + nodeHeight / 2, r: 4, class: "graph-node-dot"});
      const label = create("text", {x: point.x + 28, y: point.y + 27, class: "map-label"});
      label.textContent = item.name.length > 13 ? item.name.slice(0, 12) + "…" : item.name;
      const title = document.createElementNS(ns, "title");
      title.textContent = `${item.kind}：${item.name}。${item.summary || "点击阅读档案与资料"}`;
      link.append(box, marker, label, title);
      nodes.push(link);
      svg.append(link);
    }
    const highlight = (id) => {
      const related = new Set();
      svg.classList.toggle("has-focus", id !== null);
      for (const edge of edges) {
        const linked = id !== null && (edge.dataset.from === id || edge.dataset.to === id);
        edge.classList.toggle("is-related", linked);
        if (linked) related.add(edge.dataset.from).add(edge.dataset.to);
      }
      for (const node of nodes) {
        node.classList.toggle("is-focused", node.dataset.id === id);
        node.classList.toggle("is-related", related.has(node.dataset.id));
      }
    };
    for (const node of nodes) {
      node.addEventListener("pointerenter", () => highlight(node.dataset.id));
      node.addEventListener("pointerleave", () => highlight(null));
      node.addEventListener("focus", () => highlight(node.dataset.id));
      node.addEventListener("blur", () => highlight(null));
    }
    container.replaceChildren(svg);
  }).catch(() => {
    showMessage("关系图暂时无法读取，请使用下方文字列表。");
  });
});
