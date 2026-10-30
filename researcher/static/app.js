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
});
