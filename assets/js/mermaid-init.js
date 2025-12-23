document.addEventListener("DOMContentLoaded", function () {
  if (window.mermaid && typeof window.mermaid.initialize === "function") {
    window.mermaid.initialize({ startOnLoad: true });
  }
});
