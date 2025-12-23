(function () {
  var root = document.documentElement;
  var stored = localStorage.getItem("color-mode");

  if (!root.hasAttribute("data-color-mode")) {
    var prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    root.setAttribute("data-color-mode", stored || (prefersDark ? "dark" : "light"));
  } else if (stored) {
    root.setAttribute("data-color-mode", stored);
  }

  window.toggleColorMode = function () {
    var next = root.getAttribute("data-color-mode") === "dark" ? "light" : "dark";
    root.setAttribute("data-color-mode", next);
    localStorage.setItem("color-mode", next);
  };
})();
