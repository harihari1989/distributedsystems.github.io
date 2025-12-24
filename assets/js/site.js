document.addEventListener("DOMContentLoaded", function () {
  var items = document.querySelectorAll("main > *");
  var visibleIndex = 0;
  items.forEach(function (el) {
    if (el.tagName === "SCRIPT") {
      return;
    }
    el.classList.add("stagger-in");
    el.style.animationDelay = String(visibleIndex * 60) + "ms";
    visibleIndex += 1;
  });
});
