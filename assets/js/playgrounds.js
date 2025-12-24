(function () {
  if (!window.React || !window.ReactDOM) {
    return;
  }

  var h = window.React.createElement;

  function distributeLoad(total, strategy, size) {
    var counts = new Array(size).fill(0);
    for (var i = 0; i < total; i += 1) {
      var index = 0;
      if (strategy === "round-robin") {
        index = i % size;
      } else if (strategy === "random") {
        index = Math.floor(Math.random() * size);
      } else {
        var min = Math.min.apply(null, counts);
        var candidates = [];
        for (var j = 0; j < size; j += 1) {
          if (counts[j] === min) {
            candidates.push(j);
          }
        }
        index = candidates[i % candidates.length];
      }
      counts[index] += 1;
    }
    return counts;
  }

  function LoadBalancingPlayground() {
    var _React = window.React;
    var useMemo = _React.useMemo;
    var useState = _React.useState;
    var _a = useState(120), requests = _a[0], setRequests = _a[1];
    var _b = useState("round-robin"), strategy = _b[0], setStrategy = _b[1];

    var counts = useMemo(function () {
      return distributeLoad(requests, strategy, 4);
    }, [requests, strategy]);

    var max = Math.max.apply(null, counts.concat([1]));
    var labels = ["Service A", "Service B", "Service C", "Service D"];

    return h(
      "div",
      { className: "playground" },
      h(
        "div",
        { className: "playground-controls" },
        h(
          "label",
          null,
          "Requests per interval",
          h("input", {
            type: "range",
            min: 20,
            max: 200,
            value: requests,
            onChange: function (event) {
              setRequests(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(requests))
        ),
        h(
          "label",
          null,
          "Strategy",
          h(
            "select",
            {
              value: strategy,
              onChange: function (event) {
                setStrategy(event.target.value);
              }
            },
            h("option", { value: "round-robin" }, "Round robin"),
            h("option", { value: "random" }, "Random"),
            h("option", { value: "least" }, "Least connections")
          )
        )
      ),
      h(
        "div",
        { className: "playground-output" },
        h(
          "div",
          { className: "playground-bars" },
          counts.map(function (value, index) {
            var width = (value / max) * 100;
            return h(
              "div",
              { className: "playground-bar", key: labels[index] },
              h("div", null, labels[index]),
              h("div", {
                className: "playground-bar-fill",
                style: { width: String(width) + "%" }
              }),
              h("div", null, String(value))
            );
          })
        ),
        h(
          "p",
          { className: "playground-note" },
          "Compare how strategies react to the same request volume and see how uneven load appears."
        )
      )
    );
  }

  function RetryStormPlayground() {
    var _React = window.React;
    var useState = _React.useState;
    var _a = useState(200), base = _a[0], setBase = _a[1];
    var _b = useState(20), failure = _b[0], setFailure = _b[1];
    var _c = useState(2), retries = _c[0], setRetries = _c[1];

    var failureRate = failure / 100;
    var attempts = 1;
    if (failureRate >= 1) {
      attempts = retries + 1;
    } else if (failureRate > 0) {
      attempts = (1 - Math.pow(failureRate, retries + 1)) / (1 - failureRate);
    }
    var effective = base * attempts;
    var multiplier = effective / base;
    var width = Math.min(multiplier / 3, 1) * 100;

    return h(
      "div",
      { className: "playground" },
      h(
        "div",
        { className: "playground-controls" },
        h(
          "label",
          null,
          "Base requests per second",
          h("input", {
            type: "range",
            min: 50,
            max: 500,
            value: base,
            onChange: function (event) {
              setBase(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(base))
        ),
        h(
          "label",
          null,
          "Failure rate (%)",
          h("input", {
            type: "range",
            min: 0,
            max: 80,
            value: failure,
            onChange: function (event) {
              setFailure(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(failure) + "%")
        ),
        h(
          "label",
          null,
          "Max retries",
          h("input", {
            type: "range",
            min: 0,
            max: 5,
            value: retries,
            onChange: function (event) {
              setRetries(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(retries))
        )
      ),
      h(
        "div",
        { className: "playground-output" },
        h(
          "div",
          { className: "playground-metrics" },
          h("div", null, "Expected attempts per request: ", h("span", null, attempts.toFixed(2))),
          h("div", null, "Effective load: ", h("span", null, Math.round(effective)), " rps"),
          h("div", null, "Retry amplification: ", h("span", null, multiplier.toFixed(2) + "x"))
        ),
        h(
          "div",
          { className: "playground-meter" },
          h("div", {
            className: "playground-meter-fill",
            style: { width: String(width) + "%" }
          })
        ),
        h(
          "p",
          { className: "playground-note" },
          "Higher failure rates combined with aggressive retries can multiply load and deepen outages."
        )
      )
    );
  }

  function HpaPlayground() {
    var _React = window.React;
    var useState = _React.useState;
    var rpsState = useState(1200);
    var rps = rpsState[0];
    var setRps = rpsState[1];
    var capacityState = useState(250);
    var capacity = capacityState[0];
    var setCapacity = capacityState[1];
    var maxPodsState = useState(16);
    var maxPods = maxPodsState[0];
    var setMaxPods = maxPodsState[1];
    var minPods = 2;

    var desired = Math.ceil(rps / capacity);
    var status = "Within limits";
    if (desired < minPods) {
      desired = minPods;
      status = "Below min, holding";
    }
    if (desired > maxPods) {
      desired = maxPods;
      status = "Capped at max";
    }

    return h(
      "div",
      { className: "playground" },
      h(
        "div",
        { className: "playground-controls" },
        h(
          "label",
          null,
          "Requests per second",
          h("input", {
            type: "range",
            min: 200,
            max: 4000,
            value: rps,
            onChange: function (event) {
              setRps(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(rps))
        ),
        h(
          "label",
          null,
          "Capacity per pod (rps)",
          h("input", {
            type: "range",
            min: 100,
            max: 500,
            value: capacity,
            onChange: function (event) {
              setCapacity(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(capacity))
        ),
        h(
          "label",
          null,
          "Max pods",
          h("input", {
            type: "range",
            min: 4,
            max: 30,
            value: maxPods,
            onChange: function (event) {
              setMaxPods(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(maxPods))
        )
      ),
      h(
        "div",
        { className: "playground-output" },
        h(
          "div",
          { className: "playground-metrics" },
          h("div", null, "Min pods: ", h("span", null, String(minPods))),
          h("div", null, "Desired pods: ", h("span", null, String(desired))),
          h("div", null, "Scaling status: ", h("span", null, status))
        ),
        h(
          "div",
          { className: "playground-pods" },
          Array.from({ length: maxPods }).map(function (_, index) {
            var className = index < desired ? "playground-pod" : "playground-pod is-idle";
            return h("div", { className: className, key: String(index) });
          })
        ),
        h(
          "p",
          { className: "playground-note" },
          "Autoscaling needs accurate signals and sane limits to avoid oscillation or saturation."
        )
      )
    );
  }

  function distributeShards(total, shards, skewPercent) {
    var weights = new Array(shards).fill(1);
    var skew = skewPercent / 100;
    weights[0] = 1 + skew * (shards - 1);
    var sum = weights.reduce(function (acc, value) {
      return acc + value;
    }, 0);
    var counts = weights.map(function (weight) {
      return Math.round((weight / sum) * total);
    });
    var diff = total - counts.reduce(function (acc, value) {
      return acc + value;
    }, 0);
    if (diff !== 0) {
      counts[0] += diff;
    }
    return counts;
  }

  function ShardSkewPlayground() {
    var _React = window.React;
    var useState = _React.useState;
    var _a = useState(4), shards = _a[0], setShards = _a[1];
    var _b = useState(30), skew = _b[0], setSkew = _b[1];
    var _c = useState(1500), requests = _c[0], setRequests = _c[1];

    var counts = distributeShards(requests, shards, skew);
    var max = Math.max.apply(null, counts.concat([1]));

    return h(
      "div",
      { className: "playground" },
      h(
        "div",
        { className: "playground-controls" },
        h(
          "label",
          null,
          "Shards",
          h("input", {
            type: "range",
            min: 2,
            max: 8,
            value: shards,
            onChange: function (event) {
              setShards(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(shards))
        ),
        h(
          "label",
          null,
          "Skew (% toward shard 1)",
          h("input", {
            type: "range",
            min: 0,
            max: 90,
            value: skew,
            onChange: function (event) {
              setSkew(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(skew) + "%")
        ),
        h(
          "label",
          null,
          "Requests per interval",
          h("input", {
            type: "range",
            min: 500,
            max: 3000,
            value: requests,
            onChange: function (event) {
              setRequests(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(requests))
        )
      ),
      h(
        "div",
        { className: "playground-output" },
        h(
          "div",
          { className: "playground-bars" },
          counts.map(function (value, index) {
            var width = (value / max) * 100;
            return h(
              "div",
              { className: "playground-bar", key: String(index) },
              h("div", null, "Shard " + String(index + 1)),
              h("div", {
                className: "playground-bar-fill",
                style: { width: String(width) + "%" }
              }),
              h("div", null, String(value))
            );
          })
        ),
        h(
          "p",
          { className: "playground-note" },
          "Skewed keys create hot shards and force rebalancing or key redesign."
        )
      )
    );
  }

  function QuorumPlayground() {
    var _React = window.React;
    var useEffect = _React.useEffect;
    var useState = _React.useState;
    var _a = useState(3), replicas = _a[0], setReplicas = _a[1];
    var _b = useState(2), read = _b[0], setRead = _b[1];
    var _c = useState(2), write = _c[0], setWrite = _c[1];

    useEffect(
      function () {
        if (read > replicas) {
          setRead(replicas);
        }
        if (write > replicas) {
          setWrite(replicas);
        }
      },
      [replicas, read, write]
    );

    var overlap = read + write - replicas;
    var isOverlap = overlap > 0;
    var width = Math.max(0, overlap) / replicas * 100;

    return h(
      "div",
      { className: "playground" },
      h(
        "div",
        { className: "playground-controls" },
        h(
          "label",
          null,
          "Replicas (N)",
          h("input", {
            type: "range",
            min: 3,
            max: 7,
            value: replicas,
            onChange: function (event) {
              setReplicas(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(replicas))
        ),
        h(
          "label",
          null,
          "Read quorum (R)",
          h("input", {
            type: "range",
            min: 1,
            max: replicas,
            value: read,
            onChange: function (event) {
              setRead(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(read))
        ),
        h(
          "label",
          null,
          "Write quorum (W)",
          h("input", {
            type: "range",
            min: 1,
            max: replicas,
            value: write,
            onChange: function (event) {
              setWrite(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(write))
        )
      ),
      h(
        "div",
        { className: "playground-output" },
        h(
          "div",
          { className: "playground-metrics" },
          h("div", null, "Overlap status: ", h("span", null, isOverlap ? "Overlapping" : "Non-overlapping")),
          h("div", null, "Max read failures: ", h("span", null, String(Math.max(0, replicas - read)))),
          h("div", null, "Max write failures: ", h("span", null, String(Math.max(0, replicas - write))))
        ),
        h(
          "div",
          { className: "playground-meter" },
          h("div", {
            className: "playground-meter-fill",
            style: { width: String(width) + "%" }
          })
        ),
        h(
          "p",
          { className: "playground-note" },
          "Use R + W > N for overlapping quorums and stronger read-after-write guarantees."
        )
      )
    );
  }

  function RateLimitPlayground() {
    var _React = window.React;
    var useState = _React.useState;
    var _a = useState(900), incoming = _a[0], setIncoming = _a[1];
    var _b = useState(700), limit = _b[0], setLimit = _b[1];
    var _c = useState(200), burst = _c[0], setBurst = _c[1];

    var allowed = Math.min(incoming, limit + burst);
    var throttled = Math.max(0, incoming - allowed);
    var max = Math.max(incoming, 1);

    return h(
      "div",
      { className: "playground" },
      h(
        "div",
        { className: "playground-controls" },
        h(
          "label",
          null,
          "Incoming rps",
          h("input", {
            type: "range",
            min: 100,
            max: 2000,
            value: incoming,
            onChange: function (event) {
              setIncoming(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(incoming))
        ),
        h(
          "label",
          null,
          "Limit rps",
          h("input", {
            type: "range",
            min: 100,
            max: 1500,
            value: limit,
            onChange: function (event) {
              setLimit(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(limit))
        ),
        h(
          "label",
          null,
          "Burst budget",
          h("input", {
            type: "range",
            min: 0,
            max: 600,
            value: burst,
            onChange: function (event) {
              setBurst(parseInt(event.target.value, 10));
            }
          }),
          h("span", null, String(burst))
        )
      ),
      h(
        "div",
        { className: "playground-output" },
        h(
          "div",
          { className: "playground-bars" },
          h(
            "div",
            { className: "playground-bar" },
            h("div", null, "Allowed"),
            h("div", {
              className: "playground-bar-fill",
              style: { width: String((allowed / max) * 100) + "%" }
            }),
            h("div", null, String(allowed))
          ),
          h(
            "div",
            { className: "playground-bar" },
            h("div", null, "Throttled"),
            h("div", {
              className: "playground-bar-fill",
              style: { width: String((throttled / max) * 100) + "%" }
            }),
            h("div", null, String(throttled))
          )
        ),
        h(
          "p",
          { className: "playground-note" },
          "Burst budgets absorb short spikes, but sustained overload still triggers throttling."
        )
      )
    );
  }

  function mount(id, Component) {
    var node = document.getElementById(id);
    if (!node) {
      return;
    }
    if (window.ReactDOM.createRoot) {
      window.ReactDOM.createRoot(node).render(h(Component));
    } else {
      window.ReactDOM.render(h(Component), node);
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    mount("playground-load-balancing-root", LoadBalancingPlayground);
    mount("playground-retries-root", RetryStormPlayground);
    mount("playground-hpa-root", HpaPlayground);
    mount("playground-sharding-root", ShardSkewPlayground);
    mount("playground-quorums-root", QuorumPlayground);
    mount("playground-rate-limits-root", RateLimitPlayground);
  });
})();
