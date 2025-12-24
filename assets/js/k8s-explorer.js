(function () {
  if (!window.React || !window.ReactDOM) {
    return;
  }

  var e = window.React.createElement;
  var useState = window.React.useState;
  var useEffect = window.React.useEffect;
  var useMemo = window.React.useMemo;

  var tabs = [
    { id: "big-picture", label: "Big picture" },
    { id: "control-plane", label: "Control plane" },
    { id: "pod-lifecycle", label: "Pod lifecycle" },
    { id: "scheduler", label: "Scheduler" },
    { id: "networking", label: "Networking" },
    { id: "failure", label: "Failure and healing" },
    { id: "compare", label: "Compare" }
  ];

  function useTicker(playing, speed) {
    var _a = useState(0), tick = _a[0], setTick = _a[1];

    useEffect(
      function () {
        if (!playing) {
          return;
        }
        var interval = Math.max(250, 900 / speed);
        var handle = window.setInterval(function () {
          setTick(function (current) {
            return current + 1;
          });
        }, interval);
        return function () {
          window.clearInterval(handle);
        };
      },
      [playing, speed]
    );

    return [tick, setTick];
  }

  function Node(props) {
    var className = "k8s-node";
    if (props.status) {
      className += " is-" + props.status;
    }
    if (props.pulse) {
      className += " pulse";
    }
    if (props.pulseSlow) {
      className += " pulse-slow";
    }

    return e(
      "g",
      null,
      e("rect", {
        x: props.x,
        y: props.y,
        width: props.w,
        height: props.h,
        rx: 12,
        className: className
      }),
      e(
        "text",
        {
          x: props.x + props.w / 2,
          y: props.y + props.h / 2,
          textAnchor: "middle",
          dominantBaseline: "middle"
        },
        props.label
      )
    );
  }

  function Edge(props) {
    var className = "k8s-edge";
    if (props.flow) {
      className += " flow";
    }
    if (props.active) {
      className += " is-active";
    }
    if (props.blocked) {
      className += " is-blocked";
    }

    return e("path", { d: props.d, className: className });
  }

  function Packet(props) {
    var className = "k8s-packet";
    if (props.blocked) {
      className += " is-blocked";
    }
    if (!props.active) {
      className += " is-hidden";
    }

    return e("circle", {
      r: 4,
      className: className,
      transform: "translate(" + props.x + " " + props.y + ")"
    });
  }

  function BigPictureView(props) {
    var phase = props.phase;
    var mode = props.mode;
    var workerAStatus = "good";
    var workerBStatus = "good";
    if (mode === "failure") {
      workerBStatus = "bad";
    } else if (mode === "load") {
      workerBStatus = "warn";
    }

    var packetPath = [
      { x: 90, y: 70 },
      { x: 200, y: 70 },
      { x: 320, y: 70 },
      { x: 480, y: 70 },
      { x: 590, y: 180 }
    ];
    var packetIndex = Math.min(phase, packetPath.length - 1);
    var packet = packetPath[packetIndex];

    return e(
      "div",
      { className: "k8s-canvas" },
      e(
        "svg",
        { viewBox: "0 0 820 320" },
        e(Node, { x: 40, y: 40, w: 130, h: 60, label: "Desired", pulse: true }),
        e(Node, { x: 200, y: 40, w: 130, h: 60, label: "API Server", pulseSlow: true }),
        e(Node, { x: 360, y: 40, w: 130, h: 60, label: "etcd", status: props.stale ? "warn" : "good" }),
        e(Node, { x: 520, y: 40, w: 130, h: 60, label: "Controllers", pulse: true }),
        e(Node, { x: 680, y: 40, w: 110, h: 60, label: "Scheduler" }),
        e(Node, { x: 140, y: 180, w: 160, h: 70, label: "Worker A", status: workerAStatus }),
        e(Node, { x: 340, y: 180, w: 160, h: 70, label: "Worker B", status: workerBStatus }),
        e(Node, { x: 560, y: 180, w: 200, h: 70, label: "Actual state", pulseSlow: true }),
        e(Edge, { d: "M170 70 L200 70", flow: true, active: phase === 0 }),
        e(Edge, { d: "M330 70 L360 70", flow: true, active: phase === 1 }),
        e(Edge, { d: "M490 70 L520 70", flow: true, active: phase === 2 }),
        e(Edge, { d: "M650 70 L680 70", flow: true, active: phase === 3 }),
        e(Edge, { d: "M735 100 L420 180", flow: true, active: phase >= 3 }),
        e(Edge, { d: "M300 215 L560 215", flow: true, active: phase >= 4 }),
        e(Packet, { x: packet.x, y: packet.y, active: true })
      )
    );
  }

  function ControlPlaneView(props) {
    var phase = props.phase;
    var loopsActive = !props.paused;
    var loopPhase = loopsActive ? phase : 0;

    return e(
      "div",
      { className: "k8s-canvas" },
      e(
        "svg",
        { viewBox: "0 0 820 260" },
        e(Node, { x: 60, y: 60, w: 150, h: 70, label: "API Server", pulse: true }),
        e(Node, { x: 260, y: 40, w: 150, h: 70, label: "etcd", status: props.stale ? "warn" : "good" }),
        e(Node, { x: 260, y: 140, w: 150, h: 70, label: "Scheduler" }),
        e(Node, { x: 470, y: 90, w: 200, h: 70, label: "Controller Manager", pulseSlow: true }),
        e(Node, { x: 700, y: 90, w: 100, h: 70, label: "Node" }),
        e(Edge, { d: "M210 95 L260 75", flow: true, active: loopsActive && loopPhase === 0 }),
        e(Edge, { d: "M210 95 L260 175", flow: true, active: loopsActive && loopPhase === 1 }),
        e(Edge, { d: "M410 75 L470 125", flow: true, active: loopsActive && loopPhase === 2 }),
        e(Edge, { d: "M410 175 L470 125", flow: true, active: loopsActive && loopPhase === 3 }),
        e(Edge, { d: "M670 125 L700 125", flow: true, active: loopsActive && loopPhase >= 3 }),
        e(Packet, { x: loopPhase < 2 ? 235 : 430, y: loopPhase < 2 ? 105 : 130, active: loopsActive })
      )
    );
  }

  function PodLifecycleView(props) {
    var phase = props.phase;
    var blockedStage = null;
    var failure = "";
    if (props.schedulerDown) {
      blockedStage = 3;
      failure = "Scheduler unavailable";
    }
    if (props.nodeExhausted) {
      blockedStage = 4;
      failure = "Node resources exhausted";
    }
    if (props.imageFailure) {
      blockedStage = 5;
      failure = "Image pull failure";
    }
    var effectivePhase = blockedStage === null ? phase : Math.min(phase, blockedStage);

    var stages = [
      "kubectl apply",
      "API validation",
      "etcd write",
      "Scheduler bind",
      "kubelet sync",
      "Runtime start",
      "Pod ready"
    ];

    return e(
      "div",
      { className: "k8s-canvas" },
      e(
        "svg",
        { viewBox: "0 0 880 240" },
        stages.map(function (label, index) {
          var x = 30 + index * 120;
          var status = "good";
          if (blockedStage !== null && index >= blockedStage) {
            status = index === blockedStage ? "bad" : "warn";
          } else if (index > effectivePhase) {
            status = "warn";
          }
          return e(Node, {
            key: label,
            x: x,
            y: 70,
            w: 110,
            h: 60,
            label: label,
            status: status,
            pulse: index === effectivePhase
          });
        }),
        stages.slice(0, -1).map(function (_, index) {
          var active = index <= effectivePhase;
          return e(Edge, {
            key: "edge-" + index,
            d: "M" + (140 + index * 120) + " 100 L" + (150 + index * 120) + " 100",
            flow: true,
            active: active,
            blocked: blockedStage !== null && index >= blockedStage
          });
        }),
        e(Packet, {
          x: 85 + effectivePhase * 120,
          y: 100,
          active: true,
          blocked: blockedStage !== null
        })
      ),
      failure
        ? e("div", { className: "k8s-status" }, "Blocked: " + failure)
        : e("div", { className: "k8s-status" }, "Deployment converging asynchronously")
    );
  }

  function SchedulerView(props) {
    var nodes = [
      { id: "node-a", label: "Node A", cpu: 16, mem: 64, usedCpu: 8, usedMem: 28, tainted: false },
      { id: "node-b", label: "Node B", cpu: 8, mem: 32, usedCpu: 6, usedMem: 18, tainted: true },
      { id: "node-c", label: "Node C", cpu: 12, mem: 48, usedCpu: 4, usedMem: 16, tainted: false }
    ];

    var evaluated = nodes.map(function (node) {
      var cpuAvailable = node.cpu - node.usedCpu;
      var memAvailable = node.mem - node.usedMem;
      var fits = cpuAvailable >= props.cpuRequest && memAvailable >= props.memRequest;
      if (props.taintsEnabled && node.tainted && !props.toleration) {
        fits = false;
      }
      var utilization = ((node.usedCpu + props.cpuRequest) / node.cpu + (node.usedMem + props.memRequest) / node.mem) / 2;
      var score = props.strategy === "binpack" ? utilization : 1 - utilization;
      if (props.affinity && node.id === "node-c") {
        score = Math.min(1, score + 0.15);
      }
      score = Math.max(0, Math.min(1, score));
      return {
        node: node,
        fits: fits,
        score: fits ? Math.round(score * 100) : 0,
        cpuAvailable: cpuAvailable,
        memAvailable: memAvailable
      };
    });

    var best = evaluated
      .filter(function (item) {
        return item.fits;
      })
      .sort(function (a, b) {
        return b.score - a.score;
      })[0];

    return e(
      "div",
      { className: "k8s-node-grid" },
      evaluated.map(function (item) {
        var node = item.node;
        var className = "k8s-node-card";
        if (!item.fits) {
          className += " is-blocked";
        } else if (best && best.node.id === node.id) {
          className += " is-selected";
        }
        return e(
          "div",
          { key: node.id, className: className },
          e("div", { className: "k8s-node-title" }, node.label),
          e("div", { className: "k8s-node-meta" }, "CPU " + node.usedCpu + "/" + node.cpu + " | Mem " + node.usedMem + "/" + node.mem + " Gi"),
          e("div", { className: "k8s-score" },
            e("div", { className: "k8s-score-bar" },
              e("span", { style: { width: item.score + "%" } })
            ),
            e("div", { className: "k8s-score-label" }, item.fits ? item.score + "" : "Filtered")
          ),
          e("div", { className: "k8s-node-meta" }, item.fits ? "Available CPU: " + item.cpuAvailable + ", Mem: " + item.memAvailable + " Gi" : "Filtered by rules")
        );
      })
    );
  }

  function NetworkingView(props) {
    var phase = props.phase;
    var packetPositions = [
      { x: 120, y: 120 },
      { x: 260, y: 120 },
      { x: 420, y: 120 },
      { x: 580, y: 120 },
      { x: 700, y: 120 }
    ];
    var packet = packetPositions[Math.min(phase, packetPositions.length - 1)];
    var blocked = props.policy;

    return e(
      "div",
      { className: "k8s-canvas" },
      e(
        "svg",
        { viewBox: "0 0 800 240" },
        e(Node, { x: 40, y: 90, w: 130, h: 60, label: "Pod A", pulse: true }),
        e(Node, { x: 200, y: 90, w: 140, h: 60, label: "Service VIP" }),
        e(Node, { x: 370, y: 90, w: 160, h: 60, label: "kube-proxy", pulseSlow: true }),
        e(Node, { x: 560, y: 90, w: 150, h: 60, label: "Node B" }),
        e(Node, { x: 720, y: 90, w: 130, h: 60, label: "Pod B", status: blocked ? "bad" : "good" }),
        e(Edge, { d: "M170 120 L200 120", flow: true, active: phase >= 0, blocked: blocked }),
        e(Edge, { d: "M340 120 L370 120", flow: true, active: phase >= 1, blocked: blocked }),
        e(Edge, { d: "M530 120 L560 120", flow: true, active: phase >= 2, blocked: blocked }),
        e(Edge, { d: "M710 120 L720 120", flow: true, active: phase >= 3, blocked: blocked }),
        e(Packet, { x: packet.x, y: packet.y, active: !blocked, blocked: blocked })
      ),
      e("div", { className: "k8s-status" }, "Mode: " + props.mode + (blocked ? " | NetworkPolicy blocking" : " | Policy open"))
    );
  }

  function FailureView(props) {
    var phase = props.phase;
    var scenario = props.scenario;
    var status = {
      pod: "good",
      node: "good",
      api: "good",
      etcd: "good",
      controller: "good"
    };

    if (scenario === "pod-crash") {
      if (phase === 1) {
        status.pod = "bad";
        status.controller = "warn";
      } else if (phase === 2) {
        status.pod = "warn";
        status.controller = "good";
      }
    }

    if (scenario === "node-down") {
      if (phase === 1) {
        status.node = "bad";
        status.controller = "warn";
      } else if (phase === 2) {
        status.node = "warn";
        status.controller = "good";
      }
    }

    if (scenario === "api-restart") {
      if (phase === 1) {
        status.api = "bad";
      } else if (phase === 2) {
        status.api = "warn";
      }
    }

    if (scenario === "etcd-loss") {
      if (phase === 1) {
        status.etcd = "bad";
      } else if (phase === 2) {
        status.etcd = "warn";
      }
    }

    return e(
      "div",
      { className: "k8s-canvas" },
      e(
        "svg",
        { viewBox: "0 0 820 260" },
        e(Node, { x: 40, y: 60, w: 150, h: 60, label: "API Server", status: status.api }),
        e(Node, { x: 220, y: 60, w: 150, h: 60, label: "etcd", status: status.etcd }),
        e(Node, { x: 400, y: 60, w: 180, h: 60, label: "Controller", status: status.controller, pulse: true }),
        e(Node, { x: 120, y: 160, w: 180, h: 70, label: "Node", status: status.node }),
        e(Node, { x: 360, y: 160, w: 180, h: 70, label: "Pod", status: status.pod, pulseSlow: true }),
        e(Edge, { d: "M190 120 L220 120", flow: true, active: phase >= 0 }),
        e(Edge, { d: "M370 120 L400 120", flow: true, active: phase >= 0 }),
        e(Edge, { d: "M490 120 L210 160", flow: true, active: phase >= 1 })
      ),
      e("div", { className: "k8s-status" }, "Failure scenario: " + scenario.replace(/-/g, " "))
    );
  }

  function CompareView(props) {
    var phase = props.phase;
    var imperative = ["Provision VM", "Install runtime", "Deploy binary", "Manual recovery"];
    var declarative = ["Apply YAML", "Controllers reconcile", "State converges", "Auto-healing"];

    return e(
      "div",
      { className: "k8s-compare" },
      e(
        "div",
        { className: "k8s-compare-column" },
        e("h3", null, "Imperative"),
        e(
          "ol",
          null,
          imperative.map(function (step, index) {
            var className = "k8s-step";
            if (index === phase % imperative.length) {
              className += " is-active";
            }
            return e("li", { key: step, className: className }, step);
          })
        )
      ),
      e(
        "div",
        { className: "k8s-compare-column" },
        e("h3", null, "Declarative"),
        e(
          "ol",
          null,
          declarative.map(function (step, index) {
            var className = "k8s-step";
            if (index === phase % declarative.length) {
              className += " is-active";
            }
            return e("li", { key: step, className: className }, step);
          })
        )
      )
    );
  }

  function Explorer() {
    var _a = useState("big-picture"), activeTab = _a[0], setActiveTab = _a[1];
    var _b = useState(true), playing = _b[0], setPlaying = _b[1];
    var _c = useState(1), speed = _c[0], setSpeed = _c[1];
    var _d = useTicker(playing, speed), tick = _d[0], setTick = _d[1];
    var _e = useState("healthy"), clusterMode = _e[0], setClusterMode = _e[1];
    var _f = useState(false), pauseControllers = _f[0], setPauseControllers = _f[1];
    var _g = useState(false), staleEtcd = _g[0], setStaleEtcd = _g[1];
    var _h = useState(false), schedulerDown = _h[0], setSchedulerDown = _h[1];
    var _j = useState(false), nodeExhausted = _j[0], setNodeExhausted = _j[1];
    var _k = useState(false), imageFailure = _k[0], setImageFailure = _k[1];
    var _l = useState(4), cpuRequest = _l[0], setCpuRequest = _l[1];
    var _m = useState(8), memRequest = _m[0], setMemRequest = _m[1];
    var _n = useState(true), taintsEnabled = _n[0], setTaintsEnabled = _n[1];
    var _o = useState(false), toleration = _o[0], setToleration = _o[1];
    var _p = useState(false), affinity = _p[0], setAffinity = _p[1];
    var _q = useState("spread"), strategy = _q[0], setStrategy = _q[1];
    var _r = useState("iptables"), netMode = _r[0], setNetMode = _r[1];
    var _s = useState(false), networkPolicy = _s[0], setNetworkPolicy = _s[1];
    var _t = useState("pod-crash"), failureScenario = _t[0], setFailureScenario = _t[1];

    var phaseCounts = {
      "big-picture": 6,
      "control-plane": 6,
      "pod-lifecycle": 7,
      scheduler: 4,
      networking: 5,
      failure: 4,
      compare: 4
    };

    var phase = tick % phaseCounts[activeTab];

    function stepForward() {
      setTick(function (value) {
        return value + 1;
      });
    }

    function stepBack() {
      setTick(function (value) {
        return value > 0 ? value - 1 : 0;
      });
    }

    var view = null;
    if (activeTab === "big-picture") {
      view = e(BigPictureView, { phase: phase, mode: clusterMode, stale: staleEtcd });
    }
    if (activeTab === "control-plane") {
      view = e(ControlPlaneView, { phase: phase, paused: pauseControllers, stale: staleEtcd });
    }
    if (activeTab === "pod-lifecycle") {
      view = e(PodLifecycleView, {
        phase: phase,
        schedulerDown: schedulerDown,
        nodeExhausted: nodeExhausted,
        imageFailure: imageFailure
      });
    }
    if (activeTab === "scheduler") {
      view = e(SchedulerView, {
        cpuRequest: cpuRequest,
        memRequest: memRequest,
        toleration: toleration,
        taintsEnabled: taintsEnabled,
        affinity: affinity,
        strategy: strategy
      });
    }
    if (activeTab === "networking") {
      view = e(NetworkingView, { phase: phase, mode: netMode, policy: networkPolicy });
    }
    if (activeTab === "failure") {
      view = e(FailureView, { phase: phase, scenario: failureScenario });
    }
    if (activeTab === "compare") {
      view = e(CompareView, { phase: phase });
    }

    var controlPanel = null;
    if (activeTab === "big-picture") {
      controlPanel = e(
        "div",
        { className: "k8s-controls" },
        e(
          "label",
          { className: "k8s-control" },
          "Cluster mode",
          e(
            "select",
            { value: clusterMode, onChange: function (event) { setClusterMode(event.target.value); } },
            e("option", { value: "healthy" }, "Healthy"),
            e("option", { value: "failure" }, "Partial failure"),
            e("option", { value: "load" }, "High load")
          )
        )
      );
    }
    if (activeTab === "control-plane") {
      controlPanel = e(
        "div",
        { className: "k8s-controls" },
        e(
          "label",
          { className: "k8s-control" },
          "Pause controller loops",
          e("input", {
            type: "checkbox",
            checked: pauseControllers,
            onChange: function (event) { setPauseControllers(event.target.checked); }
          })
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Inject stale etcd",
          e("input", {
            type: "checkbox",
            checked: staleEtcd,
            onChange: function (event) { setStaleEtcd(event.target.checked); }
          })
        )
      );
    }
    if (activeTab === "pod-lifecycle") {
      controlPanel = e(
        "div",
        { className: "k8s-controls" },
        e(
          "label",
          { className: "k8s-control" },
          "Scheduler unavailable",
          e("input", {
            type: "checkbox",
            checked: schedulerDown,
            onChange: function (event) { setSchedulerDown(event.target.checked); }
          })
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Node resource exhausted",
          e("input", {
            type: "checkbox",
            checked: nodeExhausted,
            onChange: function (event) { setNodeExhausted(event.target.checked); }
          })
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Image pull failure",
          e("input", {
            type: "checkbox",
            checked: imageFailure,
            onChange: function (event) { setImageFailure(event.target.checked); }
          })
        )
      );
    }
    if (activeTab === "scheduler") {
      controlPanel = e(
        "div",
        { className: "k8s-controls" },
        e(
          "label",
          { className: "k8s-control" },
          "CPU request",
          e("input", {
            type: "range",
            min: 1,
            max: 8,
            value: cpuRequest,
            onChange: function (event) { setCpuRequest(parseInt(event.target.value, 10)); }
          }),
          e("span", null, String(cpuRequest))
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Memory request (Gi)",
          e("input", {
            type: "range",
            min: 2,
            max: 32,
            step: 2,
            value: memRequest,
            onChange: function (event) { setMemRequest(parseInt(event.target.value, 10)); }
          }),
          e("span", null, String(memRequest))
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Tolerate taints",
          e("input", {
            type: "checkbox",
            checked: toleration,
            onChange: function (event) { setToleration(event.target.checked); }
          })
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Enable taints",
          e("input", {
            type: "checkbox",
            checked: taintsEnabled,
            onChange: function (event) { setTaintsEnabled(event.target.checked); }
          })
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Affinity bias",
          e("input", {
            type: "checkbox",
            checked: affinity,
            onChange: function (event) { setAffinity(event.target.checked); }
          })
        ),
        e(
          "label",
          { className: "k8s-control" },
          "Strategy",
          e(
            "select",
            { value: strategy, onChange: function (event) { setStrategy(event.target.value); } },
            e("option", { value: "binpack" }, "Bin pack"),
            e("option", { value: "spread" }, "Spread")
          )
        )
      );
    }
    if (activeTab === "networking") {
      controlPanel = e(
        "div",
        { className: "k8s-controls" },
        e(
          "label",
          { className: "k8s-control" },
          "Mode",
          e(
            "select",
            { value: netMode, onChange: function (event) { setNetMode(event.target.value); } },
            e("option", { value: "iptables" }, "iptables"),
            e("option", { value: "ipvs" }, "IPVS"),
            e("option", { value: "ebpf" }, "eBPF")
          )
        ),
        e(
          "label",
          { className: "k8s-control" },
          "NetworkPolicy",
          e("input", {
            type: "checkbox",
            checked: networkPolicy,
            onChange: function (event) { setNetworkPolicy(event.target.checked); }
          })
        )
      );
    }
    if (activeTab === "failure") {
      controlPanel = e(
        "div",
        { className: "k8s-controls" },
        e(
          "label",
          { className: "k8s-control" },
          "Failure scenario",
          e(
            "select",
            { value: failureScenario, onChange: function (event) { setFailureScenario(event.target.value); } },
            e("option", { value: "pod-crash" }, "Pod crash"),
            e("option", { value: "node-down" }, "Node down"),
            e("option", { value: "api-restart" }, "API restart"),
            e("option", { value: "etcd-loss" }, "etcd quorum loss")
          )
        )
      );
    }

    return e(
      "div",
      { className: "k8s-explorer" },
      e(
        "div",
        { className: "k8s-toolbar" },
        e(
          "div",
          { className: "k8s-tabs" },
          tabs.map(function (tab) {
            var className = "k8s-tab" + (tab.id === activeTab ? " is-active" : "");
            return e(
              "button",
              {
                key: tab.id,
                className: className,
                type: "button",
                onClick: function () { setActiveTab(tab.id); }
              },
              tab.label
            );
          })
        ),
        e(
          "div",
          { className: "k8s-playback" },
          e(
            "button",
            { type: "button", onClick: function () { setPlaying(!playing); } },
            playing ? "Pause" : "Play"
          ),
          e(
            "button",
            { type: "button", onClick: stepBack },
            "Step back"
          ),
          e(
            "button",
            { type: "button", onClick: stepForward },
            "Step forward"
          ),
          e(
            "label",
            { className: "k8s-control" },
            "Speed",
            e(
              "select",
              { value: speed, onChange: function (event) { setSpeed(parseFloat(event.target.value)); } },
              e("option", { value: 0.5 }, "0.5x"),
              e("option", { value: 1 }, "1x"),
              e("option", { value: 2 }, "2x"),
              e("option", { value: 4 }, "4x")
            )
          )
        )
      ),
      controlPanel,
      view,
      e(
        "div",
        { className: "k8s-legend" },
        e("span", { className: "k8s-badge" }, e("span", { className: "k8s-dot good" }), "Healthy"),
        e("span", { className: "k8s-badge" }, e("span", { className: "k8s-dot warn" }), "Warning"),
        e("span", { className: "k8s-badge" }, e("span", { className: "k8s-dot bad" }), "Failure"),
        e("span", { className: "k8s-badge" }, "Tick " + tick)
      )
    );
  }

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("k8s-explorer-root");
    if (!root) {
      return;
    }
    if (window.ReactDOM.createRoot) {
      window.ReactDOM.createRoot(root).render(e(Explorer));
    } else {
      window.ReactDOM.render(e(Explorer), root);
    }
  });
})();
