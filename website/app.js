// ==============================================================================
// 🌌 VortexKV In-Browser Interactive Simulator & Landing Deck Engine
// ==============================================================================

// 1. In-Memory KV and Vector Storage Simulation
class VortexSimulator {
  constructor() {
    this.store = new Map();
    this.vectors = new Map(); // index -> Map(id -> number[])
    this.bootTime = Date.now();
    this.totalOps = 42;

    // Seed realistic initial data
    this.store.set("app:name", { type: "string", value: "VortexKV Production" });
    this.store.set("app:version", { type: "string", value: "1.0.0-PROD" });
    this.store.set("session:100", { type: "string", value: "CyberVortex" });
    this.store.set("user:101", { type: "hash", value: { name: "Alice Vance", role: "admin", org: "CyberDyne" } });

    // Seed vector index "articles"
    const articleVecs = new Map();
    articleVecs.set("doc_ai", [0.95, 0.10, 0.05]);
    articleVecs.set("doc_quantum", [0.12, 0.94, 0.20]);
    articleVecs.set("doc_crypto", [0.30, 0.25, 0.90]);
    this.vectors.set("articles", articleVecs);

    // Seed Streams
    this.streams = new Map();
    const eventStream = [
      { id: "1726150000000-0", fields: { user: "Alice", action: "login", ip: "192.168.1.5" } },
      { id: "1726150005000-0", fields: { user: "Bob", action: "checkout", amount: "$149.00" } }
    ];
    this.streams.set("events", eventStream);
  }

  cosineSimilarity(a, b) {
    let dot = 0.0, normA = 0.0, normB = 0.0;
    for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    if (normA === 0 || normB === 0) return 0;
    return dot / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  euclideanDistance(a, b) {
    let sum = 0.0;
    for (let i = 0; i < a.length; i++) {
      const diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  execute(rawCmd) {
    this.totalOps++;
    const parts = rawCmd.trim().match(/(?:[^\s"]+|"[^"]*")+/g) || [];
    if (parts.length === 0) return null;

    const cmd = parts[0].toUpperCase();
    const args = parts.slice(1).map(s => s.replace(/^"|"$/g, ''));

    switch (cmd) {
      case "PING":
        return { type: "ok", text: "PONG" };

      case "SET": {
        if (args.length < 2) return { type: "err", text: "(error) ERR wrong number of arguments for 'set' command" };
        this.store.set(args[0], { type: "string", value: args[1] });
        return { type: "ok", text: "OK" };
      }

      case "GET": {
        if (args.length < 1) return { type: "err", text: "(error) ERR wrong number of arguments for 'get' command" };
        const entry = this.store.get(args[0]);
        if (!entry) return { type: "dim", text: "(nil)" };
        if (entry.type !== "string") return { type: "err", text: "(error) WRONGTYPE Operation against a key holding the wrong kind of value" };
        return { type: "val", text: `"${entry.value}"` };
      }

      case "INCR": {
        if (args.length < 1) return { type: "err", text: "(error) ERR wrong number of arguments for 'incr' command" };
        const entry = this.store.get(args[0]);
        let val = 0;
        if (entry) {
          val = parseInt(entry.value, 10);
          if (isNaN(val)) return { type: "err", text: "(error) ERR value is not an integer or out of range" };
        }
        val++;
        this.store.set(args[0], { type: "string", value: val.toString() });
        return { type: "val", text: `(integer) ${val}` };
      }

      case "KEYS": {
        const pattern = args[0] || "*";
        const keys = Array.from(this.store.keys());
        if (keys.length === 0) return { type: "dim", text: "(empty list or set)" };
        return {
          type: "val",
          lines: keys.map((k, idx) => `${idx + 1}) "${k}"`)
        };
      }

      case "DEL": {
        if (args.length < 1) return { type: "err", text: "(error) ERR wrong number of arguments for 'del' command" };
        let count = 0;
        for (const k of args) {
          if (this.store.delete(k)) count++;
        }
        return { type: "val", text: `(integer) ${count}` };
      }

      case "HSET": {
        if (args.length < 3 || (args.length - 1) % 2 !== 0) {
          return { type: "err", text: "(error) ERR wrong number of arguments for 'hset' command" };
        }
        let entry = this.store.get(args[0]);
        if (!entry) {
          entry = { type: "hash", value: {} };
          this.store.set(args[0], entry);
        }
        let added = 0;
        for (let i = 1; i < args.length; i += 2) {
          if (!(args[i] in entry.value)) added++;
          entry.value[args[i]] = args[i + 1];
        }
        return { type: "val", text: `(integer) ${added}` };
      }

      case "HGETALL": {
        if (args.length < 1) return { type: "err", text: "(error) ERR wrong number of arguments for 'hgetall' command" };
        const entry = this.store.get(args[0]);
        if (!entry) return { type: "dim", text: "(empty array)" };
        const lines = [];
        let idx = 1;
        for (const [k, v] of Object.entries(entry.value)) {
          lines.push(`${idx++}) "${k}"`);
          lines.push(`${idx++}) "${v}"`);
        }
        return { type: "val", lines };
      }

      // Next-Gen Vector Search Primitives
      case "VADD": {
        if (args.length < 3) return { type: "err", text: "Syntax: VADD <index> <id> <f1> <f2> ... <fN>" };
        const [indexName, id, ...floats] = args;
        const vec = floats.map(f => parseFloat(f));
        if (vec.some(isNaN)) return { type: "err", text: "(error) ERR vector components must be valid floats" };

        if (!this.vectors.has(indexName)) {
          this.vectors.set(indexName, new Map());
        }
        this.vectors.get(indexName).set(id, vec);
        return { type: "ok", text: `OK (Vector '${id}' indexed in '${indexName}' with ${vec.length} dimensions)` };
      }

      case "VSEARCH": {
        if (args.length < 4) return { type: "err", text: "Syntax: VSEARCH <index> <topK> <cosine|euclidean> <q1> <q2> ... <qN>" };
        const indexName = args[0];
        const topK = parseInt(args[1], 10);
        const metric = args[2].toLowerCase();
        const queryVec = args.slice(3).map(f => parseFloat(f));

        const indexMap = this.vectors.get(indexName);
        if (!indexMap || indexMap.size === 0) return { type: "dim", text: `(empty vector index '${indexName}')` };

        const scores = [];
        for (const [id, targetVec] of indexMap.entries()) {
          if (targetVec.length !== queryVec.length) continue;
          let sim = 0;
          if (metric === "cosine") {
            sim = this.cosineSimilarity(queryVec, targetVec);
          } else {
            sim = 1 / (1 + this.euclideanDistance(queryVec, targetVec));
          }
          scores.push({ id, score: sim.toFixed(6) });
        }

        scores.sort((a, b) => b.score - a.score);
        const results = scores.slice(0, topK);

        const lines = [];
        results.forEach((item, idx) => {
          lines.push(`${idx + 1}) 1) "${item.id}"`);
          lines.push(`   2) "${item.score}"`);
        });

        return { type: "val", lines };
      }

      // Redis Streams Primitives
      case "XADD": {
        if (args.length < 4 || (args.length - 2) % 2 !== 0) {
          return { type: "err", text: "(error) ERR syntax: XADD <stream> <ID|*> <field> <value> [field value...]" };
        }
        const streamKey = args[0];
        let id = args[1];
        if (id === "*") {
          id = `${Date.now()}-${Math.floor(Math.random() * 10)}`;
        }
        const fields = {};
        for (let i = 2; i < args.length; i += 2) {
          fields[args[i]] = args[i + 1];
        }
        if (!this.streams.has(streamKey)) {
          this.streams.set(streamKey, []);
        }
        this.streams.get(streamKey).push({ id, fields });
        return { type: "val", text: `"${id}"` };
      }

      case "XRANGE": {
        if (args.length < 3) return { type: "err", text: "(error) ERR syntax: XRANGE <stream> <start> <end> [COUNT count]" };
        const streamKey = args[0];
        const entries = this.streams.get(streamKey);
        if (!entries || entries.length === 0) return { type: "dim", text: "(empty array)" };
        const lines = [];
        let itemNum = 1;
        entries.forEach(entry => {
          lines.push(`${itemNum}) 1) "${entry.id}"`);
          lines.push(`   2)`);
          let fIdx = 1;
          for (const [k, v] of Object.entries(entry.fields)) {
            lines.push(`      ${fIdx++}) "${k}"`);
            lines.push(`      ${fIdx++}) "${v}"`);
          }
          itemNum++;
        });
        return { type: "val", lines };
      }

      case "XLEN": {
        if (args.length < 1) return { type: "err", text: "(error) ERR wrong number of arguments for 'xlen' command" };
        const streamKey = args[0];
        const entries = this.streams.get(streamKey) || [];
        return { type: "val", text: `(integer) ${entries.length}` };
      }

      // Embedded Lua 5.1 Scripting
      case "EVAL": {
        if (args.length < 2) return { type: "err", text: "(error) ERR wrong number of arguments for 'eval' command" };
        const script = args[0];
        const numKeys = parseInt(args[1], 10) || 0;
        const keys = args.slice(2, 2 + numKeys);
        if (keys.length > 0) {
          const target = this.store.get(keys[0]);
          if (target && target.type === "string") {
            return { type: "val", text: `"${target.value}"` };
          }
        }
        return { type: "val", text: `(integer) 1` };
      }

      case "EVALSHA": {
        if (args.length < 2) return { type: "err", text: "(error) ERR wrong number of arguments for 'evalsha' command" };
        return { type: "val", text: `(integer) 1` };
      }

      case "SCRIPT": {
        const sub = (args[0] || "").toUpperCase();
        if (sub === "LOAD") {
          return { type: "val", text: `"6b142468d20025f187a2d829fd240f92b0c360b8"` };
        } else if (sub === "EXISTS") {
          return { type: "val", lines: ["1) (integer) 1"] };
        } else if (sub === "FLUSH") {
          return { type: "ok", text: "OK" };
        }
        return { type: "ok", text: "OK" };
      }

      // WebAssembly (Wasm) Engine
      case "WASM": {
        const sub = (args[0] || "").toUpperCase();
        if (sub === "LIST") {
          return {
            type: "val",
            lines: [
              "1) 1) \"name\"",
              "   2) \"fast_hash_v1\"",
              "   3) \"runtime\"",
              "   4) \"wazero-pure-go\"",
              "2) 1) \"name\"",
              "   2) \"tensor_norm_v2\"",
              "   3) \"runtime\"",
              "   4) \"wazero-pure-go\""
            ]
          };
        } else if (sub === "CALL") {
          if (args.length < 2) return { type: "err", text: "(error) ERR syntax: WASM CALL <func_name> [args...]" };
          return { type: "val", text: `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"` };
        } else if (sub === "LOAD") {
          return { type: "ok", text: "OK (WebAssembly module compiled and instantiated via wazero)" };
        }
        return { type: "err", text: "(error) ERR unknown WASM subcommand, use LIST, CALL, LOAD, or DELETE" };
      }

      // Cluster & Gossip Bus
      case "CLUSTER": {
        const sub = (args[0] || "").toUpperCase();
        if (sub === "NODES") {
          return {
            type: "val",
            lines: [
              "07c40f06 127.0.0.1:7379@17379 myself,master - 0 1726150000000 1 connected 0-5460",
              "e4a8b712 127.0.0.1:7381@17381 master - 0 1726150000000 2 connected 5461-10922",
              "9f3c1d45 127.0.0.1:7382@17382 master - 0 1726150000000 3 connected 10923-16383"
            ]
          };
        } else if (sub === "SLOTS") {
          return {
            type: "val",
            lines: [
              "1) 1) (integer) 0",
              "   2) (integer) 5460",
              "   3) 1) \"127.0.0.1\"",
              "      2) (integer) 7379",
              "2) 1) (integer) 5461",
              "   2) (integer) 10922",
              "   3) 1) \"127.0.0.1\"",
              "      2) (integer) 7381",
              "3) 1) (integer) 10923",
              "   2) (integer) 16383",
              "   3) 1) \"127.0.0.1\"",
              "      2) (integer) 7382"
            ]
          };
        } else if (sub === "KEYSLOT") {
          return { type: "val", text: `(integer) 7124` };
        }
        return { type: "ok", text: "OK" };
      }

      case "INFO": {
        const uptime = Math.floor((Date.now() - this.bootTime) / 1000);
        return {
          type: "val",
          lines: [
            "# Server",
            "vortex_version:1.0.0-PROD",
            "redis_version:7.2.0-compat",
            "os:Linux 6.6.0-cloud",
            `uptime_in_seconds:${uptime}`,
            "",
            "# Clients & Concurrency",
            "connected_clients:1",
            "shards_count:64 (lock-striped)",
            "",
            "# Stats",
            `total_commands_processed:${this.totalOps}`,
            "instantaneous_ops_per_sec:210970",
            `total_keys:${this.store.size}`,
            "vector_indices:1",
            "latency_p50_us:135"
          ]
        };
      }

      case "HELP": {
        return {
          type: "val",
          lines: [
            "Available Sandbox Commands:",
            "  • SET key value          : Store a string value",
            "  • GET key                : Retrieve a string value",
            "  • INCR key               : Increment an integer value",
            "  • KEYS *                 : List all keys in keyspace",
            "  • HSET key field value   : Store a field in a hash",
            "  • HGETALL key            : Retrieve all fields from hash",
            "  • XADD strm * f v...     : Append event to Redis Stream",
            "  • XRANGE strm start end  : Query stream events",
            "  • EVAL script num keys.. : Run atomic Lua 5.1 script",
            "  • WASM LIST / CALL func  : Execute WebAssembly function",
            "  • CLUSTER NODES / SLOTS  : View distributed cluster & gossip topology",
            "  • VADD idx id f1 f2...   : Store embedding vector",
            "  • VSEARCH idx K metric.. : Nearest neighbor vector search",
            "  • INFO                   : View live engine metrics & stats",
            "  • CLEAR                  : Clear terminal screen"
          ]
        };
      }

      case "CLEAR":
        return { type: "clear" };

      default:
        return { type: "err", text: `(error) ERR unknown command '${cmd}', try 'HELP'` };
    }
  }
}

// 2. Terminal UI Controller
document.addEventListener("DOMContentLoaded", () => {
  const sim = new VortexSimulator();
  const termBody = document.getElementById("termBody");
  const termOutput = document.getElementById("termOutput");
  const termInput = document.getElementById("termInput");
  const presetChips = document.querySelectorAll(".preset-chip");
  const copyBtns = document.querySelectorAll(".copy-btn");
  const instTabs = document.querySelectorAll(".inst-tab");
  const instCode = document.getElementById("instCode");

  // History buffer
  const history = [];
  let historyIdx = -1;

  function appendLine(content, className = "resp-val") {
    const div = document.createElement("div");
    div.className = `term-line ${className}`;
    div.textContent = content;
    termOutput.appendChild(div);
    termBody.scrollTop = termBody.scrollHeight;
  }

  function handleCommand(raw) {
    if (!raw.trim()) return;

    // Add to history
    history.push(raw);
    historyIdx = history.length;

    // Print command line
    appendLine(`> ${raw}`, "cmd-line");

    const res = sim.execute(raw);
    if (!res) return;

    if (res.type === "clear") {
      termOutput.innerHTML = "";
      return;
    }

    if (res.lines) {
      res.lines.forEach(l => appendLine(l, "resp-val"));
    } else if (res.text) {
      let cls = "resp-val";
      if (res.type === "ok") cls = "resp-ok";
      if (res.type === "err") cls = "resp-err";
      if (res.type === "dim") cls = "resp-dim";
      appendLine(res.text, cls);
    }
  }

  termInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      const val = termInput.value;
      termInput.value = "";
      handleCommand(val);
    } else if (e.key === "ArrowUp") {
      if (historyIdx > 0) {
        historyIdx--;
        termInput.value = history[historyIdx];
      }
      e.preventDefault();
    } else if (e.key === "ArrowDown") {
      if (historyIdx < history.length - 1) {
        historyIdx++;
        termInput.value = history[historyIdx];
      } else {
        historyIdx = history.length;
        termInput.value = "";
      }
      e.preventDefault();
    }
  });

  // Focus terminal input when clicking anywhere on terminal
  termBody.addEventListener("click", () => {
    termInput.focus();
  });

  // Preset Chips
  presetChips.forEach(chip => {
    chip.addEventListener("click", () => {
      const cmd = chip.getAttribute("data-cmd");
      termInput.value = cmd;
      termInput.focus();
      handleCommand(cmd);
      termInput.value = "";
    });
  });

  // Installer Tabs
  const installCommands = {
    curl: "curl -fsSL https://raw.githubusercontent.com/GargAnshu9468/vortexkv/main/install.sh | bash",
    docker: "docker run -d -p 7379:7379 -p 7380:7380 ianshugarg/vortexkv:latest",
    helm: "helm install vortexkv ./deployments/helm/vortexkv",
    operator: "kubectl apply -f deployments/operator/crd.yaml && kubectl apply -f deployments/operator/operator.yaml",
    brew: "brew install vortexkv/tap/vortexkv",
    source: "git clone https://github.com/GargAnshu9468/vortexkv.git && make build"
  };

  instTabs.forEach(tab => {
    tab.addEventListener("click", () => {
      instTabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      const target = tab.getAttribute("data-target");
      if (installCommands[target]) {
        instCode.textContent = installCommands[target];
      }
    });
  });

  // Copy Buttons & Toast
  const toast = document.getElementById("toast");
  let toastTimer = null;

  function showToast(msg) {
    if (!toast) return;
    toast.textContent = msg;
    toast.classList.add("show");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => {
      toast.classList.remove("show");
    }, 2400);
  }

  copyBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      const text = instCode.textContent.trim();
      navigator.clipboard.writeText(text).then(() => {
        showToast("✓ Copied to clipboard!");
      }).catch(() => {
        showToast("✓ Command ready!");
      });
    });
  });

  window.copyBenchCmd = function() {
    const cmd = "redis-benchmark -h 127.0.0.1 -p 7379 -c 50 -n 100000 -t get,set -q";
    navigator.clipboard.writeText(cmd).then(() => {
      showToast("✓ Copied benchmark command!");
    }).catch(() => {
      showToast("✓ redis-benchmark command ready!");
    });
  };

  // Mobile Navigation Drawer Toggle
  const mobileToggleBtn = document.getElementById("mobileToggleBtn");
  const mobileNavDrawer = document.getElementById("mobileNavDrawer");
  const mobileNavLinks = document.querySelectorAll(".mobile-nav-link");

  if (mobileToggleBtn && mobileNavDrawer) {
    mobileToggleBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const isOpen = mobileNavDrawer.classList.toggle("open");
      mobileToggleBtn.classList.toggle("active", isOpen);
      mobileToggleBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
    });

    // Close when clicking any link inside drawer
    mobileNavLinks.forEach(link => {
      link.addEventListener("click", () => {
        mobileNavDrawer.classList.remove("open");
        mobileToggleBtn.classList.remove("active");
        mobileToggleBtn.setAttribute("aria-expanded", "false");
      });
    });

    // Close when clicking outside drawer
    document.addEventListener("click", (e) => {
      if (!mobileNavDrawer.contains(e.target) && !mobileToggleBtn.contains(e.target)) {
        mobileNavDrawer.classList.remove("open");
        mobileToggleBtn.classList.remove("active");
        mobileToggleBtn.setAttribute("aria-expanded", "false");
      }
    });

    // Close on Escape key
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && mobileNavDrawer.classList.contains("open")) {
        mobileNavDrawer.classList.remove("open");
        mobileToggleBtn.classList.remove("active");
        mobileToggleBtn.setAttribute("aria-expanded", "false");
      }
    });
  }

  // FAQ Accordion Interaction with Accessibility
  const faqQuestions = document.querySelectorAll(".faq-question");
  faqQuestions.forEach(btn => {
    btn.addEventListener("click", () => {
      const item = btn.closest(".faq-item");
      if (!item) return;
      const isOpen = item.classList.toggle("open");
      btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
    });
  });
});


