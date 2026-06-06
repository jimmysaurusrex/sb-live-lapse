(function redirectCanonicalHost() {
  var githubPagesHost = "jimmysaurusrex.github.io";
  var githubPagesPrefix = "/sb-live-lapse";
  var canonicalOrigin = "https://sb-live-lapse.com";

  if (window.location.hostname !== githubPagesHost) return;

  var path = window.location.pathname;
  if (path.indexOf(githubPagesPrefix) === 0) {
    path = path.slice(githubPagesPrefix.length) || "/";
  }
  if (path === "/index.html") {
    path = "/";
  }

  var target = canonicalOrigin + path + window.location.search + window.location.hash;
  window.location.replace(target);
})();

(function initChartControls() {
  var slotMs = 15 * 60 * 1000;
  var slot = Math.floor(Date.now() / slotMs);
  var img = document.getElementById("chart");
  var title = document.getElementById("chartTitle");
  var metricBtn = document.getElementById("metricBtn");
  var imperialBtn = document.getElementById("imperialBtn");
  var prevBtn = document.getElementById("prevSnapshotBtn");
  var nextBtn = document.getElementById("nextSnapshotBtn");

  if (!img || !title || !metricBtn || !imperialBtn || !prevBtn || !nextBtn) return;

  var storageKey = "sb_units";
  var snapshotPathRe = /^snapshots\/\d{8}T\d{4}Z_(metric|imperial)\.svg$/;
  var latestSources = {
    metric: "./sba_wwtemp_chart_metric.svg?v=" + slot,
    imperial: "./sba_wwtemp_chart_imperial.svg?v=" + slot
  };
  var state = {
    unit: "metric",
    snapshots: [],
    index: -1
  };
  var fetchedLatestTitle = false;

  function formatPacificRefreshLabel(isoTime) {
    if (!isoTime) return null;
    var dt = new Date(isoTime);
    if (isNaN(dt.getTime())) return null;
    var parts = new Intl.DateTimeFormat("en-US", {
      weekday: "short",
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "America/Los_Angeles"
    }).formatToParts(dt);
    var values = {};
    parts.forEach(function (part) {
      if (part.type !== "literal") values[part.type] = part.value;
    });
    if (!values.weekday || !values.month || !values.day || !values.hour || !values.minute) {
      return null;
    }
    return values.weekday + " " + values.month + "/" + values.day + " at " + values.hour + ":" + values.minute;
  }

  function setTitleForIso(isoTime) {
    var label = formatPacificRefreshLabel(isoTime);
    if (!label) return false;
    title.textContent = "Santa Barbara Lapse Chart (Refreshed " + label + ")";
    return true;
  }

  function updateUnitButtons() {
    var isMetric = state.unit === "metric";
    metricBtn.classList.toggle("is-active", isMetric);
    imperialBtn.classList.toggle("is-active", !isMetric);
  }

  function setNavButtonDisabled(btn, disabled) {
    btn.disabled = disabled;
    btn.classList.toggle("is-disabled", disabled);
  }

  function updateNavButtons() {
    var hasSnapshots = state.snapshots.length > 0;
    setNavButtonDisabled(prevBtn, !hasSnapshots || state.index <= 0);
    setNavButtonDisabled(nextBtn, !hasSnapshots || state.index >= state.snapshots.length - 1);
  }

  function currentSnapshot() {
    if (state.index < 0 || state.index >= state.snapshots.length) return null;
    return state.snapshots[state.index];
  }

  function isChartPath(value, unit) {
    if (typeof value !== "string" || !snapshotPathRe.test(value)) return false;
    return unit === "imperial" ? value.indexOf("_imperial.svg") !== -1 : value.indexOf("_metric.svg") !== -1;
  }

  function sourceForSnapshot(snapshot, unit) {
    if (snapshot && snapshot.charts) {
      var key = unit === "imperial" ? "imperial_svg" : "metric_svg";
      var value = snapshot.charts[key];
      if (isChartPath(value, unit)) {
        return value + "?v=" + encodeURIComponent(snapshot.run_at || String(slot));
      }
    }
    return latestSources[unit];
  }

  function fetchLatestTitle() {
    if (fetchedLatestTitle) return;
    fetchedLatestTitle = true;
    fetch("./station_state.json?v=" + slot, { cache: "no-store" })
      .then(function (resp) { return resp.ok ? resp.json() : null; })
      .then(function (latestState) {
        if (!latestState || !latestState.generated_at) return;
        setTitleForIso(latestState.generated_at);
      })
      .catch(function () {});
  }

  function render() {
    updateUnitButtons();
    updateNavButtons();
    var snapshot = currentSnapshot();
    img.src = sourceForSnapshot(snapshot, state.unit);
    if (!snapshot || !setTitleForIso(snapshot.run_at)) {
      fetchLatestTitle();
    }
  }

  function setUnit(unit) {
    state.unit = unit === "imperial" ? "imperial" : "metric";
    try { localStorage.setItem(storageKey, state.unit); } catch (e) {}
    render();
  }

  metricBtn.addEventListener("click", function () { setUnit("metric"); });
  imperialBtn.addEventListener("click", function () { setUnit("imperial"); });
  prevBtn.addEventListener("click", function () {
    if (state.index > 0) {
      state.index -= 1;
      render();
    }
  });
  nextBtn.addEventListener("click", function () {
    if (state.index < state.snapshots.length - 1) {
      state.index += 1;
      render();
    }
  });

  try {
    var raw = localStorage.getItem(storageKey);
    if (raw === "imperial") state.unit = "imperial";
  } catch (e) {}
  render();

  fetch("./station_history.json?v=" + slot, { cache: "no-store" })
    .then(function (resp) { return resp.ok ? resp.json() : null; })
    .then(function (historyPayload) {
      var snapshots = [];
      if (historyPayload && Array.isArray(historyPayload.snapshots)) {
        snapshots = historyPayload.snapshots.filter(function (snap) {
          return !!(
            snap &&
            typeof snap.run_at === "string" &&
            snap.charts &&
            isChartPath(snap.charts.metric_svg, "metric") &&
            isChartPath(snap.charts.imperial_svg, "imperial")
          );
        });
      }
      snapshots.sort(function (a, b) {
        return a.run_at < b.run_at ? -1 : (a.run_at > b.run_at ? 1 : 0);
      });
      state.snapshots = snapshots;
      state.index = snapshots.length ? snapshots.length - 1 : -1;
      render();
    })
    .catch(function () {});
})();
