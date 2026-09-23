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
  var daySelect = document.getElementById("snapshotDay");
  var timeInput = document.getElementById("snapshotTime");

  if (!img || !title || !metricBtn || !imperialBtn || !prevBtn || !nextBtn || !daySelect || !timeInput) return;

  var storageKey = "sb_beta_units";
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
  var pacificFormat = new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZone: "America/Los_Angeles"
  });

  function pacificParts(isoTime) {
    if (!isoTime) return null;
    var dt = new Date(isoTime);
    if (isNaN(dt.getTime())) return null;
    var parts = pacificFormat.formatToParts(dt);
    var values = {};
    parts.forEach(function (part) {
      if (part.type !== "literal") values[part.type] = part.value;
    });
    values.date = values.year + "-" + values.month.padStart(2, "0") + "-" + values.day.padStart(2, "0");
    values.time = values.hour + ":" + values.minute;
    values.minutes = Number(values.hour) * 60 + Number(values.minute);
    return values;
  }

  function formatPacificRefreshLabel(isoTime) {
    var values = pacificParts(isoTime);
    if (!values) return null;
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
    daySelect.disabled = !hasSnapshots;
    timeInput.disabled = !hasSnapshots;
  }

  function clearTimeError() {
    timeInput.setCustomValidity("");
    timeInput.removeAttribute("aria-invalid");
  }

  function updateSnapshotFields() {
    var snapshot = currentSnapshot();
    if (!snapshot) return;
    daySelect.value = snapshot.local.date;
    daySelect.title = snapshot.local.weekday + " " + snapshot.local.month + "/" + snapshot.local.day + " (Pacific time)";
    timeInput.value = snapshot.local.time;
    clearTimeError();
  }

  function populateDays() {
    if (!state.snapshots.length) return;
    daySelect.replaceChildren();
    var seen = {};
    var weekdays = { Tue: "Tues", Wed: "Weds", Thu: "Thurs" };
    state.snapshots.slice().reverse().forEach(function (snapshot) {
      var local = snapshot.local;
      if (seen[local.date]) return;
      seen[local.date] = true;
      var option = document.createElement("option");
      option.value = local.date;
      option.textContent = weekdays[local.weekday] || local.weekday;
      option.title = local.month + "/" + local.day + "/" + local.year;
      daySelect.appendChild(option);
    });
  }

  function jumpToTime(reportError) {
    var raw = timeInput.value.trim();
    var match = /^(\d{1,2}):(\d{2})$/.exec(raw) || /^(\d{1,2})(\d{2})$/.exec(raw);
    if (!match || Number(match[1]) > 23 || Number(match[2]) > 59) {
      if (reportError) {
        timeInput.setCustomValidity("Enter a 24-hour time from 00:00 to 23:59.");
        timeInput.setAttribute("aria-invalid", "true");
        timeInput.reportValidity();
      } else {
        updateSnapshotFields();
      }
      return;
    }
    var minutes = Number(match[1]) * 60 + Number(match[2]);
    var nearest = -1;
    var distance = Infinity;
    state.snapshots.forEach(function (snapshot, index) {
      if (snapshot.local.date !== daySelect.value) return;
      var gap = Math.abs(snapshot.local.minutes - minutes);
      // Keep the closest chronological occurrence when clocks repeat at DST.
      if (gap < distance || (gap === distance && Math.abs(index - state.index) < Math.abs(nearest - state.index))) {
        nearest = index;
        distance = gap;
      }
    });
    if (nearest !== -1) {
      if (nearest === state.index) {
        updateSnapshotFields();
        return;
      }
      state.index = nearest;
      render();
    }
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
        if (!latestState || !latestState.generated_at || currentSnapshot()) return;
        setTitleForIso(latestState.generated_at);
      })
      .catch(function () {});
  }

  function render() {
    updateUnitButtons();
    updateNavButtons();
    updateSnapshotFields();
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
  daySelect.addEventListener("change", function () { jumpToTime(false); });
  timeInput.addEventListener("input", clearTimeError);
  timeInput.addEventListener("blur", function () { jumpToTime(false); });
  timeInput.addEventListener("keydown", function (event) {
    if (event.key === "Enter") {
      event.preventDefault();
      jumpToTime(true);
    } else if (event.key === "Escape") {
      event.preventDefault();
      updateSnapshotFields();
      timeInput.blur();
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
            !isNaN(Date.parse(snap.run_at)) &&
            snap.charts &&
            isChartPath(snap.charts.metric_svg, "metric") &&
            isChartPath(snap.charts.imperial_svg, "imperial")
          );
        });
      }
      snapshots.sort(function (a, b) {
        return Date.parse(a.run_at) - Date.parse(b.run_at);
      });
      snapshots = snapshots.map(function (snapshot) {
        return { run_at: snapshot.run_at, charts: snapshot.charts, local: pacificParts(snapshot.run_at) };
      });
      state.snapshots = snapshots;
      state.index = snapshots.length ? snapshots.length - 1 : -1;
      populateDays();
      render();
    })
    .catch(function () {});
})();
