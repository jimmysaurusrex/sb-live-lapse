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

function createDeferredImageLoader(chart, dataReady, camera) {
  var prefix = camera || "satellite";
  var label = camera ? "camera image" : "satellite image";
  var folder = camera ? "./cameras/" : "./satellite/";
  var section = document.getElementById(prefix + "Section");
  if (!section) return { pause: function () {}, schedule: function () {} };
  var image = document.getElementById(prefix + "Image");
  var status = document.getElementById(prefix + "Status");
  var button = document.getElementById(camera ? camera + "Load" : "loadSatellite");
  var loopButton = camera ? null : document.getElementById("playSatellite");
  var caption = document.getElementById(prefix + "Caption");
  var connection = navigator.connection;
  var pageLoaded = document.readyState === "complete";
  var visible = false, requested = false, finished = false, failed = false;
  var active = null, timer = null, pendingUrl = null;
  var stillUrl = null, loopUrl = null, manifest = null;
  var loopRequested = false, playing = false;
  var waitingText = "Loads after the chart, when this section is in view.";

  function conserveData() {
    return connection && (connection.saveData || /^(slow-2g|2g|3g)$/.test(connection.effectiveType));
  }
  function coreReady() {
    return pageLoaded && dataReady() && chart.complete && chart.naturalWidth > 0;
  }
  function eligible() {
    return (!finished || loopRequested) && !active && coreReady() && document.visibilityState !== "hidden" &&
      (visible || requested || loopRequested) &&
      (requested || loopRequested || (!failed && !conserveData() && "IntersectionObserver" in window));
  }
  function updateCaption() {
    if (!manifest) return;
    var observed = new Date(manifest.observed_at);
    var minutes = Math.max(0, Math.floor((Date.now() - observed.getTime()) / 60000));
    var when = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "2-digit",
      minute: "2-digit", hourCycle: "h23", timeZone: "America/Los_Angeles", timeZoneName: "short" }).format(observed);
    if (camera) {
      if (!finished) return;
      caption.textContent = "Latest camera panorama: " + when + " · " + minutes + " min old";
      status.textContent = minutes > 15 ? "Delayed · " + minutes + " min old" : "";
      status.hidden = minutes <= 15;
      return;
    }
    caption.textContent = "Latest scan: " + when + " · " + (manifest.mode === "visible" ? "Visible" : "Night low clouds") +
      " · " + minutes + " min old" + (minutes > 30 ? " — delayed" : "") +
      (manifest.twilight ? " · Twilight: reduced reliability" : "");
    caption.classList.toggle("satellite-stale", minutes > 30);
  }
  function schedule() {
    button.hidden = finished || (!failed && !conserveData() && "IntersectionObserver" in window);
    button.disabled = !coreReady() || !!active;
    if (loopButton) {
      loopButton.hidden = !finished || !manifest || !manifest.loop;
      loopButton.disabled = !coreReady() || !!active;
      loopButton.textContent = playing ? "Stop loop" : "Play last hour";
      loopButton.title = manifest && manifest.loop ? "Download: " + Math.ceil(manifest.loop_bytes / 1024) + " KB" : "";
    }
    if (!finished && !active && !failed) {
      status.textContent = !requested && conserveData() ? "Image paused to save data. Tap to load." : waitingText;
    }
    if (timer !== null || !eligible()) return;
    timer = setTimeout(function () {
      timer = null;
      if (eligible()) load();
    }, 250);
  }
  function stopLoop() {
    loopRequested = false;
    playing = false;
    if (stillUrl) image.src = stillUrl;
  }
  function pause() {
    if (timer !== null) clearTimeout(timer);
    timer = null;
    if (active) {
      active.abort();
      active = null;
      image.onload = null;
      image.onerror = null;
      if (pendingUrl) URL.revokeObjectURL(pendingUrl);
      pendingUrl = null;
      if (!finished) image.removeAttribute("src");
      status.textContent = waitingText;
      status.hidden = finished;
    }
    stopLoop();
    scheduleButtonsOnly();
  }
  function scheduleButtonsOnly() {
    if (loopButton) {
      loopButton.disabled = true;
      loopButton.textContent = "Play last hour";
    }
  }
  function failure(isLoop) {
    active = null;
    status.hidden = false;
    if (isLoop) {
      stopLoop();
      status.textContent = "Loop unavailable. Tap Play to retry.";
    } else {
      failed = true;
      requested = false;
      image.hidden = true;
      status.textContent = "Image unavailable. Tap Retry to try again.";
      button.textContent = "Retry " + label;
    }
    schedule();
  }
  function validManifest(data) {
    if (camera) {
      var filename = new RegExp("^" + camera + "-\\d{10}-v\\d+\\.jpg$");
      if (!data || !filename.test(data.image) || !Number.isFinite(new Date(data.observed_at).getTime())) {
        throw new Error("Invalid camera metadata");
      }
      return data;
    }
    if (!data || !/^\d{14}-(visible|night)-v\d+\.jpg$/.test(data.image) ||
        !Number.isFinite(new Date(data.observed_at).getTime()) || !/^(visible|night)$/.test(data.mode)) {
      throw new Error("Invalid satellite metadata");
    }
    if (data.loop && (!/^\d{14}-\d{14}-\d+-v\d+\.gif$/.test(data.loop) ||
        !Number.isFinite(data.loop_bytes) || data.loop_bytes <= 0)) {
      throw new Error("Invalid satellite loop");
    }
    return data;
  }
  function load() {
    var isLoop = finished && loopRequested;
    if (isLoop && loopUrl) {
      playing = true;
      loopRequested = false;
      image.src = loopUrl;
      status.hidden = true;
      schedule();
      return;
    }
    var request = new AbortController();
    active = request;
    button.disabled = true;
    if (loopButton) loopButton.disabled = true;
    status.hidden = false;
    status.textContent = isLoop ? "Loading last hour…" : "Loading " + label + "…";
    var options = { signal: request.signal, priority: "low", cache: "no-cache", credentials: "omit", referrerPolicy: "no-referrer" };
    // Both metadata and imagery wait for the chart, its data, window load and viewport.
    var metadata = isLoop ? Promise.resolve(manifest) : fetch(folder + (camera ? camera + ".json" : "latest.json"), options).then(function (response) {
      if (!response.ok) throw new Error("Image metadata unavailable");
      return response.json();
    }).then(validManifest);
    metadata.then(function (data) {
      if (active !== request) return null;
      manifest = data;
      return fetch(folder + (isLoop ? data.loop : data.image), options);
    }).then(function (response) {
      if (active !== request) return null;
      var type = isLoop ? "image/gif" : "image/jpeg";
      if (!response.ok || (response.headers.get("content-type") || "").split(";")[0] !== type) {
        throw new Error("Image unavailable");
      }
      return response.blob();
    }).then(function (blob) {
      if (active !== request) return;
      pendingUrl = URL.createObjectURL(blob);
      image.onload = function () {
        if (active !== request) return;
        if (isLoop) {
          loopUrl = pendingUrl;
          playing = true;
          loopRequested = false;
        } else {
          stillUrl = pendingUrl;
          finished = true;
        }
        pendingUrl = null;
        active = null;
        image.onload = image.onerror = null;
        image.hidden = false;
        status.hidden = true;
        updateCaption();
        schedule();
      };
      image.onerror = function () {
        if (active !== request) return;
        URL.revokeObjectURL(pendingUrl);
        pendingUrl = null;
        image.onload = image.onerror = null;
        failure(isLoop);
      };
      image.src = pendingUrl;
    }).catch(function () { if (active === request) failure(isLoop); });
  }
  button.addEventListener("click", function () { requested = true; schedule(); });
  if (loopButton) loopButton.addEventListener("click", function () {
    if (playing) stopLoop();
    else if (finished && manifest && manifest.loop) loopRequested = true;
    schedule();
  });
  chart.addEventListener("load", schedule);
  window.addEventListener("load", function () { pageLoaded = true; schedule(); }, { once: true });
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "hidden") pause();
    else { updateCaption(); schedule(); }
  });
  if (connection && connection.addEventListener) connection.addEventListener("change", function () {
    if (conserveData() && !requested && !loopRequested) pause();
    schedule();
  });
  if ("IntersectionObserver" in window) {
    var observer = new IntersectionObserver(function (entries) {
      visible = entries.some(function (entry) { return entry.isIntersecting; });
      schedule();
    }, { rootMargin: "0px" });
    observer.observe(section);
  }
  setInterval(updateCaption, 60000); // Text only; never fetch in the background.
  schedule();
  return { pause: pause, schedule: schedule };
}

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

  var historySettled = false;
  var latestStateSettled = false;
  var imagery = [null, "gibraltar", "tvhill"].map(function (camera) {
    return createDeferredImageLoader(img, function () { return historySettled && latestStateSettled; }, camera);
  });
  var satellite = {
    pause: function () { imagery.forEach(function (loader) { loader.pause(); }); },
    schedule: function () { imagery.forEach(function (loader) { loader.schedule(); }); }
  };
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
      .catch(function () {})
      .finally(function () { latestStateSettled = true; satellite.schedule(); });
  }

  function render() {
    updateUnitButtons();
    updateNavButtons();
    updateSnapshotFields();
    var snapshot = currentSnapshot();
    satellite.pause();
    img.src = sourceForSnapshot(snapshot, state.unit);
    if (!snapshot || !setTitleForIso(snapshot.run_at)) {
      fetchLatestTitle();
    }
    satellite.schedule();
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
    .catch(function () {})
    .finally(function () { historySettled = true; satellite.schedule(); });
})();
