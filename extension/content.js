function nowMs() {
  return Date.now();
}

function getVideoIdFromUrl(url) {
  try {
    const u = new URL(url);
    return u.searchParams.get("v");
  } catch {
    return null;
  }
}

function randomId() {
  return `${nowMs()}-${Math.random().toString(16).slice(2)}`;
}

function safeSendMessage(payload) {
  try {
    chrome.runtime.sendMessage(payload, () => {
      const err = chrome.runtime.lastError;
      if (err) console.warn("sendMessage error:", err.message);
    });
  } catch (e) {
    console.warn("Extension context invalidated:", e);
  }
}

function sendEvent(event) {
  safeSendMessage({ kind: "YT_EVENT", event });
}

function getChannelInfo() {
  const owner =
    document.querySelector("ytd-video-owner-renderer") ||
    document.querySelector("#owner") ||
    document.querySelector("ytd-watch-metadata #owner");

  if (!owner) return { name: null, url: null, handle: null, channel_id: null };

  const channelLink =
    owner.querySelector("ytd-channel-name a#text") ||
    owner.querySelector("ytd-channel-name a") ||
    owner.querySelector('a[href^="/@"]') ||
    owner.querySelector('a[href^="/channel/"]');

  const name = channelLink?.textContent?.trim() || null;
  const href = channelLink?.getAttribute("href") || null;
  const url = href ? new URL(href, location.origin).toString() : null;

  let handle = null;
  if (href && href.startsWith("/@")) handle = href.split("/")[1] || null;

  let channel_id = null;
  if (href && href.startsWith("/channel/")) {
    channel_id = href.split("/")[2] || null;
  }

  return { name, url, handle, channel_id };
}

async function waitForChannelInfo(maxWaitMs = 2500, intervalMs = 120) {
  const deadline = nowMs() + maxWaitMs;
  while (nowMs() < deadline) {
    const info = getChannelInfo();
    if (info.name || info.handle || info.url || info.channel_id) return info;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return getChannelInfo();
}

function getPlayerState(videoEl) {
  if (!videoEl) return "unknown";
  if (videoEl.ended) return "ended";
  if (videoEl.paused) return "paused";
  return "playing";
}

function isVisibleNow() {
  return document.visibilityState === "visible";
}

function getWatchMode() {
  return isVisibleNow() ? "foreground" : "background";
}

const state = {
  schema: 1,
  tab_id: randomId(),
  client_session_id: null,
  current_video_id: null,
  current_session_id: null,
  channel_name: null,
  channel_url: null,
  channel_handle: null,
  channel_id: null,
  last_url: location.href,
  is_visible: isVisibleNow(),
  player_state: "unknown",
  tick_interval_ms: 10_000,
  last_tick_ms: null,
  tick_timer: null,
  last_context_missing_emit_ms: 0
};

function baseEvent(event_type) {
  return {
    schema: state.schema,
    event_id: randomId(),
    event_ts: nowMs(),
    event_type,
    client_session_id: state.client_session_id,
    tab_id: state.tab_id,
    video_id: state.current_video_id,
    channel_name: state.channel_name,
    channel_url: state.channel_url,
    channel_handle: state.channel_handle,
    channel_id: state.channel_id,
    url: location.href,
    is_visible: state.is_visible,
    watch_mode: getWatchMode(),
    player_state: state.player_state,
    tz_offset_min: new Date().getTimezoneOffset()
  };
}

async function ensureClientSessionId() {
  return new Promise((resolve) => {
    chrome.storage.local.get(["client_session_id"], (res) => {
      if (res && res.client_session_id) {
        state.client_session_id = res.client_session_id;
        resolve(res.client_session_id);
        return;
      }
      const id = randomId();
      chrome.storage.local.set({ client_session_id: id }, () => {
        state.client_session_id = id;
        resolve(id);
      });
    });
  });
}

async function setVideoContext(videoId) {
  state.current_video_id = videoId;
  const ch = await waitForChannelInfo();
  state.channel_name = ch.name;
  state.channel_url = ch.url;
  state.channel_handle = ch.handle;
  state.channel_id = ch.channel_id;
}

function clearVideoContext() {
  state.current_video_id = null;
  state.current_session_id = null;
  state.channel_name = null;
  state.channel_url = null;
  state.channel_handle = null;
  state.channel_id = null;
}

function startNewVideoSession() {
  state.current_session_id = randomId();
  state.last_tick_ms = null;
  sendEvent({
    ...baseEvent("video_start"),
    video_session_id: state.current_session_id
  });
}

function stopVideoSession(reason = "unknown") {
  if (!state.current_video_id || !state.current_session_id) return;
  sendEvent({
    ...baseEvent("video_stop"),
    video_session_id: state.current_session_id,
    reason
  });
  state.current_session_id = null;
  state.last_tick_ms = null;
}

function maybeEmitContextMissing(reason) {
  const t = nowMs();
  if (t - state.last_context_missing_emit_ms < 30_000) return;
  state.last_context_missing_emit_ms = t;
  sendEvent({
    ...baseEvent("context_missing"),
    context_type: "unknown",
    reason: reason || "unknown"
  });
}

function clampDeltaMs(delta) {
  if (delta < 0) return 0;
  return Math.min(delta, 20_000);
}

function flushPendingTick(videoEl, reason = "flush") {
  if (!state.current_video_id || !state.current_session_id) return;
  if (!videoEl) return;

  const isActuallyPlaying = !videoEl.paused && !videoEl.ended;
  if (!isActuallyPlaying) {
    if (state.last_tick_ms == null) return;
  }

  const t = nowMs();
  if (state.last_tick_ms == null) {
    state.last_tick_ms = t;
    return;
  }

  const delta = clampDeltaMs(t - state.last_tick_ms);
  state.last_tick_ms = null;

  if (delta <= 0) return;

  sendEvent({
    ...baseEvent("watch_tick"),
    video_session_id: state.current_session_id,
    watch_ms_delta: delta,
    position_s: typeof videoEl.currentTime === "number" ? videoEl.currentTime : null,
    playback_rate: typeof videoEl.playbackRate === "number" ? videoEl.playbackRate : null,
    flush_reason: reason
  });
}

function startTicking(videoEl) {
  if (state.tick_timer) return;
  state.last_tick_ms = nowMs();

  state.tick_timer = setInterval(() => {
    const vid = getVideoIdFromUrl(location.href);
    if (!vid) {
      stopTicking();
      return;
    }

    state.is_visible = isVisibleNow();
    state.player_state = getPlayerState(videoEl);

    const isActuallyPlaying = videoEl && !videoEl.paused && !videoEl.ended;
    if (!isActuallyPlaying) {
      return;
    }

    if (!state.current_session_id) {
      if (state.current_video_id !== vid) {
        clearVideoContext();
        setVideoContext(vid).then(() => startNewVideoSession());
      } else {
        startNewVideoSession();
      }
      return;
    }

    if (state.current_video_id !== vid) {
      flushPendingTick(videoEl, "navigate");
      stopVideoSession("navigate");
      clearVideoContext();
      setVideoContext(vid).then(() => startNewVideoSession());
      return;
    }

    if (!state.current_video_id) {
      maybeEmitContextMissing("no_video_id");
      state.last_tick_ms = null;
      return;
    }

    const t = nowMs();
    if (state.last_tick_ms == null) {
      state.last_tick_ms = t;
      return;
    }

    const delta = clampDeltaMs(t - state.last_tick_ms);
    if (delta < state.tick_interval_ms) return;

    sendEvent({
      ...baseEvent("watch_tick"),
      video_session_id: state.current_session_id,
      watch_ms_delta: Math.min(delta, state.tick_interval_ms),
      position_s: typeof videoEl.currentTime === "number" ? videoEl.currentTime : null,
      playback_rate: typeof videoEl.playbackRate === "number" ? videoEl.playbackRate : null
    });

    state.last_tick_ms = t;
  }, 500);
}

function stopTicking() {
  if (state.tick_timer) clearInterval(state.tick_timer);
  state.tick_timer = null;
}

function attachVideoListeners() {
  const video = document.querySelector("video");
  if (!video) return false;
  if (video.__ytTrackerBound) return true;
  video.__ytTrackerBound = true;

  async function ensureContextOnPlay() {
    const vid = getVideoIdFromUrl(location.href);
    if (!vid) {
      maybeEmitContextMissing("play_without_video_id");
      return;
    }

    if (state.current_video_id && state.current_video_id !== vid && state.current_session_id) {
      flushPendingTick(video, "navigate");
      stopVideoSession("navigate");
      clearVideoContext();
    }

    if (!state.current_video_id || state.current_video_id !== vid) {
      await setVideoContext(vid);
      startNewVideoSession();
    } else if (!state.current_session_id) {
      startNewVideoSession();
    }
  }

  video.addEventListener("play", async () => {
    state.is_visible = isVisibleNow();
    state.player_state = "playing";
    sendEvent({
      ...baseEvent("player_state_change"),
      new_state: "playing"
    });
    await ensureContextOnPlay();
    startTicking(video);
  });

  video.addEventListener("pause", () => {
    flushPendingTick(video, "pause");
    state.player_state = "paused";
    sendEvent({
      ...baseEvent("player_state_change"),
      new_state: "paused"
    });
    stopTicking();
    stopVideoSession("pause");
    state.last_tick_ms = null;
  });

  video.addEventListener("ended", () => {
    flushPendingTick(video, "ended");
    state.player_state = "ended";
    sendEvent({
      ...baseEvent("player_state_change"),
      new_state: "ended"
    });
    stopTicking();
    stopVideoSession("ended");
    state.last_tick_ms = null;
  });

  return true;
}

document.addEventListener("visibilitychange", () => {
  state.is_visible = isVisibleNow();
  sendEvent({
    ...baseEvent("visibility_change"),
    is_visible: state.is_visible
  });
});

function handleUrlChange(prevUrl, newUrl) {
  const prevVid = getVideoIdFromUrl(prevUrl);
  const newVid = getVideoIdFromUrl(newUrl);
  const videoEl = document.querySelector("video");

  if (prevVid && newVid && prevVid !== newVid) {
    flushPendingTick(videoEl, "navigate");
    if (state.current_session_id) stopVideoSession("navigate");
    clearVideoContext();
  } else if (prevVid && !newVid) {
    flushPendingTick(videoEl, "leave_watch");
    if (state.current_session_id) stopVideoSession("leave_watch");
    clearVideoContext();
    stopTicking();
    state.last_tick_ms = null;
  }
}

function tick() {
  attachVideoListeners();
  if (location.href !== state.last_url) {
    const prev = state.last_url;
    state.last_url = location.href;
    handleUrlChange(prev, state.last_url);
  }
  setTimeout(tick, 750);
}

(async function init() {
  await ensureClientSessionId();
  tick();
})();
