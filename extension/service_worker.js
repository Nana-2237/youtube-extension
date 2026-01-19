// service_worker.js (MV3) - batched ingest
const INGEST_URL = "http://localhost:4000/ingest";

// queue + flush controls
let queue = [];
let flushing = false;

// tuning knobs
const MAX_BATCH_SIZE = 50;     // events per request
const FLUSH_INTERVAL_MS = 750; // how often to try flushing

async function postBatch(events) {
  try {
    const res = await fetch(INGEST_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ events }),
      keepalive: true
    });

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`Ingest failed: ${res.status} ${text}`);
    }
    
    console.log(`✅ Sent ${events.length} events to ${INGEST_URL}`);
  } catch (err) {
    // Enhanced error logging for connection failures
    if (err.message.includes("Failed to fetch") || err.message.includes("network") || err.name === "TypeError") {
      console.error(`❌ CONNECTION ERROR: Cannot reach ${INGEST_URL}`);
      console.error(`   Make sure the Flask API server is running on port 4000`);
      console.error(`   Error:`, err);
    } else {
      console.error(`❌ Ingest error:`, err);
    }
    throw err;
  }
}

// flush loop
async function flushQueue() {
  if (flushing) return;
  if (queue.length === 0) return;

  flushing = true;
  try {
    const batch = queue.slice(0, MAX_BATCH_SIZE);
    await postBatch(batch);
    queue = queue.slice(batch.length);
    console.log(`📦 Queue after flush: ${queue.length} events remaining`);
  } catch (err) {
    // Keep events in queue; we'll retry next tick
    console.warn(`⚠️ flushQueue error (${queue.length} events queued):`, err.message);
    // Log queue size periodically so user knows events are accumulating
    if (queue.length % 10 === 0) {
      console.warn(`⚠️ ${queue.length} events queued - waiting for API server...`);
    }
  } finally {
    flushing = false;
  }
}

// periodic flusher (keeps behavior stable even if messages slow down)
setInterval(() => {
  flushQueue().catch(() => {});
}, FLUSH_INTERVAL_MS);

// receive events from content script
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg && msg.kind === "YT_EVENT" && msg.event) {
    queue.push(msg.event);

    // respond quickly so content script doesn't keep channels open too long
    sendResponse({ ok: true, queued: true, qsize: queue.length });

    // kick a flush soon (don’t await)
    flushQueue().catch(() => {});
    return true;
  }
});
