// ============================================================================
// Multithreaded Flipkart.com Direct HTTP Worker (`fetch-worker.js`)
// ============================================================================
// Runs inside Node.js `worker_threads` to concurrently fetch Flipkart search
// pages (`GET https://www.flipkart.com/search?...`) and extract all 40 product
// records per page directly from `window.__INITIAL_STATE__` JSON — zero browser
// overhead, 40 live product prices per HTTP call.
// ============================================================================

const { isMainThread, parentPort, workerData } = require("worker_threads");

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
];

function parseNumericPrice(val) {
  if (val == null) return null;
  if (typeof val === "number") return isNaN(val) || val <= 0 ? null : val;
  const cleaned = String(val).replace(/[^\d.]/g, "");
  const num = parseFloat(cleaned);
  return isNaN(num) || num <= 0 ? null : num;
}

/**
 * Extract structured product listings from Flipkart's SSR `window.__INITIAL_STATE__`.
 * Uses fast indexOf slicing (no slow backtracking regexes on 800KB HTML).
 */
function extractProductsFromFlipkartHtml(html) {
  if (!html || typeof html !== "string") return [];

  const marker = "window.__INITIAL_STATE__";
  const markerIdx = html.indexOf(marker);
  if (markerIdx === -1) return [];

  const jsonStart = html.indexOf("{", markerIdx + marker.length);
  if (jsonStart === -1) return [];

  const scriptEnd = html.indexOf("</script>", jsonStart);
  if (scriptEnd === -1) return [];

  let rawJson = html.slice(jsonStart, scriptEnd).trim();
  if (rawJson.endsWith(";")) rawJson = rawJson.slice(0, -1).trim();

  let state;
  try {
    state = JSON.parse(rawJson);
  } catch {
    return [];
  }

  const pageData = state?.pageDataV4?.page?.data;
  if (!pageData || typeof pageData !== "object") return [];

  const items = [];
  const seen = new Set();

  for (const slots of Object.values(pageData)) {
    if (!Array.isArray(slots)) continue;
    for (const slot of slots) {
      const products = slot?.widget?.data?.products;
      if (!Array.isArray(products)) continue;

      for (const entry of products) {
        const v = entry?.productInfo?.value;
        if (!v || !v.id || seen.has(v.id)) continue;

        const pricing = v.pricing;
        if (!pricing) continue;

        const finalPrice =
          parseNumericPrice(pricing.finalPrice?.value) ??
          parseNumericPrice(pricing.finalPrice?.decimalValue) ??
          parseNumericPrice(
            pricing.prices?.find((p) => !p.strikeOff)?.value
          );
        if (!finalPrice) continue;

        const mrp =
          parseNumericPrice(pricing.mrp?.value) ??
          parseNumericPrice(pricing.mrp?.decimalValue) ??
          parseNumericPrice(
            pricing.prices?.find((p) => p.strikeOff)?.value
          );

        let discountPct =
          typeof pricing.totalDiscount === "number"
            ? pricing.totalDiscount
            : null;
        if (mrp && mrp > finalPrice) {
          const calcPct = ((mrp - finalPrice) / mrp) * 100;
          if (discountPct == null || calcPct > discountPct) {
            discountPct = Math.round(calcPct * 10) / 10;
          }
        }

        const superTitle = v.titles?.superTitle?.trim() || "";
        const rawTitle =
          v.titles?.title?.trim() || v.titles?.newTitle?.trim() || v.id;
        const title =
          superTitle &&
          !rawTitle.toLowerCase().startsWith(superTitle.toLowerCase())
            ? `${superTitle} ${rawTitle}`
            : rawTitle;

        const availState = v.availability?.displayState || "IN_STOCK";
        const inStock = availState !== "OUT_OF_STOCK";

        // Keep clean short URL path (strip noisy tracking query params)
        let cleanUrl = `/p/itm?pid=${v.id}`;
        if (typeof v.baseUrl === "string" && v.baseUrl.startsWith("/")) {
          const [pathPart, qsPart] = v.baseUrl.split("?");
          cleanUrl = `${pathPart}?pid=${v.id}`;
          if (qsPart && !cleanUrl.includes("pid=")) {
            cleanUrl = `${pathPart}?pid=${v.id}`;
          }
        }

        seen.add(v.id);
        items.push({
          pid: v.id,
          title,
          currentPrice: finalPrice,
          mrp,
          discountPct,
          inStock,
          url: cleanUrl,
        });
      }
    }
  }

  return items;
}

/**
 * Fetch a single Flipkart search page URI via direct HTTP keep-alive fetch.
 */
async function fetchSearchPageDirectHttp(pageUri, options = {}) {
  const {
    timeoutMs = 15000,
    maxRetries = 2,
    uaIndex = 0,
  } = options;

  const normalizedUri = pageUri.startsWith("http")
    ? pageUri
    : `https://www.flipkart.com${pageUri.startsWith("/") ? "" : "/"}${pageUri}`;
  const ua = USER_AGENTS[uaIndex % USER_AGENTS.length];

  let lastStatus = 0;
  let retries = 0;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(normalizedUri, {
        method: "GET",
        headers: {
          "User-Agent": ua,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-IN,en;q=0.9",
          "Cache-Control": "no-cache",
          Referer: "https://www.flipkart.com/",
        },
        signal: controller.signal,
      });
      clearTimeout(timer);
      lastStatus = res.status;

      if (
        res.status === 429 ||
        res.status === 503 ||
        res.status === 502 ||
        res.status === 500
      ) {
        if (attempt < maxRetries) {
          retries++;
          const backoffMs =
            700 * (attempt + 1) + Math.floor(Math.random() * 600);
          await new Promise((r) => setTimeout(r, backoffMs));
          continue;
        }
        return { items: [], status: res.status, retries, throttled: true };
      }

      if (!res.ok) {
        return {
          items: [],
          status: res.status,
          retries,
          error: `HTTP ${res.status}`,
        };
      }

      const html = await res.text();
      const items = extractProductsFromFlipkartHtml(html);
      return { items, status: 200, retries, throttled: false };
    } catch (err) {
      clearTimeout(timer);
      if (attempt < maxRetries) {
        retries++;
        const backoffMs =
          350 * (attempt + 1) + Math.floor(Math.random() * 350);
        await new Promise((r) => setTimeout(r, backoffMs));
        continue;
      }
      return {
        items: [],
        status: lastStatus,
        retries,
        error: err.name === "AbortError" ? "Timeout" : err.message,
      };
    }
  }

  return { items: [], status: lastStatus, retries };
}

/**
 * Fetch a single custom Flipkart PID via direct search lookup (`/search?q=PID`).
 */
async function fetchSinglePidDirectHttp(pid, options = {}) {
  const res = await fetchSearchPageDirectHttp(
    `/search?q=${encodeURIComponent(pid)}`,
    options
  );
  const exact = (res.items || []).filter((item) => item.pid === pid);
  return { ...res, items: exact };
}

if (!isMainThread && parentPort) {
  (async () => {
    const {
      workerId = 0,
      mode = "searchPages",
      tasks = [],
      concurrency = 8,
      timeoutMs = 15000,
      maxRetries = 2,
    } = workerData || {};

    let nextIdx = 0;
    let tasksDone = 0;
    let totalRetries = 0;
    let totalErrors = 0;
    let totalThrottled = 0;
    const collectedByPid = new Map();

    const reportInterval = Math.max(10, Math.floor(tasks.length / 8));

    async function workerLoop(slotId) {
      while (nextIdx < tasks.length) {
        const myIdx = nextIdx++;
        const task = tasks[myIdx];
        const res =
          mode === "pids"
            ? await fetchSinglePidDirectHttp(task, {
                timeoutMs,
                maxRetries,
                uaIndex: workerId + slotId + myIdx,
              })
            : await fetchSearchPageDirectHttp(task, {
                timeoutMs,
                maxRetries,
                uaIndex: workerId + slotId + myIdx,
              });

        totalRetries += res.retries || 0;
        if (res.throttled) totalThrottled++;
        if (res.error) totalErrors++;
        if (res.items && res.items.length > 0) {
          for (const item of res.items) {
            if (!collectedByPid.has(item.pid)) {
              collectedByPid.set(item.pid, item);
            }
          }
        }

        tasksDone++;
        if (tasksDone % reportInterval === 0 || tasksDone === tasks.length) {
          parentPort.postMessage({
            type: "progress",
            workerId,
            tasksDone,
            totalTasks: tasks.length,
            itemsFound: collectedByPid.size,
          });
        }
      }
    }

    const poolSize = Math.min(concurrency, Math.max(1, tasks.length));
    await Promise.all(
      Array.from({ length: poolSize }, (_, slotId) => workerLoop(slotId))
    );

    parentPort.postMessage({
      type: "done",
      workerId,
      items: [...collectedByPid.values()],
      stats: {
        tasksDone,
        itemsFound: collectedByPid.size,
        retries: totalRetries,
        errors: totalErrors,
        throttled: totalThrottled,
      },
    });
  })().catch((err) => {
    parentPort.postMessage({
      type: "error",
      workerId: workerData?.workerId ?? 0,
      error: err.message,
    });
  });
}

module.exports = {
  extractProductsFromFlipkartHtml,
  fetchSearchPageDirectHttp,
  fetchSinglePidDirectHttp,
  parseNumericPrice,
};
