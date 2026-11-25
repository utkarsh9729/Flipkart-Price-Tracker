// ============================================================================
// Flipkart.com Price Error Detector (Multithreaded Direct HTTP Search Engine)
// ============================================================================
// Concurrently sweeps Flipkart.com search pages (`GET /search?...`, 40 live
// structured JSON products per HTTP request from `window.__INITIAL_STATE__`)
// across Node.js worker threads, tracks historical averages in `price-data.json`,
// detects pricing errors (>= 30% off historical average), and sends instant
// tiered alerts to Telegram groups.
//
// Usage:
//   node price-monitor.js                       # Run once
//   node price-monitor.js --loop --interval 5   # Run every 5 minutes
//   node price-monitor.js --shard 1/8 --shard-out shard-1.json
//   node price-monitor.js --merge-shards shards # Merge shard deltas
//   node price-monitor.js --add MOBXXXXXX       # Add custom Flipkart PID(s)
//   node price-monitor.js --remove MOBXXXXXX    # Remove a PID
//   node price-monitor.js --list                # List monitored PIDs & averages
// ============================================================================

const fs = require("fs");
const os = require("os");
const path = require("path");
const https = require("https");
const { Worker } = require("worker_threads");
const { fetchSearchPageDirectHttp } = require("./fetch-worker");

// ─── Configuration ──────────────────────────────────────────────────────────

const CONFIG = {
  // Telegram Bot credentials (set via env vars or configure below)
  telegramBotToken:
    process.env.TELEGRAM_BOT_TOKEN ||
    "5420527489:AAE1F1DDhqaiUHCu-kHauFfqhgEhOlG0aMI",
  // Primary Flipkart Telegram group (>= 30% discount)
  telegramChatId: process.env.TELEGRAM_CHAT_ID || "-5460461500",
  // Dedicated Flipkart Telegram group (>= 70% discount)
  midDiscountTelegramChatId:
    process.env.MID_DISCOUNT_TELEGRAM_CHAT_ID || "-5533342235",
  midDiscountMinPercent: parseFloat(
    process.env.MID_DISCOUNT_MIN_PERCENT || "70"
  ),
  // Optional ultra-high discount group (> 90% discount, if configured via env)
  highDiscountTelegramChatId: process.env.HIGH_DISCOUNT_TELEGRAM_CHAT_ID || "",
  highDiscountMinPercent: parseFloat(
    process.env.HIGH_DISCOUNT_MIN_PERCENT || "90"
  ),

  // Discount rule ONLY (no minimum price rule):
  // Alert when discount is >= 30% below historical average JSON price
  minDiscountPercent: parseFloat(process.env.MIN_DISCOUNT_PERCENT || "30"),

  // Max repeat alerts per product if price has not changed by >= 15% of avg price
  maxAlertsPerSamePrice: parseInt(
    process.env.MAX_ALERTS_PER_SAME_PRICE || "3",
    10
  ),
  minPriceChangePercentForNewAlert: parseFloat(
    process.env.MIN_PRICE_CHANGE_PERCENT || "15"
  ),

  // Data and SKU files
  dataFile: path.join(__dirname, "price-data.json"),
  skuFile: path.join(__dirname, "skus.json"),

  // Multithreaded HTTP Worker Pool settings
  workerThreads: parseInt(
    process.env.WORKERS || String(Math.min(Math.max(os.cpus().length, 2), 6)),
    10
  ),
  concurrencyPerWorker: parseInt(process.env.CONCURRENCY_PER_WORKER || "8", 10),
  requestTimeoutMs: parseInt(process.env.REQUEST_TIMEOUT_MS || "15000", 10),
  maxRetries: parseInt(process.env.MAX_RETRIES || "2", 10),

  // Max history entries retained per product (keep only last 10 prices)
  maxHistoryEntries: 10,

  // Minimum historical data points before average-based alert triggers
  minDataPointsForAlert: 2,
};

// ─── Helpers ────────────────────────────────────────────────────────────────

function formatPrice(num) {
  if (num == null || isNaN(num)) return "N/A";
  return (
    "₹" + Number(num).toLocaleString("en-IN", { maximumFractionDigits: 2 })
  );
}

function extractHistoryPrices(history) {
  if (!Array.isArray(history) || history.length === 0) return [];
  const prices = [];
  for (const h of history) {
    const p = typeof h === "number" ? h : h?.price;
    if (p != null && p > 0 && !isNaN(p)) {
      prices.push(p);
    }
  }
  return prices;
}

function calculateAverage(history) {
  const prices = extractHistoryPrices(history);
  if (prices.length === 0) return null;
  const avg = prices.reduce((sum, p) => sum + p, 0) / prices.length;
  return Math.round(avg * 100) / 100;
}

function calculateMedian(history) {
  const prices = extractHistoryPrices(history).sort((a, b) => a - b);
  if (prices.length === 0) return null;
  const mid = Math.floor(prices.length / 2);
  const med =
    prices.length % 2 !== 0
      ? prices[mid]
      : (prices[mid - 1] + prices[mid]) / 2;
  return Math.round(med * 100) / 100;
}

function calculateMinMax(history) {
  const prices = extractHistoryPrices(history);
  if (prices.length === 0) return { min: null, max: null };
  return {
    min: Math.min(...prices),
    max: Math.max(...prices),
  };
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function normalizeProductTitle(title) {
  if (!title) return "";
  return String(title).replace(/\s+/g, " ").trim().toLowerCase();
}

function compactProductRecord(product) {
  if (!product || typeof product !== "object") return product;
  const prices = extractHistoryPrices(product.history).slice(
    -CONFIG.maxHistoryEntries
  );
  const compact = {
    pid: product.pid,
    title: product.title,
    mrp: product.mrp ?? null,
    currentPrice: product.currentPrice ?? null,
    averagePrice: product.averagePrice ?? null,
    medianPrice: product.medianPrice ?? null,
    minPrice: product.minPrice ?? null,
    maxPrice: product.maxPrice ?? null,
    history: prices,
    lastChecked: product.lastChecked ?? null,
  };
  if (product.url) compact.url = product.url;
  if (product.category) compact.category = product.category;
  if (product.alertCount) compact.alertCount = product.alertCount;
  if (product.lastAlertPrice != null)
    compact.lastAlertPrice = product.lastAlertPrice;
  if (product.lastAlertAvgPrice != null)
    compact.lastAlertAvgPrice = product.lastAlertAvgPrice;
  if (product.lastAlertAt) compact.lastAlertAt = product.lastAlertAt;
  if (product.samePriceAlertCount)
    compact.samePriceAlertCount = product.samePriceAlertCount;
  return compact;
}

function loadData() {
  try {
    if (fs.existsSync(CONFIG.dataFile)) {
      const raw = fs.readFileSync(CONFIG.dataFile, "utf-8");
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed.products === "object") {
        return parsed;
      }
    }
  } catch (err) {
    console.error(
      "⚠️  Error reading price-data.json, starting fresh:",
      err.message
    );
  }
  return { products: {}, lastRun: null, runCount: 0 };
}

function saveData(data) {
  if (data && data.products) {
    for (const [pid, product] of Object.entries(data.products)) {
      data.products[pid] = compactProductRecord(product);
    }
  }
  data.lastRun = new Date().toISOString();
  data.runCount = (data.runCount || 0) + 1;
  fs.writeFileSync(CONFIG.dataFile, JSON.stringify(data), "utf-8");
}

function loadSkus() {
  try {
    if (fs.existsSync(CONFIG.skuFile)) {
      const raw = fs.readFileSync(CONFIG.skuFile, "utf-8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return { categories: {}, searchPages: [], customSkus: [], skus: parsed };
      }
      if (parsed && Array.isArray(parsed.skus)) {
        return {
          categories: parsed.categories || {},
          searchPages: parsed.searchPages || [],
          customSkus: parsed.customSkus || [],
          skus: parsed.skus,
        };
      }
    }
  } catch (err) {
    console.error("⚠️  Error reading skus.json:", err.message);
  }
  return { categories: {}, searchPages: [], customSkus: [], skus: [] };
}

function saveSkus(skuData) {
  fs.writeFileSync(CONFIG.skuFile, JSON.stringify(skuData, null, 2), "utf-8");
}

/**
 * Repeat-alert throttle:
 *   - Max `maxAlerts` (default 3) alerts while price stays within `< 15%` of avg.
 *   - Starts a new cycle when price changes by `>= 15%` of reference avg.
 */
function evaluateRepeatAlertThrottle(
  tracker,
  currentPrice,
  comparisonAvg,
  maxAlerts = CONFIG.maxAlertsPerSamePrice,
  minChangePct = CONFIG.minPriceChangePercentForNewAlert
) {
  const prevPrice = tracker?.lastAlertPrice ?? null;
  const prevAvg = tracker?.lastAlertAvgPrice ?? null;
  const prevCount = tracker?.samePriceAlertCount || 0;

  if (prevPrice == null || prevCount === 0) {
    return {
      allow: true,
      isNewCycle: true,
      priceChangePctOfAvg: null,
      alertNumber: 1,
      nextState: {
        lastAlertPrice: currentPrice,
        lastAlertAvgPrice: comparisonAvg,
        samePriceAlertCount: 1,
      },
    };
  }

  const refAvg = prevAvg && prevAvg > 0 ? prevAvg : comparisonAvg;
  const priceChangePctOfAvg =
    refAvg && refAvg > 0
      ? (Math.abs(currentPrice - prevPrice) / refAvg) * 100
      : Infinity;

  if (priceChangePctOfAvg >= minChangePct) {
    return {
      allow: true,
      isNewCycle: true,
      priceChangePctOfAvg,
      alertNumber: 1,
      nextState: {
        lastAlertPrice: currentPrice,
        lastAlertAvgPrice: comparisonAvg,
        samePriceAlertCount: 1,
      },
    };
  }

  if (prevCount >= maxAlerts) {
    return {
      allow: false,
      isNewCycle: false,
      priceChangePctOfAvg,
      alertNumber: prevCount,
      nextState: {
        lastAlertPrice: prevPrice,
        lastAlertAvgPrice: refAvg,
        samePriceAlertCount: prevCount,
      },
    };
  }

  const nextCount = prevCount + 1;
  return {
    allow: true,
    isNewCycle: false,
    priceChangePctOfAvg,
    alertNumber: nextCount,
    nextState: {
      lastAlertPrice: prevPrice,
      lastAlertAvgPrice: refAvg,
      samePriceAlertCount: nextCount,
    },
  };
}

// ─── Telegram Notification ──────────────────────────────────────────────────

function sendTelegram(message, targetChatId = CONFIG.telegramChatId) {
  return new Promise((resolve, reject) => {
    const chatId = targetChatId || CONFIG.telegramChatId;
    if (
      !CONFIG.telegramBotToken ||
      CONFIG.telegramBotToken === "YOUR_BOT_TOKEN_HERE" ||
      !chatId ||
      chatId === "YOUR_FLIPKART_CHAT_ID_HERE" ||
      chatId === "YOUR_CHAT_ID_HERE"
    ) {
      console.log(
        "\n📱 [Telegram Alert Preview — configure TELEGRAM_CHAT_ID to send live]:"
      );
      console.log(message.replace(/<[^>]+>/g, ""));
      return resolve({ skipped: true });
    }

    const payload = JSON.stringify({
      chat_id: chatId,
      text: message,
      parse_mode: "HTML",
      disable_web_page_preview: false,
    });

    const urlObj = new URL(
      `https://api.telegram.org/bot${CONFIG.telegramBotToken}/sendMessage`
    );

    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(payload),
      },
    };

    const req = https.request(options, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        if (res.statusCode === 200) {
          console.log(`✅ Telegram alert sent successfully (chat: ${chatId}).`);
          resolve({ sent: true, chatId });
        } else if (res.statusCode === 429) {
          let retryAfter = 35;
          try {
            const parsed = JSON.parse(body);
            retryAfter = (parsed?.parameters?.retry_after || 30) + 2;
          } catch {}
          console.log(
            `⏳ Telegram rate limit (429) — waiting ${retryAfter}s and retrying...`
          );
          setTimeout(() => {
            sendTelegram(message, chatId).then(resolve).catch(reject);
          }, retryAfter * 1000);
        } else {
          let migratedId = null;
          try {
            const parsed = JSON.parse(body);
            migratedId = parsed?.parameters?.migrate_to_chat_id || null;
          } catch {}
          if (migratedId && String(migratedId) !== String(chatId)) {
            console.log(
              `🔄 Telegram group migrated to supergroup ${migratedId} — retrying...`
            );
            return sendTelegram(message, String(migratedId))
              .then(resolve)
              .catch(reject);
          }
          console.error("❌ Telegram API error:", res.statusCode, body);
          reject(new Error(`Telegram ${res.statusCode}: ${body}`));
        }
      });
    });

    req.on("error", (err) => {
      console.error("❌ Telegram request error:", err.message);
      reject(err);
    });

    req.write(payload);
    req.end();
  });
}

function resolveAlertChatIds(dropPct) {
  const chats = [];
  const addChat = (id) => {
    if (!id) return;
    const s = String(id).trim();
    if (
      s &&
      s !== "YOUR_FLIPKART_CHAT_ID_HERE" &&
      s !== "YOUR_CHAT_ID_HERE" &&
      !chats.includes(s)
    ) {
      chats.push(s);
    }
  };
  if (dropPct > CONFIG.highDiscountMinPercent) {
    addChat(CONFIG.highDiscountTelegramChatId);
  }
  if (dropPct >= CONFIG.midDiscountMinPercent) {
    addChat(CONFIG.midDiscountTelegramChatId);
  }
  addChat(CONFIG.telegramChatId);
  return chats;
}

function buildAlertMessage(alert) {
  const {
    product,
    currentPrice,
    avgPrice,
    medianPrice,
    minMax,
    reason,
    alertNumber,
  } = alert;
  const dropFromAvg =
    avgPrice && avgPrice > 0
      ? (((avgPrice - currentPrice) / avgPrice) * 100).toFixed(1)
      : "0.0";
  const productUrl = product.url
    ? `https://www.flipkart.com${product.url}`
    : `https://www.flipkart.com/p/itm?pid=${product.pid}`;
  const alertTag = alertNumber
    ? `🔔 Notification: <b>${alertNumber}/${CONFIG.maxAlertsPerSamePrice}</b> (muted until ≥${CONFIG.minPriceChangePercentForNewAlert}% price change vs avg)\n`
    : "";

  return (
    `🚨 <b>FLIPKART PRICE ERROR DETECTED!</b>\n\n` +
    `📦 <b>${escapeHtml(product.title)}</b>\n` +
    `🔖 PID: <code>${product.pid}</code>\n` +
    `⚡ Trigger: ${escapeHtml(reason)}\n` +
    alertTag +
    `\n` +
    `🏷️ Current Price: <b>${formatPrice(currentPrice)}</b>\n` +
    `📊 Historical Avg Price (JSON): <b>${formatPrice(avgPrice)}</b>\n` +
    `📈 Median Price: ${formatPrice(medianPrice)}\n` +
    `📉 Historical Range: ${formatPrice(minMax.min)} – ${formatPrice(minMax.max)}\n` +
    `⬇️ Discount vs Historical Price: <b>${dropFromAvg}% off</b>\n\n` +
    `📊 Data Points: ${product.history.length}\n` +
    `🔗 <a href="${productUrl}">Buy Now on Flipkart.com</a>\n` +
    `⏰ ${new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })}`
  );
}

// ─── Multithreaded Flipkart Fetcher ─────────────────────────────────────────

async function fetchViaWorkerPool(tasks, mode = "searchPages") {
  if (!tasks || tasks.length === 0) return new Map();

  const numWorkers = Math.min(
    Math.max(1, CONFIG.workerThreads || 4),
    Math.max(1, tasks.length)
  );
  const concurrencyPerWorker = CONFIG.concurrencyPerWorker || 8;

  const workerTasks = Array.from({ length: numWorkers }, () => []);
  for (let i = 0; i < tasks.length; i++) {
    workerTasks[i % numWorkers].push(tasks[i]);
  }

  console.log(
    `\n⚡ [Multithreaded Flipkart Pool:${mode}] Launching ${numWorkers} worker(s) × ${concurrencyPerWorker} concurrency (${tasks.length.toLocaleString("en-IN")} tasks)...`
  );

  const workerPath = path.join(__dirname, "fetch-worker.js");
  const resultsByPid = new Map();
  const progressByWorker = new Array(numWorkers).fill(0);
  const startMs = Date.now();
  let lastLogMs = 0;
  let totalRetries = 0;
  let totalErrors = 0;
  let totalThrottled = 0;

  await Promise.all(
    workerTasks.map(
      (wt, workerId) =>
        new Promise((resolve, reject) => {
          const worker = new Worker(workerPath, {
            workerData: {
              workerId,
              mode,
              tasks: wt,
              concurrency: concurrencyPerWorker,
              timeoutMs: CONFIG.requestTimeoutMs,
              maxRetries: CONFIG.maxRetries,
            },
          });

          worker.on("message", (msg) => {
            if (msg.type === "progress") {
              progressByWorker[msg.workerId] = msg.tasksDone;
              const now = Date.now();
              if (now - lastLogMs > 2500) {
                lastLogMs = now;
                const done = progressByWorker.reduce((a, b) => a + b, 0);
                const pct = ((done / tasks.length) * 100).toFixed(0);
                const elapsedSec = ((now - startMs) / 1000).toFixed(1);
                console.log(
                  `   ⚡ Progress: ${done}/${tasks.length} (${pct}%) | ${elapsedSec}s elapsed`
                );
              }
            } else if (msg.type === "done") {
              const scrapedAt = new Date().toISOString();
              for (const item of msg.items || []) {
                if (!resultsByPid.has(item.pid)) {
                  resultsByPid.set(item.pid, {
                    ...item,
                    scrapedAt,
                    unavailable: !item.inStock || item.currentPrice == null,
                  });
                }
              }
              totalRetries += msg.stats?.retries || 0;
              totalErrors += msg.stats?.errors || 0;
              totalThrottled += msg.stats?.throttled || 0;
              resolve();
            } else if (msg.type === "error") {
              reject(new Error(`Worker ${msg.workerId}: ${msg.error}`));
            }
          });

          worker.on("error", reject);
          worker.on("exit", (code) => {
            if (code !== 0) {
              reject(new Error(`Worker ${workerId} exited with code ${code}`));
            }
          });
        })
    )
  );

  const elapsedSec = ((Date.now() - startMs) / 1000).toFixed(2);
  console.log(
    `   ✅ Worker pool finished in ${elapsedSec}s | Unique products fetched: ${resultsByPid.size.toLocaleString("en-IN")} | Retries: ${totalRetries} | 429/503: ${totalThrottled} | Errors: ${totalErrors}`
  );

  return resultsByPid;
}

// ─── Core: Main Monitoring Logic ────────────────────────────────────────────

async function runMonitor(options = {}) {
  const {
    forceTestAlert = false,
    shardIndex = null,
    shardTotal = null,
    shardOut = null,
  } = options;

  const runStartMs = Date.now();
  const skuData = loadSkus();
  const allSkus = [
    ...new Set(
      (skuData.skus || []).map((s) => s.trim().toUpperCase()).filter(Boolean)
    ),
  ];
  const allSearchPages = [...new Set(skuData.searchPages || [])];
  const allCustomSkus = [
    ...new Set(
      (skuData.customSkus || [])
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean)
    ),
  ];

  if (allSkus.length === 0 && allSearchPages.length === 0) {
    console.error(
      "❌ No Flipkart SKUs configured in skus.json! Run `npm run harvest` or `node price-monitor.js --add MOBXXXXX`."
    );
    process.exit(1);
  }

  let searchPages = allSearchPages;
  let customSkus = allCustomSkus;
  let shardLabel = "";

  if (shardIndex != null && shardTotal != null && shardTotal > 1) {
    const pageChunk = Math.ceil(allSearchPages.length / shardTotal);
    const pStart = (shardIndex - 1) * pageChunk;
    const pEnd = Math.min(pStart + pageChunk, allSearchPages.length);
    searchPages = allSearchPages.slice(pStart, pEnd);

    const customChunk = Math.ceil(allCustomSkus.length / shardTotal);
    const cStart = (shardIndex - 1) * customChunk;
    const cEnd = Math.min(cStart + customChunk, allCustomSkus.length);
    customSkus = allCustomSkus.slice(cStart, cEnd);

    shardLabel = ` [Shard ${shardIndex}/${shardTotal}: ${searchPages.length} search pages]`;
  }

  const trackedSkuSet = new Set(allSkus);

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  🛒 Flipkart.com Price Error Detector (Multithreaded HTTP)");
  console.log(
    `  📦 Catalog: ${allSkus.length.toLocaleString("en-IN")} PIDs | Sweeping ${searchPages.length.toLocaleString("en-IN")} search pages${shardLabel}`
  );
  console.log(
    `  ⚡ Engine: ${CONFIG.workerThreads} Worker Threads × ${CONFIG.concurrencyPerWorker} Concurrency (40 SKUs/page)`
  );
  console.log(
    `  🎯 Discount Rule: >= ${CONFIG.minDiscountPercent}% off (No minimum price rule)`
  );
  if (CONFIG.midDiscountTelegramChatId) {
    console.log(
      `  🔥 Mid-Discount Group:  >= ${CONFIG.midDiscountMinPercent}% off -> chat ${CONFIG.midDiscountTelegramChatId}`
    );
  }
  if (CONFIG.highDiscountTelegramChatId) {
    console.log(
      `  🚨 High-Discount Group: > ${CONFIG.highDiscountMinPercent}% off -> chat ${CONFIG.highDiscountTelegramChatId}`
    );
  }
  console.log(
    `  🔕 Repeat Limit: Max ${CONFIG.maxAlertsPerSamePrice} alerts/product until price changes by ≥${CONFIG.minPriceChangePercentForNewAlert}% of avg price`
  );
  console.log(
    `  ⏰ ${new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })}`
  );
  console.log("═══════════════════════════════════════════════════════════");

  // Quick connectivity probe
  const probeUri = searchPages[0] || "/search?q=smartphone&page=1";
  const probe = await fetchSearchPageDirectHttp(probeUri, {
    timeoutMs: 12000,
    maxRetries: 1,
  });
  if (probe.status !== 200 || probe.items.length === 0) {
    console.error(
      `❌ Flipkart probe failed (status=${probe.status}, items=${probe.items.length}).`
    );
    process.exit(42);
  }
  console.log(
    `🌐 Flipkart Direct HTTP probe OK (${probe.items.length} products on sample page)!`
  );

  const data = loadData();
  if (!data.titleAlertTracker || typeof data.titleAlertTracker !== "object") {
    data.titleAlertTracker = {};
  }

  const fetchedResults = await fetchViaWorkerPool(searchPages, "searchPages");

  if (customSkus.length > 0) {
    const customResults = await fetchViaWorkerPool(customSkus, "pids");
    for (const [pid, item] of customResults.entries()) {
      fetchedResults.set(pid, item);
    }
  }

  const alerts = [];
  const alertedTitlesThisRun = new Set();
  const updatedProducts = {};
  const titleAlertUpdates = {};

  let checkedCount = 0;
  let errorCount = 0;
  let suppressedCount = 0;
  let skippedCount = 0;

  for (const [pid, result] of fetchedResults.entries()) {
    // Only track products that are in our monitored SKU catalog
    if (!trackedSkuSet.has(pid)) continue;

    if (!data.products[pid]) {
      data.products[pid] = {
        pid,
        title: result.title,
        mrp: result.mrp,
        currentPrice: result.currentPrice,
        averagePrice: null,
        medianPrice: null,
        minPrice: null,
        maxPrice: null,
        url: result.url,
        history: [],
        lastChecked: null,
        alertCount: 0,
        lastAlertPrice: null,
        lastAlertAvgPrice: null,
        lastAlertAt: null,
        samePriceAlertCount: 0,
      };
    }

    const product = data.products[pid];
    const priorAvg = calculateAverage(product.history);
    const priorCount = extractHistoryPrices(product.history).length;

    if (result.title) product.title = result.title;
    if (result.mrp) product.mrp = result.mrp;
    if (result.url) product.url = result.url;
    product.currentPrice = result.currentPrice;
    product.lastChecked = result.scrapedAt;

    if (result.unavailable || result.currentPrice == null) {
      skippedCount++;
      continue;
    }

    checkedCount++;

    product.history = extractHistoryPrices(product.history);
    product.history.push(result.currentPrice);
    if (product.history.length > CONFIG.maxHistoryEntries) {
      product.history = product.history.slice(-CONFIG.maxHistoryEntries);
    }

    product.averagePrice = calculateAverage(product.history);
    product.medianPrice = calculateMedian(product.history);
    const minMax = calculateMinMax(product.history);
    product.minPrice = minMax.min;
    product.maxPrice = minMax.max;

    const cur = result.currentPrice;
    const avg = product.averagePrice;
    const comparisonAvg = priorAvg ?? avg;

    let isError = false;
    let errorReason = "";
    let dropPctNum = 0;

    if (
      comparisonAvg != null &&
      comparisonAvg > 0 &&
      priorCount + 1 >= CONFIG.minDataPointsForAlert
    ) {
      dropPctNum = ((comparisonAvg - cur) / comparisonAvg) * 100;
      if (dropPctNum >= CONFIG.minDiscountPercent) {
        const dropPct = dropPctNum.toFixed(1);
        isError = true;
        errorReason = `Discount ${dropPct}% (>= ${CONFIG.minDiscountPercent}%) below historical JSON price (${formatPrice(comparisonAvg)})`;
      }
    }

    if (isError) {
      const normTitle = normalizeProductTitle(product.title);
      const pidDecision = evaluateRepeatAlertThrottle(
        product,
        cur,
        comparisonAvg
      );
      const titleTracker = normTitle ? data.titleAlertTracker[normTitle] : null;
      const titleDecision = normTitle
        ? evaluateRepeatAlertThrottle(titleTracker, cur, comparisonAvg)
        : { allow: true };

      if (!pidDecision.allow) {
        suppressedCount++;
        console.log(
          `⏭️  [${pid}] Skipping repeat alert: price (${formatPrice(cur)}) changed only ${pidDecision.priceChangePctOfAvg.toFixed(1)}% (< ${CONFIG.minPriceChangePercentForNewAlert}% of avg) and already sent ${pidDecision.alertNumber}/${CONFIG.maxAlertsPerSamePrice} times.`
        );
      } else if (normTitle && alertedTitlesThisRun.has(normTitle)) {
        suppressedCount++;
      } else if (normTitle && !titleDecision.allow) {
        suppressedCount++;
      } else {
        const nowIso = new Date().toISOString();
        product.lastAlertPrice = pidDecision.nextState.lastAlertPrice;
        product.lastAlertAvgPrice = pidDecision.nextState.lastAlertAvgPrice;
        product.samePriceAlertCount = pidDecision.nextState.samePriceAlertCount;
        product.lastAlertAt = nowIso;
        product.alertCount = (product.alertCount || 0) + 1;

        const effectiveAlertNum = normTitle
          ? Math.max(pidDecision.alertNumber, titleDecision.alertNumber)
          : pidDecision.alertNumber;

        if (normTitle) {
          alertedTitlesThisRun.add(normTitle);
          const titleState = {
            lastAlertPrice: titleDecision.nextState.lastAlertPrice,
            lastAlertAvgPrice: titleDecision.nextState.lastAlertAvgPrice,
            samePriceAlertCount: titleDecision.nextState.samePriceAlertCount,
            lastAlertAt: nowIso,
            lastPid: pid,
          };
          data.titleAlertTracker[normTitle] = titleState;
          titleAlertUpdates[normTitle] = titleState;
        }

        console.log(
          `\n🚨 [${pid}] FLIPKART PRICE ERROR DETECTED: ${errorReason} (Alert ${effectiveAlertNum}/${CONFIG.maxAlertsPerSamePrice})`
        );
        console.log(`   📦 ${product.title?.substring(0, 72) || pid}`);
        console.log(
          `   💰 Current: ${formatPrice(cur)} | Avg: ${formatPrice(avg)} | MRP: ${formatPrice(product.mrp)}`
        );
        errorCount++;

        alerts.push({
          product,
          currentPrice: cur,
          avgPrice: comparisonAvg,
          medianPrice: product.medianPrice,
          mrp: product.mrp,
          minMax,
          reason: errorReason,
          alertNumber: effectiveAlertNum,
          dropPct: dropPctNum,
        });
      }
    }

    if (shardOut) {
      updatedProducts[pid] = compactProductRecord(product);
    }
  }

  if (shardOut) {
    fs.mkdirSync(path.dirname(path.resolve(shardOut)), { recursive: true });
    fs.writeFileSync(
      shardOut,
      JSON.stringify({
        shardIndex,
        shardTotal,
        completedAt: new Date().toISOString(),
        checkedCount,
        skippedCount,
        errorCount,
        suppressedCount,
        updatedProducts,
        titleAlertUpdates,
      }),
      "utf-8"
    );
    console.log(
      `\n💾 Shard delta (${Object.keys(updatedProducts).length} products) saved to ${shardOut}`
    );
  } else {
    saveData(data);
    console.log(`\n💾 Updated prices & averages saved to ${CONFIG.dataFile}`);
  }

  if (forceTestAlert && alerts.length === 0) {
    const firstPid = allSkus.find((id) => data.products[id]?.currentPrice != null);
    if (firstPid) {
      const p = data.products[firstPid];
      alerts.push({
        product: p,
        currentPrice: p.currentPrice,
        avgPrice: p.averagePrice,
        medianPrice: p.medianPrice,
        mrp: p.mrp,
        minMax: { min: p.minPrice, max: p.maxPrice },
        reason: "Test Run Verification (Live Flipkart Direct HTTP Data)",
        dropPct: 50,
      });
    }
  }

  if (alerts.length > 0) {
    console.log(
      `\n📱 Sending ${alerts.length} price error alert(s) to Telegram...`
    );
    for (const alert of alerts) {
      const msg = buildAlertMessage(alert);
      const dropPct =
        typeof alert.dropPct === "number"
          ? alert.dropPct
          : alert.avgPrice && alert.avgPrice > 0
            ? ((alert.avgPrice - alert.currentPrice) / alert.avgPrice) * 100
            : 0;
      const targetChats = resolveAlertChatIds(dropPct);
      for (const chatId of targetChats) {
        try {
          await sendTelegram(msg, chatId);
          await new Promise((r) => setTimeout(r, 400));
        } catch (err) {
          console.error(
            `   ❌ Failed to send Telegram alert to ${chatId}:`,
            err.message
          );
        }
      }
    }
  } else {
    console.log("\n✅ No pricing errors detected in this run.");
  }

  const totalSec = ((Date.now() - runStartMs) / 1000).toFixed(1);
  console.log("\n══════════════ Run Summary ══════════════════════════════");
  console.log(`  Catalog PIDs:        ${allSkus.length.toLocaleString("en-IN")}${shardLabel}`);
  console.log(`  Products priced:     ${checkedCount.toLocaleString("en-IN")}`);
  console.log(`  Products skipped:    ${skippedCount}`);
  console.log(`  Price errors sent:   ${errorCount}`);
  console.log(`  Repeats suppressed:  ${suppressedCount}`);
  console.log(`  Total duration:      ${totalSec}s`);
  if (!shardOut) {
    console.log(`  Total runs logged:   ${data.runCount}`);
  }
  console.log("═══════════════════════════════════════════════════════════\n");

  return { data, alerts };
}

function mergeShardFiles(shardDir) {
  const data = loadData();
  if (!data.titleAlertTracker || typeof data.titleAlertTracker !== "object") {
    data.titleAlertTracker = {};
  }

  if (!fs.existsSync(shardDir)) {
    console.error(`❌ Shard directory not found: ${shardDir}`);
    process.exit(1);
  }

  const files = fs
    .readdirSync(shardDir)
    .filter((f) => f.endsWith(".json"))
    .sort();

  if (files.length === 0) {
    console.log("ℹ️  No shard JSON files found to merge.");
    return;
  }

  let totalUpdated = 0;
  let totalErrors = 0;
  let totalSuppressed = 0;

  for (const file of files) {
    const fullPath = path.join(shardDir, file);
    const shard = JSON.parse(fs.readFileSync(fullPath, "utf-8"));
    const products = shard.updatedProducts || {};
    for (const [pid, prod] of Object.entries(products)) {
      data.products[pid] = prod;
      totalUpdated++;
    }
    const titleUpdates = shard.titleAlertUpdates || {};
    for (const [normTitle, state] of Object.entries(titleUpdates)) {
      const existing = data.titleAlertTracker[normTitle];
      if (
        !existing ||
        (state.lastAlertAt &&
          (!existing.lastAlertAt || state.lastAlertAt >= existing.lastAlertAt))
      ) {
        data.titleAlertTracker[normTitle] = state;
      }
    }
    totalErrors += shard.errorCount || 0;
    totalSuppressed += shard.suppressedCount || 0;
    console.log(
      `   🧩 Merged ${file}: ${Object.keys(products).length} products (alerts: ${shard.errorCount || 0}, suppressed: ${shard.suppressedCount || 0})`
    );
  }

  saveData(data);
  console.log(
    `\n✅ Merged ${files.length} shard(s): ${totalUpdated.toLocaleString("en-IN")} products updated | Alerts: ${totalErrors} | Suppressed: ${totalSuppressed} | Total runs: ${data.runCount}`
  );
}

// ─── CLI: SKU Management ────────────────────────────────────────────────────

function handleBulkAdd(pids) {
  const skuData = loadSkus();
  let addedCount = 0;
  for (const raw of pids) {
    const cleaned = raw.trim().toUpperCase();
    if (cleaned && !skuData.skus.includes(cleaned)) {
      skuData.skus.push(cleaned);
      if (!skuData.customSkus.includes(cleaned)) {
        skuData.customSkus.push(cleaned);
      }
      addedCount++;
      console.log(`  ✅ Added PID: ${cleaned}`);
    } else if (skuData.skus.includes(cleaned)) {
      console.log(`  ⏭️  Already monitored: ${cleaned}`);
    }
  }
  saveSkus(skuData);
  console.log(
    `\n✅ Added ${addedCount} new PID(s). Total monitored: ${skuData.skus.length}`
  );
}

function handleRemoveSku(pid) {
  const skuData = loadSkus();
  const idx = skuData.skus.indexOf(pid);
  if (idx === -1) {
    console.log(`ℹ️  PID ${pid} is not in skus.json.`);
    return;
  }
  skuData.skus.splice(idx, 1);
  skuData.customSkus = (skuData.customSkus || []).filter((x) => x !== pid);
  saveSkus(skuData);
  console.log(`✅ Removed PID: ${pid} (remaining: ${skuData.skus.length})`);
}

function handleListSkus() {
  const skuData = loadSkus();
  const data = loadData();
  console.log("\n📋 Monitored Flipkart PIDs (showing first 50):");
  console.log(
    "──────────────────────────────────────────────────────────────────────"
  );
  for (const pid of skuData.skus.slice(0, 50)) {
    const p = data.products[pid];
    if (p) {
      console.log(
        `  • ${pid} | Current: ${formatPrice(p.currentPrice)} | Avg: ${formatPrice(p.averagePrice)} | MRP: ${formatPrice(p.mrp)} | Runs: ${p.history?.length || 0}\n    ${p.title?.substring(0, 75) || ""}`
      );
    } else {
      console.log(`  • ${pid} | (not checked yet)`);
    }
  }
  console.log(
    `\nTotal: ${skuData.skus.length.toLocaleString("en-IN")} PID(s) across ${Object.keys(skuData.categories || {}).length} categories (${(skuData.searchPages || []).length} search pages)\n`
  );
}

// ─── CLI Entrypoint ─────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);

  const addIdx = args.indexOf("--add");
  if (addIdx >= 0) {
    const pids = args.slice(addIdx + 1).filter((a) => !a.startsWith("--"));
    if (pids.length === 0) {
      console.error("Usage: node price-monitor.js --add MOBXXXXX [MOBYYYYY ...]");
      process.exit(1);
    }
    handleBulkAdd(pids);
    return;
  }

  const removeIdx = args.indexOf("--remove");
  if (removeIdx >= 0) {
    const pid = args[removeIdx + 1];
    if (!pid) {
      console.error("Usage: node price-monitor.js --remove MOBXXXXX");
      process.exit(1);
    }
    handleRemoveSku(pid.trim().toUpperCase());
    return;
  }

  if (args.includes("--list")) {
    handleListSkus();
    return;
  }

  if (args.includes("--test-telegram")) {
    console.log("📱 Sending test message to Flipkart Telegram group...");
    await sendTelegram(
      `✅ <b>Flipkart.com Price Error Detector Connected!</b>\n\n` +
        `🔬 Engine: Multithreaded Flipkart Direct HTTP Search Engine\n` +
        `🎯 Rule: Alert when discount &gt;= ${CONFIG.minDiscountPercent}% (No price rule)\n` +
        `⏰ ${new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" })}`
    );
    return;
  }

  const mergeIdx = args.indexOf("--merge-shards");
  if (mergeIdx >= 0) {
    const shardDir = args[mergeIdx + 1];
    if (!shardDir) {
      console.error("Usage: node price-monitor.js --merge-shards <dir>");
      process.exit(1);
    }
    mergeShardFiles(shardDir);
    return;
  }

  const workersIdx = args.indexOf("--workers");
  if (workersIdx >= 0 && args[workersIdx + 1]) {
    CONFIG.workerThreads = Math.max(
      1,
      parseInt(args[workersIdx + 1], 10) || CONFIG.workerThreads
    );
  }

  let shardIndex = null;
  let shardTotal = null;
  const shardIdx = args.indexOf("--shard");
  if (shardIdx >= 0 && args[shardIdx + 1]) {
    const parts = args[shardIdx + 1].split("/");
    shardIndex = parseInt(parts[0], 10);
    shardTotal = parseInt(parts[1], 10);
    if (
      !shardIndex ||
      !shardTotal ||
      shardIndex < 1 ||
      shardIndex > shardTotal
    ) {
      console.error("Usage: --shard <1..N>/<N> (e.g. --shard 1/8)");
      process.exit(1);
    }
  }

  const shardOutIdx = args.indexOf("--shard-out");
  const shardOut = shardOutIdx >= 0 ? args[shardOutIdx + 1] : null;
  const forceTestAlert = args.includes("--send-test-alert");
  const isLoop = args.includes("--loop");
  const intervalIdx = args.indexOf("--interval");
  const intervalMin =
    intervalIdx >= 0 ? parseInt(args[intervalIdx + 1], 10) || 5 : 5;

  const runOpts = {
    forceTestAlert,
    shardIndex,
    shardTotal,
    shardOut,
  };

  if (isLoop) {
    console.log(
      `🔄 Continuous mode: checking Flipkart every ${intervalMin} minute(s). Ctrl+C to stop.\n`
    );
    while (true) {
      try {
        await runMonitor(runOpts);
      } catch (err) {
        console.error("❌ Run failed:", err.message);
      }
      console.log(`⏳ Next check in ${intervalMin} minute(s)...`);
      await new Promise((r) => setTimeout(r, intervalMin * 60 * 1000));
    }
  } else {
    await runMonitor(runOpts);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
  });
}

module.exports = {
  runMonitor,
  mergeShardFiles,
  sendTelegram,
  resolveAlertChatIds,
  loadData,
  loadSkus,
  compactProductRecord,
  evaluateRepeatAlertThrottle,
  normalizeProductTitle,
  CONFIG,
};
