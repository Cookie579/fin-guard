const https = require("https");
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require("@aws-sdk/client-secrets-manager");

const sm = new SecretsManagerClient({});

// Defaults: only process alarms starting with these prefixes (unless AlarmDescription JSON overrides)
const ALLOWED_ALARM_PREFIXES = (process.env.ALLOWED_ALARM_PREFIXES ||
  "prod.finguard-,stage.finguard-")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// Default allowlist of PD services (prevents weird routing / missing-secret errors)
const ALLOWED_PD_SERVICES = (process.env.ALLOWED_PD_SERVICES ||
  "finguard-broker,finguard-ingestion,finguard-processing,finguard-streams,finguard-dataplane")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

function safeJsonParse(s) {
  if (!s || typeof s !== "string") return null;
  const t = s.trim();
  if (!(t.startsWith("{") && t.endsWith("}"))) return null;
  try {
    return JSON.parse(t);
  } catch {
    return null;
  }
}

function normalizeSeverity(s) {
  const v = (s || "").toLowerCase();
  return ["critical", "error", "warning", "info"].includes(v) ? v : "error";
}

function postPD(body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request(
      {
        hostname: "events.pagerduty.com",
        path: "/v2/enqueue",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(data),
        },
        timeout: 8000,
      },
      (res) => {
        let out = "";
        res.on("data", (c) => (out += c));
        res.on("end", () => resolve({ statusCode: res.statusCode, body: out }));
      }
    );

    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.write(data);
    req.end();
  });
}

async function getSecretString(secretId) {
  const r = await sm.send(new GetSecretValueCommand({ SecretId: secretId }));
  if (!r.SecretString) throw new Error(`Secret ${secretId} missing SecretString`);
  return r.SecretString.trim();
}

function classifyFromAlarmName(alarmName, envDefault) {
  // Convention (recommended):
  // "prod.finguard-broker.critical.api_availability.5xx_rate_high"
  const p = (alarmName || "").split(".");
  return {
    env: p[0] || envDefault || "prod",
    pd_service: p[1] || "finguard-broker",
    severity: p[2] || "error",
    class: p[3] || "alarm",
    scope: "global",
  };
}

exports.handler = async (event) => {
  const detail = event?.detail || {};
  const alarmName = detail.alarmName || "unknown_alarm";
  const state = detail?.state?.value || "UNKNOWN"; // ALARM | OK | INSUFFICIENT_DATA
  const reason = detail?.state?.reason || "";
  const region = event.region || process.env.AWS_REGION;

  // --- HARDENING #1: Only process FinGuard alarms ---
  const metaFromDesc = safeJsonParse(detail.alarmDescription);
  const isAllowedByPrefix = ALLOWED_ALARM_PREFIXES.some((pfx) =>
    alarmName.startsWith(pfx)
  );

  // If no JSON metadata AND alarm doesn't match prefix, ignore entirely
  if (!metaFromDesc && !isAllowedByPrefix) {
    console.log("Ignoring non-FinGuard alarm (no metadata + not allowed prefix)", {
      alarmName,
      allowedPrefixes: ALLOWED_ALARM_PREFIXES,
    });
    return { ignored: true };
  }

  // Recommended: put JSON in AlarmDescription for exact routing/classification
  const meta =
    metaFromDesc || classifyFromAlarmName(alarmName, process.env.DEFAULT_ENV);

  // --- HARDENING #2: Allow alarms that do NOT page (pd_notify=false) ---
  // This lets you create tons of alarms (Tier 1/2) without paging.
  if (meta && meta.pd_notify === false) {
    console.log("pd_notify=false; not sending to PagerDuty", { alarmName });
    return { ignored: true };
  }

  // Ignore insufficient data (simple default; you can change later)
  if (state === "INSUFFICIENT_DATA") {
    console.log("Ignoring INSUFFICIENT_DATA", { alarmName });
    return { ignored: true };
  }

  // Only act on ALARM and OK (ignore UNKNOWN or other unexpected values)
  if (state !== "ALARM" && state !== "OK") {
    console.log("Ignoring unsupported alarm state", { alarmName, state });
    return { ignored: true };
  }

  const env = meta.env || process.env.DEFAULT_ENV || "prod";
  const pdService = meta.pd_service || "finguard-broker";

  // Guard against typos / unknown services (prevents missing secret lookups)
  if (!ALLOWED_PD_SERVICES.includes(pdService)) {
    console.log("Ignoring alarm with unknown pd_service", {
      alarmName,
      pd_service: pdService,
      allowed: ALLOWED_PD_SERVICES,
    });
    return { ignored: true };
  }

  const severity = normalizeSeverity(meta.severity);
  const cls = meta.class || "alarm";
  const scope = meta.scope || "global";
  const runbook = meta.runbook || "";

  // Deterministic dedup key (trigger/resolve MUST match)
  const dedupKey =
    meta.dedup_key || `${env}:${pdService}:${cls}:${scope}:${alarmName}`;

  const eventAction = state === "OK" ? "resolve" : "trigger";

  const secretPrefix = process.env.PD_SECRET_PREFIX || "pd";
  const routingKeySecretId = `${secretPrefix}/${env}/${pdService}`;
  const routingKey = await getSecretString(routingKeySecretId);

  const payload = {
    routing_key: routingKey,
    event_action: eventAction,
    dedup_key: dedupKey,
    payload: {
      summary: `[${env}] ${pdService} ${state}: ${alarmName}`,
      source: "aws.cloudwatch",
      severity: eventAction === "resolve" ? "info" : severity,
      timestamp: new Date().toISOString(),
      custom_details: {
        env,
        pd_service: pdService,
        class: cls,
        scope,
        alarmName,
        state,
        reason,
        region,
        runbook,
      },
    },
    links: [
      runbook ? { href: runbook, text: "Runbook" } : null,
      {
        href: `https://${region}.console.aws.amazon.com/cloudwatch/home?region=${region}#alarmsV2:alarm/${encodeURIComponent(
          alarmName
        )}`,
        text: "CloudWatch Alarm",
      },
    ].filter(Boolean),
  };

  const resp = await postPD(payload);
  console.log("PagerDuty response", resp.statusCode, resp.body);

  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    throw new Error(`PagerDuty enqueue failed: ${resp.statusCode} ${resp.body}`);
  }

  return { ok: true };
};
