"use strict";

const https = require("https");
const { SecretsManagerClient, GetSecretValueCommand } = require("@aws-sdk/client-secrets-manager");

// Optional OpenTelemetry API (only used if available in runtime)
let otel = null;
try {
  otel = require("@opentelemetry/api");
} catch {
  // ok: ADOT auto-instrumentation can still produce traces without this
}

const sm = new SecretsManagerClient({});
const httpsAgent = new https.Agent({ keepAlive: true });

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
        agent: httpsAgent,
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
  // Optional convention: "prod.finguard-broker.critical.api_availability.5xx_rate_high"
  const p = (alarmName || "").split(".");
  return {
    env: p[0] || envDefault || "prod",
    pd_service: p[1] || "finguard-broker",
    severity: p[2] || "error",
    class: p[3] || "alarm",
    scope: "global",
  };
}

function withSpan(spanName, fn) {
  if (!otel) return fn(null);

  const tracer = otel.trace.getTracer("finguard-pd-relay-prod");
  return tracer.startActiveSpan(spanName, async (span) => {
    try {
      return await fn(span);
    } catch (err) {
      try {
        span.recordException(err);
        span.setStatus({ code: otel.SpanStatusCode.ERROR, message: String(err?.message || err) });
      } catch { }
      throw err;
    } finally {
      try {
        span.end();
      } catch { }
    }
  });
}

exports.handler = async (event, context) => {
  return withSpan("pd_relay.handle_event", async (span) => {
    const xrayTraceId = process.env._X_AMZN_TRACE_ID || "";
    const awsRequestId = context?.awsRequestId || "";
    const region = event?.region || process.env.AWS_REGION || "unknown";

    const detail = event?.detail || {};
    const alarmName = detail.alarmName || "unknown_alarm";
    const state = detail?.state?.value || "UNKNOWN"; // ALARM | OK | INSUFFICIENT_DATA
    const reason = detail?.state?.reason || "";

    // Recommended: put JSON in AlarmDescription for exact routing/classification
    const meta =
      safeJsonParse(detail.alarmDescription) ||
      classifyFromAlarmName(alarmName, process.env.DEFAULT_ENV);

    const env = meta.env || process.env.DEFAULT_ENV || "prod";
    const pdService = meta.pd_service || "finguard-broker";
    const severity = normalizeSeverity(meta.severity);
    const cls = meta.class || "alarm";
    const scope = meta.scope || "global";
    const runbook = meta.runbook || "";
    const pdNotify = meta.pd_notify !== false; // default true unless explicitly false

    const dedupKey =
      meta.dedup_key ||
      `${env}:${pdService}:${cls}:${scope}:${alarmName}`;

    // Basic span attributes (only if manual OTel API available)
    if (span) {
      span.setAttribute("finguard.env", env);
      span.setAttribute("finguard.pd_service", pdService);
      span.setAttribute("finguard.class", cls);
      span.setAttribute("finguard.scope", scope);
      span.setAttribute("finguard.alarm_name", alarmName);
      span.setAttribute("finguard.state", state);
      span.setAttribute("finguard.pd_notify", pdNotify);
      span.setAttribute("aws.region", region);
    }

    console.log("pd-relay: received", {
      alarmName,
      state,
      env,
      pdService,
      cls,
      scope,
      pdNotify,
      awsRequestId,
      xrayTraceId,
    });

    if (state === "INSUFFICIENT_DATA") {
      console.log("Ignoring INSUFFICIENT_DATA", { alarmName });
      return { ignored: true };
    }

    const eventAction = state === "OK" ? "resolve" : "trigger";

    // Allow safe testing without paging
    if (!pdNotify) {
      console.log("pd_notify is false; skipping PagerDuty enqueue", { alarmName, eventAction, dedupKey });
      return { skipped: true, reason: "pd_notify_false", alarmName, eventAction, dedupKey };
    }

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
          aws_request_id: awsRequestId,
          xray_trace_id: xrayTraceId,
        },
      },
      links: [
        runbook ? { href: runbook, text: "Runbook" } : null,
        {
          href: `https://${region}.console.aws.amazon.com/cloudwatch/home?region=${region}#alarmsV2:alarm/${encodeURIComponent(alarmName)}`,
          text: "CloudWatch Alarm",
        },
      ].filter(Boolean),
    };

    const resp = await postPD(payload);
    console.log("PagerDuty response", { statusCode: resp.statusCode, body: resp.body });

    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      throw new Error(`PagerDuty enqueue failed: ${resp.statusCode} ${resp.body}`);
    }

    return { ok: true, eventAction, dedupKey };
  });
};
