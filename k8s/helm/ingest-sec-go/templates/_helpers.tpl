{{- /*
  Helpers for the ingest‑sec‑go chart

  These helpers mirror those defined in the ingest‑social‑go chart so that
  other templates (e.g. rbac.yaml) can reference a fully qualified name
  and other metadata consistently. Without these helpers, calls to
  `include "ingest-sec-go.fullname"` would fail because the functions
  would not be defined. Adding this file restores compatibility with
  Helm’s include mechanism and ensures the chart renders correctly.
*/ -}}

{{- define "ingest-sec-go.name" -}}
{{ .Chart.Name }}
{{- end -}}

{{- define "ingest-sec-go.fullname" -}}
{{ include "ingest-sec-go.name" . }}-{{ .Release.Name }}
{{- end -}}

{{- /* Convenience helper to return common labels */ -}}
{{- define "ingest-sec-go.labels" -}}
app.kubernetes.io/name: {{ include "ingest-sec-go.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- /* Helper to return the service account name */ -}}
{{- define "ingest-sec-go.serviceAccountName" -}}
{{ default .Values.serviceAccount.name (printf "%s-sa" (include "ingest-sec-go.name" .)) }}
{{- end -}}