{{- define "ingest-esg-go.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "ingest-esg-go.fullname" -}}
{{ include "ingest-esg-go.name" . }}-{{ .Release.Name }}
{{- end }}

{{- define "ingest-esg-go.labels" -}}
app.kubernetes.io/name: {{ include "ingest-esg-go.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "ingest-esg-go.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ingest-esg-go.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
