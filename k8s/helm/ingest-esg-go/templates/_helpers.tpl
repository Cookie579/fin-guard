{{- define "ingest-esg-go.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "ingest-esg-go.fullname" -}}
{{ include "ingest-esg-go.name" . }}-{{ .Release.Name }}
{{- end }}
