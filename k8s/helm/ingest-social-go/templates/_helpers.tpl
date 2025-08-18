{{- define "ingest-social-go.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "ingest-social-go.fullname" -}}
{{ include "ingest-social-go.name" . }}-{{ .Release.Name }}
{{- end }}
