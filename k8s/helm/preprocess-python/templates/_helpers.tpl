{{- define "preprocess-python.name" -}}
{{ .Chart.Name }}
{{- end }}

{{- define "preprocess-python.fullname" -}}
{{ include "preprocess-python.name" . }}-{{ .Release.Name }}
{{- end }}
