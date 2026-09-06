{{/* Reserve room for resource suffixes in Kubernetes names. */}}
{{- define "pvc-migrate.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "pvc-migrate.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Preserve the manifest selector for the default pvc-migrate release. */}}
{{- define "pvc-migrate.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pvc-migrate.fullname" . }}
app.kubernetes.io/component: controller
{{- end -}}

{{- define "pvc-migrate.labels" -}}
helm.sh/chart: {{ include "pvc-migrate.chart" . }}
{{ include "pvc-migrate.selectorLabels" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "pvc-migrate.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pvc-migrate.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "pvc-migrate.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "pvc-migrate.toolImage" -}}
{{- $repository := default .Values.image.repository .Values.toolImage.repository -}}
{{- $tag := default (default .Chart.AppVersion .Values.image.tag) .Values.toolImage.tag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
