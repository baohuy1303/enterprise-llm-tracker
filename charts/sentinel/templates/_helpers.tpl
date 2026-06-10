{{/*
Chart name (overridable). Used as app.kubernetes.io/name.
*/}}
{{- define "sentinel.name" -}}
{{- default "sentinel" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release-scoped fullname — the prefix for every resource and every in-cluster
DNS name. Defaults to the release name so a `helm install sentinel ...` yields
services named sentinel-postgres, sentinel-redis, sentinel-kafka, etc. The
rendered sentinel.yaml and the Kafka KAFKA_ADVERTISED_LISTENERS both derive
their addresses from this same helper, so they can never drift apart.
*/}}
{{- define "sentinel.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Common metadata labels applied to every object.
*/}}
{{- define "sentinel.labels" -}}
helm.sh/chart: {{ printf "%s-%s" (include "sentinel.name" .) .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "sentinel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
initContainer that blocks pod startup until Postgres, Redis, and Kafka are all
reachable. Shared by sentinel-api and sentinel-workers. This is what makes the
Kafka EnsureTopic call on sentinel-api startup reliable — without it, the API
could run EnsureTopic before the broker is listening, the warning path would
fire, and (with auto-topic-creation disabled) the 12-partition topic would
never be created. The trailing sleep gives the KRaft broker a moment to finish
coming up after the port opens.
*/}}
{{- define "sentinel.waitForDeps" -}}
- name: wait-for-deps
  image: {{ .Values.busybox.image }}
  command: ["sh", "-c"]
  args:
    - |
      until nc -z {{ include "sentinel.fullname" . }}-postgres {{ .Values.postgres.port }}; do echo "waiting for postgres..."; sleep 2; done
      until nc -z {{ include "sentinel.fullname" . }}-redis {{ .Values.redis.port }}; do echo "waiting for redis..."; sleep 2; done
      until nc -z {{ include "sentinel.fullname" . }}-kafka {{ .Values.kafka.port }}; do echo "waiting for kafka..."; sleep 2; done
      echo "all dependencies reachable"; sleep 3
{{- end -}}
