{{- define "flywheel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flywheel.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "flywheel.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "flywheel.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "flywheel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "flywheel.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "flywheel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flywheel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "flywheel.labels" -}}
{{- $labels := dict
  "helm.sh/chart" (include "flywheel.chart" .)
  "app.kubernetes.io/name" (include "flywheel.name" .)
  "app.kubernetes.io/instance" .Release.Name
  "app.kubernetes.io/version" .Chart.AppVersion
  "app.kubernetes.io/managed-by" .Release.Service
  "app.kubernetes.io/part-of" "flywheel"
-}}
{{- toYaml (merge $labels .Values.commonLabels) -}}
{{- end -}}

{{- define "flywheel.shardSelector" -}}
{{ include "flywheel.selectorLabels" . }}
app.kubernetes.io/component: shard
{{- end -}}

{{- define "flywheel.agentSelector" -}}
{{ include "flywheel.selectorLabels" . }}
app.kubernetes.io/component: agent
{{- end -}}

{{- define "flywheel.shardLabels" -}}
{{- $labels := include "flywheel.labels" . | fromYaml -}}
{{- $_ := set $labels "app.kubernetes.io/component" "shard" -}}
{{- toYaml $labels -}}
{{- end -}}

{{- define "flywheel.agentLabels" -}}
{{- $labels := include "flywheel.labels" . | fromYaml -}}
{{- $_ := set $labels "app.kubernetes.io/component" "agent" -}}
{{- toYaml $labels -}}
{{- end -}}

{{- define "flywheel.shardPodLabels" -}}
{{- $labels := include "flywheel.shardLabels" . | fromYaml -}}
{{- toYaml (merge $labels .Values.shards.podLabels) -}}
{{- end -}}

{{- define "flywheel.agentPodLabels" -}}
{{- $labels := include "flywheel.agentLabels" . | fromYaml -}}
{{- toYaml (merge $labels .Values.agent.podLabels) -}}
{{- end -}}

{{/*
Suffixed names truncate the base first so the distinguishing suffix survives
the 63-character DNS-label limit.
*/}}
{{- define "flywheel.shardsServiceName" -}}
{{- printf "%s-shards" (include "flywheel.fullname" . | trunc 56 | trimSuffix "-") -}}
{{- end -}}

{{- define "flywheel.agentName" -}}
{{- printf "%s-agent" (include "flywheel.fullname" . | trunc 57 | trimSuffix "-") -}}
{{- end -}}

{{- define "flywheel.shardPdbName" -}}
{{- printf "%s-shard-pdb" (include "flywheel.fullname" . | trunc 53 | trimSuffix "-") -}}
{{- end -}}

{{- define "flywheel.agentPdbName" -}}
{{- printf "%s-agent-pdb" (include "flywheel.fullname" . | trunc 53 | trimSuffix "-") -}}
{{- end -}}

{{- define "flywheel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "flywheel.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "flywheel.shardsServiceHost" -}}
{{- printf "%s.%s.svc.%s" (include "flywheel.shardsServiceName" .) .Release.Namespace (.Values.clusterDomain | trimSuffix ".") -}}
{{- end -}}

{{/* Named `flywheel` TCP port on the headless shard Service. */}}
{{- define "flywheel.srvName" -}}
{{- printf "_flywheel._tcp.%s" (include "flywheel.shardsServiceHost" .) -}}
{{- end -}}

{{- define "flywheel.validateValues" -}}
{{- if and .Values.service.enabled (not .Values.agent.enabled) -}}
{{- fail "service.enabled requires agent.enabled; disable the shared Service for sidecar-only installations" -}}
{{- end -}}
{{- if and .Values.ingress.enabled (not .Values.service.enabled) -}}
{{- fail "ingress.enabled requires service.enabled" -}}
{{- end -}}
{{- if and .Values.agent.autoscaling.enabled (not .Values.agent.enabled) -}}
{{- fail "agent.autoscaling.enabled requires agent.enabled" -}}
{{- end -}}
{{- if and .Values.agent.autoscaling.enabled (gt (int .Values.agent.autoscaling.minReplicas) (int .Values.agent.autoscaling.maxReplicas)) -}}
{{- fail "agent.autoscaling.maxReplicas must be at least minReplicas" -}}
{{- end -}}
{{- if and .Values.agent.autoscaling.enabled (eq .Values.agent.autoscaling.targetCPUUtilizationPercentage nil) (eq .Values.agent.autoscaling.targetMemoryUtilizationPercentage nil) -}}
{{- fail "agent autoscaling requires a CPU or memory utilization target" -}}
{{- end -}}
{{- if lt (int64 .Values.shards.config.highWatermarkBytes) (int64 .Values.shards.config.lowWatermarkBytes) -}}
{{- fail "shards.config.highWatermarkBytes must be at least lowWatermarkBytes" -}}
{{- end -}}
{{- if and (ne .Values.shards.podDisruptionBudget.minAvailable nil) (ne .Values.shards.podDisruptionBudget.maxUnavailable nil) -}}
{{- fail "set only one of shards.podDisruptionBudget.minAvailable or maxUnavailable" -}}
{{- end -}}
{{- if and .Values.shards.podDisruptionBudget.enabled (eq .Values.shards.podDisruptionBudget.minAvailable nil) (eq .Values.shards.podDisruptionBudget.maxUnavailable nil) -}}
{{- fail "shards.podDisruptionBudget requires minAvailable or maxUnavailable" -}}
{{- end -}}
{{- if and (ne .Values.agent.podDisruptionBudget.minAvailable nil) (ne .Values.agent.podDisruptionBudget.maxUnavailable nil) -}}
{{- fail "set only one of agent.podDisruptionBudget.minAvailable or maxUnavailable" -}}
{{- end -}}
{{- if and .Values.agent.enabled .Values.agent.podDisruptionBudget.enabled (eq .Values.agent.podDisruptionBudget.minAvailable nil) (eq .Values.agent.podDisruptionBudget.maxUnavailable nil) -}}
{{- fail "agent.podDisruptionBudget requires minAvailable or maxUnavailable" -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled (not .Values.agent.enabled) (empty .Values.networkPolicy.shards.additionalIngress) -}}
{{- fail "sidecar-only networkPolicy requires networkPolicy.shards.additionalIngress sources" -}}
{{- end -}}
{{- end -}}
