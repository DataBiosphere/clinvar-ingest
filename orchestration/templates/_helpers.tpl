{{/*
   * Inject references to all the Argo variables needed to
   * render a full timestamp, in the required order.
   *
   * NOTE: This timestamp does not include any special
   * characters, to ensure it is compatible with external
   * storage systems.
   */}}
{{- define "argo.timestamp" -}}
{{- $YYYY := "{{workflow.creationTimestamp.Y}}" -}}
{{- $mm := "{{workflow.creationTimestamp.m}}" -}}
{{- $dd := "{{workflow.creationTimestamp.d}}" -}}
{{- $HH := "{{workflow.creationTimestamp.H}}" -}}
{{- $MM := "{{workflow.creationTimestamp.M}}" -}}
{{- $SS := "{{workflow.creationTimestamp.S}}" -}}
{{ printf "%s%s%sT%s%s%s" $YYYY $mm $dd $HH $MM $SS }}
{{- end -}}
