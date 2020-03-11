{{- $YYYY := "{{workflow.creationTimestamp.Y}}" }}
{{- $mm := "{{workflow.creationTimestamp.m}}" }}
{{- $dd := "{{workflow.creationTimestamp.d}}" }}
{{- $HH := "{{workflow.creationTimestamp.H}}" }}
{{- $MM := "{{workflow.creationTimestamp.M}}" }}
{{- $SS := "{{workflow.creationTimestamp.S}}" }}

{{/* TODO */}}
{{- define "argo.timestamp" -}}
{{ printf "%s%s%sT%s%s%s" $YYYY $mm $dd $HH $MM $SS }}
{{- end -}}
