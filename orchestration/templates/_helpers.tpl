{{/* vim: set filetype=mustache: */}}

{{/*
  Generate a reference to an Argo input parameter.

  Helm and Argo use the same syntax for variable substitution,
  so we need to add this layer of abstraction to prevent Helm
  from trying to render out vars that should pass through to Argo.
*/}}
{{- define "argo.input" -}}
{{ printf "{{inputs.parameters.%s}}" . }}
{{- end -}}

{{/*
  Generate a reference to an Argo task output.

  Helm and Argo use the same syntax for variable substitution,
  so we need to add this layer of abstraction to prevent Helm
  from trying to render out vars that should pass through to Argo.
*/}}
{{- define "argo.task-output" -}}
{{ printf "{{tasks.%s.outputs.parameters.%s}}" (get . "task-name") (get . "output-name") }}
{{- end -}}

{{/*
  Generate a reference to workflow creation-time using a specific format-string.

  Helm and Argo use the same syntax for variable substitution,
  so we need to add this layer of abstraction to prevent Helm
  from trying to render out vars that should pass through to Argo.
*/}}
{{- define "argo.workflow-time" -}}
{{ printf "{{workflow.creationTimestamp.%s}}" . }}
{{- end -}}
