class LoggingOTLPSpanExporter(OTLPSpanExporter):
    def export(self, spans):
        for span in spans:
            parent = span.parent
            print(
                "LF_SPAN",
                format(span.context.trace_id, "032x"),
                span.name,
                "remote_parent",
                bool(parent and parent.is_remote),
                "has_parent",
                parent is not None,
            )
        result = super().export(spans)
        print("LF_RESULT", result)
        return result

    def _export(self, serialized_data, timeout_sec=None):
        response = super()._export(serialized_data, timeout_sec)
        print(
            "LF_HTTP",
            response.status_code,
            response.headers.get("content-type"),
            response.content[:300],
        )
        return response
