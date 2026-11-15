from prometheus_client import Counter, Gauge, Histogram


class Metrics:
    def __init__(self) -> None:
        self.requests = Counter("query_http_requests_total", "Query HTTP requests", ("method", "route", "status"))
        self.latency = Histogram("query_http_latency_seconds", "Query HTTP latency", ("method", "route"))
        self.events = Counter("query_events_total", "Query event processing outcomes", ("result",))
        self.pending = Gauge("query_pending_failures", "Pending failure classifications in the projection")


metrics = Metrics()
