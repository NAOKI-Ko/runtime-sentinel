"""Domain exception hierarchy."""


class SentinelError(Exception):
    """Base error raised by the runtime-sentinel domain."""


class ConfigurationError(SentinelError):
    """A supplied configuration cannot be executed safely."""


class DependencyCycleError(ConfigurationError):
    """The worker dependency graph contains a cycle."""


class UnknownDependencyError(ConfigurationError):
    """A worker references a dependency that is not registered."""


class InvalidTransitionError(SentinelError):
    """A worker state transition violates the lifecycle state machine."""


class WorkerFailedError(SentinelError):
    """A worker exhausted its retry policy."""


class CircuitOpenError(SentinelError):
    """A circuit breaker rejected an invocation."""


class PoolClosedError(SentinelError):
    """A resource was requested from a closed pool."""
