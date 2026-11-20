from functools import wraps
import time


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
):
    """
    Retry the decorated function on the given exceptions, waiting
    base_delay * backoff_factor ** attempt seconds between attempts.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if attempt >= max_retries:
                        raise
                    time.sleep(base_delay * (backoff_factor ** attempt))
                    attempt += 1
        return wrapper
    return decorator
