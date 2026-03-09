"""
Utility Decorators - Retry, timing, error handling
"""

import time
import functools
from typing import Callable, Any, Type, Tuple
import logging

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Multiplier for delay after each attempt
        exceptions: Tuple of exceptions to catch and retry
        
    Usage:
        @retry(max_attempts=3, delay=1.0, backoff=2.0)
        def my_function():
            # function that might fail
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                        f"Retrying in {current_delay:.1f}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


def timeit(func: Callable) -> Callable:
    """
    Decorator to measure function execution time.
    
    Usage:
        @timeit
        def my_slow_function():
            time.sleep(2)
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        elapsed = end_time - start_time
        logger.info(f"{func.__name__} executed in {elapsed:.2f}s")
        
        return result
    
    return wrapper


def log_errors(func: Callable) -> Callable:
    """
    Decorator to log exceptions before re-raising.
    
    Usage:
        @log_errors
        def risky_function():
            raise ValueError("Something went wrong")
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"Error in {func.__name__}: {type(e).__name__}: {e}",
                exc_info=True
            )
            raise
    
    return wrapper