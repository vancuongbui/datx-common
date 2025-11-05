import multiprocessing
import functools
import time


def run_in_parallel(funcs, args_list, concurrent):
    results = []
    with multiprocessing.Pool(concurrent) as pool:
        for func in funcs:
            result = list(pool.starmap(func, args_list))
            results.append(result)
    return results


def delayed(delay_time):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            time.sleep(delay_time)
            return result

        return wrapper

    return decorator
