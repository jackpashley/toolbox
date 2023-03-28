import traceback


def try_except(func, error_message=None):
    """
    Usage:
    @try_except
    def main():
        ...
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            if error_message:
                print(error_message)
                traceback.print_exc()
            else:
                traceback.print_exc()

    return wrapper
