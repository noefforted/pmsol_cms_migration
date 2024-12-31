def priority(order):
    def decorator(func):
        func._priority = order
        return func
    return decorator
