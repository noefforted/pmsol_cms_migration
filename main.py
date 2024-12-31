import logging
from service.core_migration import Migrate, Create
from util.log import log_begin
from util.decorator import priority  # Import the priority decorator

# Initialize logging
log_begin()
log_main = logging.getLogger('Log Main')

# Create instances
create = Create()
migrate = Migrate()

def run_all(instance):
    methods = [
        getattr(instance, method_name)
        for method_name in dir(instance)
        if callable(getattr(instance, method_name)) and hasattr(getattr(instance, method_name), '_priority')
    ]
    for method in sorted(methods, key=lambda x: x._priority):
        log_main.info(f"Executing Task: {method.__name__}")
        try:
            method()
        except Exception as e:
            log_main.error(f"Error while executing task {method.__name__}: {e}", exc_info=True)

def main():
    log_main.info("========================== Starting Migration... ==========================")
    run_all(create) 
    run_all(migrate) 
    log_main.info("========================== Migration Completed ==========================")

if __name__ == "__main__":
    main()
