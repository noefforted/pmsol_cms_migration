import asyncio
import logging
from service.core_migration import Migrate, Create
from util.log import log_begin

# Initialize logging
log_begin()
log_main = logging.getLogger('Log Main')

# Create instances
create = Create()
migrate = Migrate()

async def run_all(instance):
    tasks = []
    for method_name in dir(instance):
        method = getattr(instance, method_name)
        if callable(method) and not method_name.startswith("__") and asyncio.iscoroutinefunction(method):
            log_main.info(f"Scheduling Task: {method_name}")
            tasks.append(method())  # Collect coroutine tasks

    try:
        await asyncio.gather(*tasks)  # Execute tasks concurrently
    except Exception as e:
        log_main.error(f"Error while running tasks: {e}", exc_info=True)

async def main():
    log_main.info("============= Starting Migration... =============")
    await run_all(create)
    await run_all(migrate)
    log_main.info("============= Migration Completed =============")

# Run the async main function
asyncio.run(main())
