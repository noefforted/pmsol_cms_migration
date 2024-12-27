import asyncio
import logging
from service.migration import Migration
from util.log import log_begin

log_begin()
log_main = logging.getLogger('Log Main')


async def main():
    log_main.info("Memulai Migrasi....")
    await Migration.RegisterCrew()
    log_main.info("Migrasi Selesai")

asyncio.run(main())
