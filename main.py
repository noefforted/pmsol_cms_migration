import asyncio
import logging
from service.core_migration import Migrate, Create
from util.log import log_begin

log_begin()
log_main = logging.getLogger('Log Main')

async def main():
    log_main.info("Memulai Migrasi....")
    await Create().crewing_bank()
    await Create().crewing_interviewAssessor()
    await Create().crewing_maritalStatus()
    await Create().crewing_religion()
    await Migrate().crewing_city()
    log_main.info("Migrasi Selesai")

asyncio.run(main())
