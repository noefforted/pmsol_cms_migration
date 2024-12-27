from repository.extract_repository import ExtractRepository
from repository.load_repository import LoadRepository
import logging
import asyncio
from util.log import log_begin
from service.data_algorithm import transform_data
from prisma import Prisma

log_begin()
log_RegisterCrew = logging.getLogger("Log RegisterCrew")


class Migration:
    @staticmethod
    async def RegisterCrew():
        prisma = Prisma()
        await prisma.connect()
        try:
            df = await ExtractRepository.get_RegisterCrew()
            log_RegisterCrew.info("Mengambil data dari RegisterCrew")

            transformed_data = transform_data(df)
            log_RegisterCrew.info(f"Transformasi data selesai. Total baris: {len(transformed_data)}")

            data_records = transformed_data.to_dict(orient='records')
            for record in data_records:
                try:
                    await prisma.crewing_registercrew.create(data=record)
                except Exception as e:
                    log_RegisterCrew.error(f"Error pada data {record}: {e}")

            log_RegisterCrew.info("Migrasi data selesai.")
        finally:
            await prisma.disconnect()
