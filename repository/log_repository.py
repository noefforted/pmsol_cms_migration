from prisma import Prisma

class LogRepository:
    @staticmethod
    async def get_last_regCrewNo():
        prisma = Prisma()
        await prisma.connect()
        data = await prisma.crewing_registercrew.find_first(order={"RegCrewNo": "desc"})
        last_regCrewNo = data.RegCrewNo if data else 0
        await prisma.disconnect()
        return last_regCrewNo
    