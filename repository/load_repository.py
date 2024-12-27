from prisma import Prisma

prisma = Prisma()

class LoadRepository:
    @staticmethod
    async def RegisterCrew_create(data):
        return await prisma.crewing_registercrew.create(data=data)
