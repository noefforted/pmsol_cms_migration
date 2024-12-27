from prisma import Prisma


class LoadRepository:
    @staticmethod
    async def RegisterCrew_create(data):
        prisma = Prisma()
        return await prisma.crewing_registercrew.create(data=data)
