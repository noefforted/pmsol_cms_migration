from prisma import Prisma

prisma = Prisma()

class LoadRepository:
    @staticmethod
    def RegisterCrew_create(data):
        return prisma.crewing_registercrew.create(data=data)
