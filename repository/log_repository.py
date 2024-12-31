from prisma import Prisma

class LogRepository:
    @staticmethod
    def get_last_regCrewNo():
        prisma = Prisma()
        prisma.connect()
        data = prisma.crewing_registercrew.find_first(order={"RegCrewNo": "desc"})
        last_regCrewNo = data.RegCrewNo if data else 0
        prisma.disconnect()
        return last_regCrewNo
    