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
    
    @staticmethod
    def get_crewing_registerCrew():
        prisma = Prisma()
        prisma.connect()
        data = prisma.crewing_registercrew.find_many()
        prisma.disconnect()
        return data
    
    @staticmethod
    def get_crewing_jobPositionMarine():
        prisma = Prisma()
        prisma.connect()
        data = prisma.crewing_jobpositionmarine.find_many()
        prisma.disconnect()
        return data
    
    @staticmethod
    def get_crewing_employee():
        prisma = Prisma()
        prisma.connect()
        data = prisma.crewing_employee.find_many()
        prisma.disconnect()
        return data
    
    @staticmethod
    def get_crewing_employeeCOCDoc():
        prisma = Prisma()
        prisma.connect()
        data = prisma.crewing_employeecocdoc.find_many()
        prisma.disconnect()
        return data
    