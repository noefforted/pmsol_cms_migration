from prisma import Prisma

def coba():
    prisma = Prisma()
    prisma.connect()

    data = prisma.crewing_employeecocdoc.find_many()
    for item in data:
        print(item)


coba()