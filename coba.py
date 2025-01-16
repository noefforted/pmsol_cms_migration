from prisma import Prisma
import pandas as pd
def get_crewing_employeeSeaService():
    prisma = Prisma()
    prisma.connect()
    data = prisma.crewing_employeeseaservice.find_many()
    df = pd.DataFrame([dict(item) for item in data])
    df = 
    prisma.disconnect()
    print(data)

get_crewing_employeeSeaService()