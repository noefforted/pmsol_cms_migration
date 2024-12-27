import pandas as pd
from datetime import datetime
from prisma import Prisma
import asyncio
import uuid

async def migration():
    prisma = Prisma()
    await prisma.connect()
    df = pd.read_csv('data_source/CorePTKDb.City.csv')
    # print(dt_array[:,2])
    final_data = [
        {
            "Id": str(uuid.uuid4()),
            "CityName": row["Name"],
            "Created": datetime.now(),
            "CreatedBy": "Migration",
            "IsDeleted": False,
            "Modified": datetime.now(),
            "ModifiedBy": None
        }
        for _, row in df.iterrows()
    ]
    await prisma.crewing_city.delete_many()
    await prisma.crewing_city.create_many(data=final_data)

asyncio.run(migration())