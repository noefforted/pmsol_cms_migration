from prisma import Prisma
import asyncio

async def coba():
    prisma = Prisma()
    await prisma.connect()

    city = await prisma.crewing_city.find_unique(where={"Id": "56EA33F6-4861-47FE-BBFF-001E06A2BBB5"})
    if city:
        print(f"City found: {city}")
    else:
        print("City not found in Prisma connection.")


asyncio.run(coba())