import uuid
from datetime import datetime
from repository.extract_repository import ExtractRepository

async def crewing_city():
    df = await ExtractRepository.get_City()
    data = [
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
    return data