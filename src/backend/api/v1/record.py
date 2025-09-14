from fastapi import APIRouter, Depends
from pydantic import BaseModel

from backend.dependencies import get_activity_ingest_service
from backend.domain.models import Activity
from backend.services.ingest_activity import ActivityIngestService

router = APIRouter(prefix="/v1")


class RecordRequest(BaseModel):
    events: list[Activity]


@router.post("/record")
def record(
    req: RecordRequest,
    service: ActivityIngestService = Depends(get_activity_ingest_service),
) -> dict:
    inserted, ins_users, upd_users = service.ingest(req.events)
    return {
        "status": "ok",
        "details": {
            "inserted_events": inserted,
            "new_users": ins_users,
            "updated_users": upd_users,
        },
    }
