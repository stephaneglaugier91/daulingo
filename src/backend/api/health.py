from fastapi import APIRouter

router = APIRouter()
prefix = ""
path = "/health"


@router.get(path, summary="Service health check")
async def health():
    """
    Check if the metadata service is up and running.
    """
    return {"message": "RUNNING"}
