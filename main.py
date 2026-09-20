import os
import asyncio

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

PIKA_BASE = "https://api.dev.pika.art"
PIKA_API_KEY = os.environ.get("PIKA_API_KEY")


class GenerateRequest(BaseModel):
    openaiFileIdRefs: list
    prompt: str


@app.get("/")
async def health():
    return {
        "status": "ok",
        "service": "kling-image-animator"
    }


async def get_openai_file(file_ref):
    if isinstance(file_ref, str):
        if file_ref.startswith("http://") or file_ref.startswith("https://"):
            url = file_ref
        else:
            raise HTTPException(
                status_code=400,
                detail="Unsupported OpenAI file reference format."
            )
    elif isinstance(file_ref, dict):
        url = (
    file_ref.get("download_link")
    or file_ref.get("download_url")
    or file_ref.get("url")
    or file_ref.get("file_url")
)

        if not url:
            raise HTTPException(
                status_code=400,
                detail="Could not find a downloadable URL in the file reference."
            )
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid file reference."
        )

    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.get(url)

    if response.status_code != 200:
        raise HTTPException(
            status_code=400,
            detail=f"Could not retrieve uploaded image: HTTP {response.status_code}"
        )

    content_type = response.headers.get(
        "content-type",
        "image/png"
    ).split(";")[0]

    return response.content, content_type


async def upload_to_pika(image_bytes, content_type):
    if not PIKA_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="PIKA_API_KEY is not configured."
        )

    headers = {
        "X-API-Key": PIKA_API_KEY,
        "Content-Type": "application/json",
    }

    upload_request = {
        "content_type": content_type,
        "size_bytes": len(image_bytes),
    }

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{PIKA_BASE}/v1/media/uploads",
            headers=headers,
            json=upload_request,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Pika upload initialization failed: {response.text}"
            )

        upload_info = response.json()

        upload_url = upload_info.get("upload_url")
        permanent_url = upload_info.get("url")

        if not upload_url or not permanent_url:
            raise HTTPException(
                status_code=502,
                detail="Pika did not return the expected upload URLs."
            )

        put_response = await client.put(
            upload_url,
            content=image_bytes,
            headers={"Content-Type": content_type},
        )

        if put_response.status_code not in (200, 201):
            raise HTTPException(
                status_code=502,
                detail=f"Pika image upload failed: HTTP {put_response.status_code}"
            )

    return permanent_url


async def start_kling(image_url, prompt):
    headers = {
        "X-API-Key": PIKA_API_KEY,
        "Content-Type": "application/json",
    }

    body = {
        "image_url": image_url,
        "prompt": prompt,
        "duration": 5,
        "resolution": "720p",
        "audio": "off",
    }

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{PIKA_BASE}/v1/media/kling/kling-3.0/image-to-video",
            headers=headers,
            json=body,
        )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Kling generation failed to start: {response.text}"
        )

    result = response.json()
    request_id = result.get("id")

    if not request_id:
        raise HTTPException(
            status_code=502,
            detail="Pika did not return a job ID."
        )

    return request_id


async def wait_for_job(request_id):
    headers = {
        "X-API-Key": PIKA_API_KEY,
    }

    max_attempts = 120

    async with httpx.AsyncClient(timeout=60) as client:
        for _ in range(max_attempts):
            response = await client.get(
                f"{PIKA_BASE}/v1/media/jobs/{request_id}",
                headers=headers,
            )

            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Could not check Pika job: {response.text}"
                )

            job = response.json()
            status = job.get("status")

            if status == "completed":
                return

            if status == "failed":
                error = job.get("error") or "Unknown Pika generation error."

                raise HTTPException(
                    status_code=502,
                    detail=f"Pika generation failed: {error}"
                )

            await asyncio.sleep(3)

    raise HTTPException(
        status_code=504,
        detail="Pika generation timed out."
    )


async def get_video_url(request_id):
    headers = {
        "X-API-Key": PIKA_API_KEY,
    }

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            f"{PIKA_BASE}/v1/media/jobs/{request_id}/content",
            headers=headers,
        )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Could not retrieve generated video: {response.text}"
        )

    result = response.json()
    video_url = result.get("url")

    if not video_url:
        raise HTTPException(
            status_code=502,
            detail="Pika did not return a video URL."
        )

    return video_url


@app.post("/generate")
async def generate(request: GenerateRequest):

    if len(request.openaiFileIdRefs) != 1:
        raise HTTPException(
            status_code=400,
            detail="Exactly one source image is required."
        )

    if not request.prompt.strip():
        raise HTTPException(
            status_code=400,
            detail="A motion prompt is required."
        )

    if not PIKA_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="PIKA_API_KEY is not configured on the server."
        )

    image_bytes, content_type = await get_openai_file(
        request.openaiFileIdRefs[0]
    )

    pika_image_url = await upload_to_pika(
        image_bytes,
        content_type
    )

    request_id = await start_kling(
        pika_image_url,
        request.prompt.strip()
    )

    await wait_for_job(request_id)

    video_url = await get_video_url(request_id)

    return {
        "status": "completed",
        "video_url": video_url,
    }
