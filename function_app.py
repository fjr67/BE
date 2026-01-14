import azure.functions as func
import logging
import os
import uuid
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient
from azure.core.exceptions import ResourceExistsError
import json
import requests

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_cosmos_container(container: str):
    client = CosmosClient(
        os.environ["COSMOS_ENDPOINT"],
        os.environ["COSMOS_KEY"]
    )
    db = client.get_database_client(os.environ["COSMOS_DATABASE"])
    return db.get_container_client(os.environ[container])

def analyseImage(image_bytes: bytes, content_type: str):
    foundry_endpoint = os.environ["VISION_ENDPOINT"].rstrip("/")
    vision_key = os.environ["VISION_KEY"]

    url = f"{foundry_endpoint}/vision/v3.2/analyze"
    logging.info(f"Vision url = {url}")

    params = {
        "visualFeatures": "Description,Tags",
        "language": "en"
    }

    headers = {
        "Ocp-Apim-Subscription-Key": vision_key,
        "Content-Type": content_type or "application/octet-stream"
    }

    response = requests.post(url, params=params, headers=headers, data=image_bytes, timeout=20)
    response.raise_for_status()
    return response.json()


def limitTags(tags, max_tags=5):
    if not tags:
        return []
    
    sorted_tags = sorted(tags, key=lambda t: t.get("confidence", 0), reverse=True)
    return sorted_tags[:max_tags]

@app.route(route="uploadMedia", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def uploadMedia(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('uploadMedia called')

    user_id = req.form.get("userId")
    file = req.files.get("file")

    if not user_id or not file:
        return func.HttpResponse("Missing userId or file", status_code=400)
    
    media_id = str(uuid.uuid4())
    file_name = file.filename
    content_type = file.content_type

    blob_name = f"{user_id}/{media_id}-{file_name}"

    blob_service = BlobServiceClient.from_connection_string(os.environ["MEDIA_STORAGE_CONNECTION_STRING"])

    container_client = blob_service.get_container_client(os.environ["BLOB_CONTAINER"])

    data = file.stream.read()
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type=content_type))

    container = get_cosmos_container("COSMOS_MEDIA_CONTAINER")
    doc = {
        "id": media_id,
        "userId": user_id,
        "fileName": file_name,
        "contentType": content_type,
        "sizeBytes": len(data),
        "blobName": blob_name,
        "uploadedAt": datetime.now(timezone.utc).isoformat(),
        "imageAnalysis": { 
            "status": "pending"
        }
    }
    container.upsert_item(doc)

    # temp line - checking ga deployment
    doc['deployment'] = 'deployed using github actions'

    return func.HttpResponse(
        body=json.dumps(doc),
        status_code=201,
        mimetype="application/json"
    )

@app.route(route="createPost", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def createPost(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('createPost called')

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse('Invalid JSON body', status_code=400)
    
    user_id = body.get("userId")
    title = body.get("title")
    caption = body.get("caption", "")
    media = body.get("media", [])

    media_refs = []
    if media:
        media_container = get_cosmos_container("COSMOS_MEDIA_CONTAINER")

        for m in media:
            try:
                media_doc = media_container.read_item(item=m, partition_key=user_id)
            except Exception:
                return func.HttpResponse(f"media not found or not owned by user: {m}", status_code=404)
            
            media_refs.append({
                "mediaId": media_doc["id"],
                "blobName": media_doc["blobName"],
                "contentType": media_doc.get("contentType")
            })

    post_id = str(uuid.uuid4())
    post_doc = {
        "id": post_id,
        "userId": user_id,
        "title": title,
        "caption": caption,
        "media": media_refs,
        "createdAt": datetime.now(timezone.utc).isoformat()
    }

    post_container = get_cosmos_container("COSMOS_POST_CONTAINER")
    post_container.create_item(body=post_doc)

    return func.HttpResponse(
        body=json.dumps(post_doc),
        status_code=201,
        mimetype="application/json"
    )


@app.route(route="getPosts", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def getPosts(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('getPosts called')

    user_id = req.params.get("userId")

    if not user_id:
        return func.HttpResponse("Missing userId", status_code=400)
    
    post_container = get_cosmos_container("COSMOS_POST_CONTAINER")

    query = """
        SELECT * FROM c
        WHERE c.userId = @userId
        ORDER BY c.createdAt DESC
    """

    items = list(post_container.query_items(query=query, parameters=[{"name": "@userId", "value": user_id}], enable_cross_partition_query=False))

    return func.HttpResponse(
        body=json.dumps(items),
        status_code=200,
        mimetype="application/json"
    )


@app.route(route="getAllPosts", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def getAllPosts(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('getAllPosts called')

    post_container = get_cosmos_container("COSMOS_POST_CONTAINER")

    query = """
        SELECT * FROM c
        ORDER BY c.createdAt DESC
    """

    items = list(post_container.query_items(query=query, enable_cross_partition_query=True))

    return func.HttpResponse(
        body=json.dumps(items),
        status_code=200,
        mimetype="application/json"
    )


@app.route(route="getUserMedia", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def getUserMedia(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('getUserMedia called')

    user_id = req.params.get("userId")
    if not user_id:
        return func.HttpResponse("Missing userId", status_code=400)
    
    media_container = get_cosmos_container("COSMOS_MEDIA_CONTAINER")

    query = """
        SELECT * FROM c
        WHERE c.userId = @userId
        ORDER BY c.uploadedAt DESC
    """

    items = list(media_container.query_items(query=query, parameters=[{"name": "@userId", "value": user_id}], enable_cross_partition_query=False))

    return func.HttpResponse(
        body=json.dumps(items),
        status_code=200,
        mimetype="application/json"
    )


@app.route(route="deletePost", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
def deletePost(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('deletePost called')

    user_id = req.params.get("userId")
    post_id = req.params.get("postId")

    if not user_id or not post_id:
        return func.HttpResponse("Missing userId or postId", status_code=400)
    
    post_container = get_cosmos_container("COSMOS_POST_CONTAINER")

    try:
        post_container.delete_item(item=post_id, partition_key=user_id)
    except Exception as e:
        logging.exception("Failed to delete post")
        return func.HttpResponse("Post not found", status_code=404)
    
    return func.HttpResponse(status_code=204)


@app.route(route="deleteMedia", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
def deleteMedia(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('deleteMedia called')

    user_id = req.params.get("userId")
    media_id = req.params.get("mediaId")

    if not user_id or not media_id:
        return func.HttpResponse("missing userId or mediaId", status_code=400)
    
    media_container = get_cosmos_container("COSMOS_MEDIA_CONTAINER")

    try:
        media_doc = media_container.read_item(item=media_id, partition_key=user_id)
    except Exception:
        return func.HttpResponse("media not found", status_code=404)
    
    blob_name = media_doc.get("blobName")
    if not blob_name:
        return func.HttpResponse("media record missing blobname", status_code=500)
    
    try:
        blob_service = BlobServiceClient.from_connection_string(os.environ["MEDIA_STORAGE_CONNECTION_STRING"])
        container_client = blob_service.get_container_client(os.environ["BLOB_CONTAINER"])
        blob_client = container_client.get_blob_client(blob_name)

        blob_client.delete_blob()
    except Exception:
        logging.exception("failed to delete blob")
        return func.HttpResponse("failed to delete media blob", status_code=500)
    
    try:
        media_container.delete_item(item=media_id, partition_key=user_id)
    except Exception:
        logging.exception('failed to delete cosmos document')
        return func.HttpResponse('failed to delete cosmos document', status_code=500)
    
    return func.HttpResponse(status_code=204)


@app.route(route="analyseMedia", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def analyseMedia(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("analyseMedia called")

    supported_image_types = {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/bmp"
    }

    user_id = req.params.get("userId")
    media_id = req.params.get("mediaId")

    if not user_id or not media_id:
        return func.HttpResponse("Missing userId or mediaId", status_code=400)
    
    media_container = get_cosmos_container("COSMOS_MEDIA_CONTAINER")

    try:
        media_doc = media_container.read_item(item=media_id, partition_key=user_id)
    except Exception:
        return func.HttpResponse("Media not found", status_code=404)
    
    blob_name = media_doc.get("blobName")
    content_type = media_doc.get("contentType", "application/octet-stream")

    if content_type not in supported_image_types:
        media_doc["imageAnalysis"] = {
            "status": "skipped",
            "reason": "unsupported media type",
            "contentType": content_type
        }
        media_container.upsert_item(media_doc)

        return func.HttpResponse(
            body=json.dumps(media_doc),
            status_code=200,
            mimetype="application/json"
        )

    if not blob_name:
        return func.HttpResponse("media record missing blobName", status_code=500)
    
    try:
        blob_service = BlobServiceClient.from_connection_string(os.environ["MEDIA_STORAGE_CONNECTION_STRING"])
        container_client = blob_service.get_container_client(os.environ["BLOB_CONTAINER"])
        blob_client = container_client.get_blob_client(blob_name)

        image_bytes = blob_client.download_blob().readall()
    except Exception:
        logging.exception("failed to download blob for analysis")
        return func.HttpResponse("failed to download blob", status_code=500)
    
    try:
        result = analyseImage(image_bytes, content_type)

        captions = (result.get("description") or {}).get("captions") or []
        best_caption = captions[0] if captions else None

        raw_tags = result.get("tags", [])
        limited_tags = limitTags(raw_tags, max_tags=5)

        analysis_doc = {
            "status": "complete",
            "analysedAt": datetime.now(timezone.utc).isoformat(),
            "caption": best_caption,
            "tags": limited_tags,
            "metadata": result.get("metadata"),
            "modelVersion": result.get("modelVersion")
        }

        media_doc["imageAnalysis"] = analysis_doc
        media_container.upsert_item(media_doc)

        return func.HttpResponse(
            body=json.dumps(analysis_doc),
            status_code=200,
            mimetype="application/json"
        )
    
    except requests.HTTPError as e:
        logging.exception("vision http error")
        
        vision_status = getattr(e.response, "status_code", None)
        vision_body = getattr(e.response, "text", None)

        media_doc["imageAnalysis"] = {
            "status": "failed",
            "analysedAt": datetime.now(timezone.utc).isoformat(),
            "visionStatus": vision_status,
            "visionError": vision_body or str(e)
        }
        media_container.upsert_item(media_doc)
        return func.HttpResponse(
            body=json.dumps(media_doc["imageAnalysis"]),
            status_code=502,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.exception("vision analysis failed")
        media_doc["imageAnalysis"] = {
            "status": "failed",
            "analysedAt": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }
        media_container.upsert_item(media_doc)
        return func.HttpResponse("vision analysis failed", status_code=500)