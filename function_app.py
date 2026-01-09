import azure.functions as func
import logging
import os
import uuid
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient
from azure.core.exceptions import ResourceExistsError
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_cosmos_container(container: str):
    client = CosmosClient(
        os.environ["COSMOS_ENDPOINT"],
        os.environ["COSMOS_KEY"]
    )
    db = client.get_database_client(os.environ["COSMOS_DATABASE"])
    return db.get_container_client(os.environ[container])

@app.route(route="uploadMedia", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def uploadMedia(req: func.HttpRequest) -> func.HttpResponse:
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
        "uploadedAt": datetime.now(timezone.utc).isoformat()
    }
    container.upsert_item(doc)

    return func.HttpResponse(
        body=json.dumps(doc),
        status_code=201,
        mimetype="application/json"
    )

@app.route(route="createPost", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def createPost(req: func.HttpRequest) -> func.HttpResponse:
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