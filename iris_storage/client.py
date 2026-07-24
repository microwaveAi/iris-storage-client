import httpx
import os
import logging
import time
from contextlib import asynccontextmanager

from typing import Optional

logger = logging.getLogger("iris.storage")

if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

STORAGE_PROTOCOL = os.getenv("STORAGE_PROTOCOL", "http")
STORAGE_HOST = os.getenv("STORAGE_HOST", "storage-sidecar")
STORAGE_PORT = os.getenv("STORAGE_PORT", "5000")

STORAGE_URL = f"{STORAGE_PROTOCOL}://{STORAGE_HOST}:{STORAGE_PORT}"

class StorageClient:
    _async_client = httpx.AsyncClient(base_url=STORAGE_URL, timeout=60.0)
    _sync_client = httpx.Client(base_url=STORAGE_URL, timeout=60.0)

    @classmethod
    async def get_metadata(cls, bucket: str, path: str):
        """
        Récupère les dimensions et le format d'une image sans la télécharger entièrement.
        """
        start_time = time.perf_counter()
        logger.info(f"🔍 [Async] Getting metadata: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket, "path": path}
            response = await cls._async_client.get("/metadata", params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"✅ [Async] Metadata success: {data.get('width')}x{data.get('height')} in {time.perf_counter() - start_time:.3f}s")
            return data
        except Exception as e:
            logger.error(f"💥 [Async] Metadata failed for {path}: {str(e)}")
            raise

    @classmethod
    def get_metadata_sync(cls, bucket: str, path: str):
        """
        Récupère les dimensions et le format d'une image (Synchrone).
        """
        start_time = time.perf_counter()
        logger.info(f"🔍 [Sync] Getting metadata: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket, "path": path}
            response = cls._sync_client.get("/metadata", params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"✅ [Sync] Metadata success: {data.get('width')}x{data.get('height')} in {time.perf_counter() - start_time:.3f}s")
            return data
        except Exception as e:
            logger.error(f"💥 [Sync] Metadata failed for {path}: {str(e)}")
            raise

    @classmethod
    async def upload_stream(cls, bucket: str, path: str, file_obj, content_type: str = None):
        """
        Asynchronously upload a file using multipart streaming.

        The file is read lazily in chunks by httpx and is NEVER fully loaded
        into memory. Multipart format, compatible with the Go sidecar's
        FormFile("file") handler. Runs on the async client, so the event loop
        is not blocked during the upload.
        """
        start_time = time.perf_counter()
        logger.info(f"🌊 [Async Multipart Stream] Uploading: {path} (bucket: {bucket})")

        try:
            # Reset pointer on the underlying file object.
            file_obj.file.seek(0)

            # Passing the file-like object in 'files' tells httpx to stream it
            # in chunks instead of loading it into memory.
            files = {
                'file': (file_obj.filename, file_obj.file, content_type or file_obj.content_type)
            }
            params = {"filename": path, "bucket": bucket}

            # Note: Do NOT manually set Content-Type here,
            # httpx generates the correct Multipart boundary header.
            response = await cls._async_client.post(
                "/upload",
                files=files,
                params=params,
            )
            response.raise_for_status()

            logger.info(f"✅ [Async Multipart Stream] Success in {time.perf_counter() - start_time:.3f}s")
            return response.json()
        except Exception as e:
            logger.error(f"💥 [Async Multipart Stream] Upload failed: {str(e)}")
            raise

    @classmethod
    def upload_stream_sync(cls, bucket: str, path: str, file_obj):
        """
        Synchronously uploads a file using multipart streaming.
        This is compatible with Go's r.FormFile("file").
        """
        start_time = time.perf_counter()
        logger.info(f"📤 [Sync Multipart] Uploading: {path} (bucket: {bucket})")
        
        try:
            # Reset pointer
            file_obj.file.seek(0)
            
            # Passing the file-like object in 'files' tells httpx 
            # to stream it instead of loading it into memory.
            files = {
                'file': (file_obj.filename, file_obj.file, file_obj.content_type)
            }
            params = {"filename": path, "bucket": bucket}
            
            # Note: Do NOT manually set Content-Type header here, 
            # httpx will generate the correct Multipart boundary header.
            response = cls._sync_client.post(
                "/upload", 
                files=files, 
                params=params
            )
            response.raise_for_status()
            
            logger.info(f"✅ [Sync Multipart] Success in {time.perf_counter() - start_time:.3f}s")
            return response.json()
        except Exception as e:
            logger.error(f"💥 [Sync Multipart] Failed: {str(e)}")
            raise

    @classmethod
    async def upload_bytes(cls, bucket: str, path: str, data: bytes, content_type: str = "application/octet-stream"):
        start_time = time.perf_counter()
        logger.info(f"📤 [Async] Uploading: {path} (bucket: {bucket}, size: {len(data)} bytes)")
        try:
            files = {'file': (path, data, content_type)}
            params = {"filename": path, "bucket": bucket}
            response = await cls._async_client.post("/upload", files=files, params=params)
            response.raise_for_status()
            logger.info(f"✅ [Async] Upload success: {path} in {time.perf_counter() - start_time:.3f}s")
            return response.json()
        except Exception as e:
            logger.error(f"💥 [Async] Upload failed for {path}: {str(e)}")
            raise

    @classmethod
    async def download_bytes(cls, bucket: str, path: str):
        start_time = time.perf_counter()
        logger.info(f"📥 [Async] Downloading: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket}
            response = await cls._async_client.get(f"/download/{path}", params=params)
            response.raise_for_status()
            logger.info(f"✅ [Async] Download success: {path} in {time.perf_counter() - start_time:.3f}s")
            return response.content
        except Exception as e:
            logger.error(f"💥 [Async] Download failed for {path}: {str(e)}")
            raise

    @classmethod
    @asynccontextmanager
    async def download_stream(
        cls,
        bucket: str,
        path: str,
        format: Optional[str] = None,
        quality: Optional[int] = None,
        max_width: Optional[int] = None,
    ):
        start_time = time.perf_counter()
        logger.info(f"🌊 [Stream] Starting download: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket}
            if format:
                params["format"] = format
            if quality is not None:
                params["quality"] = str(quality)
            if max_width is not None:
                params["max_width"] = str(max_width)
            async with cls._async_client.stream("GET", f"/download/{path}", params=params) as response:
                response.raise_for_status()
                logger.info(f"✅ [Stream] Connection established for {path} in {time.perf_counter() - start_time:.3f}s")
                yield response.aiter_bytes()
        except Exception as e:
            logger.error(f"💥 [Stream] Download failed for {path}: {str(e)}")
            raise

    @classmethod
    def upload_bytes_sync(cls, bucket: str, path: str, data: bytes, content_type: str = "application/octet-stream"):
        start_time = time.perf_counter()
        logger.info(f"📤 [Sync] Uploading: {path} (bucket: {bucket}, size: {len(data)} bytes)")
        try:
            files = {'file': (path, data, content_type)}
            params = {"filename": path, "bucket": bucket}
            response = cls._sync_client.post("/upload", files=files, params=params)
            response.raise_for_status()
            logger.info(f"✅ [Sync] Upload success: {path} in {time.perf_counter() - start_time:.3f}s")
            return response.json()
        except Exception as e:
            logger.error(f"💥 [Sync] Upload failed for {path}: {str(e)}")
            raise

    @classmethod
    def download_bytes_sync(cls, bucket: str, path: str):
        start_time = time.perf_counter()
        logger.info(f"📥 [Sync] Downloading: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket}
            response = cls._sync_client.get(f"/download/{path}", params=params)
            response.raise_for_status()
            logger.info(f"✅ [Sync] Download success: {path} in {time.perf_counter() - start_time:.3f}s")
            return response.content
        except Exception as e:
            logger.error(f"💥 [Sync] Download failed for {path}: {str(e)}")
            raise

    @classmethod
    async def stream_file(cls, bucket: str, path: str):
        """
        Returns an asynchronous generator that streams the file from the Go sidecar.
        Perfect for FastAPI StreamingResponse.
        """
        start_time = time.perf_counter()
        logger.info(f"🌊 [Async] Streaming: {path} (bucket: {bucket})")
        
        params = {"bucket": bucket}
        
        try:
            async def _aiter_stream():
                async with cls._async_client.stream("GET", f"/download/{path}", params=params) as response:
                    if response.status_code != 200:
                        logger.error(f"💥 [Async] Stream failed: Sidecar returned {response.status_code}")
                        response.raise_for_status()
                    
                    async for chunk in response.aiter_bytes():
                        yield chunk
                
                logger.info(f"✅ [Async] Stream success: {path} finished in {time.perf_counter() - start_time:.3f}s")

            return _aiter_stream()
            
        except Exception as e:
            logger.error(f"💥 [Async] Stream connection failed for {path}: {str(e)}")
            raise

    @classmethod
    async def delete_object(cls, bucket: str, path: str):
        """Delete a single object from GCS via sidecar."""
        logger.info(f"🗑️ [Async] Deleting object: {path} (bucket: {bucket})")
        try:
            params = {"path": path, "bucket": bucket}
            response = await cls._async_client.delete("/delete", params=params)
            response.raise_for_status()
            logger.info(f"✅ [Async] Object deleted: {path}")
            return True
        except Exception as e:
            logger.error(f"💥 [Async] Delete failed for {path}: {str(e)}")
            return False

    @classmethod
    async def download_and_delete(cls, bucket: str, path: str):
        start_time = time.perf_counter()
        logger.info(f"📥🔥 [Async] Download & Delete: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket}
            # Note: Le path est passé dans l'URL comme pour le download classique
            response = await cls._async_client.get(f"/download-and-delete/{path}", params=params)
            response.raise_for_status()
            logger.info(f"✅ [Async] Downloaded & Deleted: {path} in {time.perf_counter() - start_time:.3f}s")
            return response.content
        except Exception as e:
            logger.error(f"💥 [Async] Download & Delete failed for {path}: {str(e)}")
            raise

    @classmethod
    def download_and_delete_sync(cls, bucket: str, path: str):
        start_time = time.perf_counter()
        logger.info(f"📥🔥 [Sync] Download & Delete: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket}
            response = cls._sync_client.get(f"/download-and-delete/{path}", params=params)
            response.raise_for_status()
            logger.info(f"✅ [Sync] Downloaded & Deleted: {path} in {time.perf_counter() - start_time:.3f}s")
            return response.content
        except Exception as e:
            logger.error(f"💥 [Sync] Download & Delete failed for {path}: {str(e)}")
            raise

    @classmethod
    async def delete_folder(cls, bucket: str, path: str):
        """Delete all contents of a folder (prefix) from GCS via sidecar."""
        logger.info(f"📂 [Async] Deleting folder: {path} (bucket: {bucket})")
        try:
            params = {"prefix": path, "bucket": bucket}
            response = await cls._async_client.delete("/delete-folder", params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"✅ [Async] Folder deleted: {path} ({data.get('deleted_count')} objects)")
            return True
        except Exception as e:
            logger.error(f"💥 [Async] Folder delete failed for {path}: {str(e)}")
            return False

    @classmethod
    def delete_folder_sync(cls, bucket: str, path: str):
        """
        Delete all contents of a folder (prefix) from GCS via sidecar (Synchronous).
        Ideal for Celery tasks or standalone scripts.
        """
        start_time = time.perf_counter()
        logger.info(f"📂 [Sync] Deleting folder: {path} (bucket: {bucket})")
        try:
            params = {"prefix": path, "bucket": bucket}
            # Utilisation du client synchrone
            response = cls._sync_client.delete("/delete-folder", params=params)
            response.raise_for_status()
            
            data = response.json()
            deleted_count = data.get('deleted_count', 0)
            duration = time.perf_counter() - start_time
            
            logger.info(f"✅ [Sync] Folder deleted: {path} ({deleted_count} objects) in {duration:.3f}s")
            return True
        except Exception as e:
            logger.error(f"💥 [Sync] Folder delete failed for {path}: {str(e)}")
            return False

    @classmethod
    def delete_object_sync(cls, bucket: str, path: str):
        params = {"path": path, "bucket": bucket}
        response = cls._sync_client.delete("/delete", params=params)
        response.raise_for_status()
        return True

    @classmethod
    async def copy_object(cls, src_path: str, dest_path: str, bucket: Optional[str] = None):
        """
        Duplicate an object from one path to another in GCS (Async). Useful for renaming or moving files without downloading/uploading.
        """
        start_time = time.perf_counter()
        logger.info(f"👯 [Async] Copying: {src_path} -> {dest_path}")
        try:
            params = {
                "src_filename": src_path, 
                "dest_filename": dest_path
            }
            if bucket:
                params["bucket"] = bucket
                
            response = await cls._async_client.post("/copy", params=params)
            response.raise_for_status()
            
            duration = time.perf_counter() - start_time
            logger.info(f"✅ [Async] Copy success in {duration:.3f}s")
            return response.json()
        except Exception as e:
            logger.error(f"💥 [Async] Copy failed: {str(e)}")
            raise

    @classmethod
    def copy_object_sync(cls, src_path: str, dest_path: str, bucket: Optional[str] = None):
        """
        Duplicate an object from one path to another in GCS (Synchronous - for Celery tasks).
        """
        start_time = time.perf_counter()
        logger.info(f"👯 [Sync] Copying: {src_path} -> {dest_path}")
        try:
            params = {
                "src_filename": src_path, 
                "dest_filename": dest_path
            }
            if bucket:
                params["bucket"] = bucket

            response = cls._sync_client.post("/copy", params=params)
            response.raise_for_status()
            
            duration = time.perf_counter() - start_time
            logger.info(f"✅ [Sync] Copy success in {duration:.3f}s")
            return response.json()
        except Exception as e:
            logger.error(f"💥 [Sync] Copy failed: {str(e)}")
            raise

    @classmethod
    @asynccontextmanager
    async def download_zip_stream(
        cls,
        entries: list,
        filename: str = "download.zip",
        output_format: Optional[str] = None,
        quality: int = 85,
        max_width: Optional[int] = None,
    ):
        """Stream a ZIP archive assembled on-the-fly by the Go storage service.

        The Go sidecar reads each GCS object and pipes it directly into a
        ``archive/zip`` writer whose output is the HTTP response body — no
        intermediate disk or full-archive buffer on either side.

        Args:
            entries: List of ``{"bucket": str, "path": str, "name": str}`` dicts.
                     ``bucket`` falls back to the server default when omitted.
                     ``name`` is the filename inside the ZIP.
            filename: Suggested ZIP filename sent in ``Content-Disposition``.
            output_format: Optional re-encoding format (``"jpeg"`` or ``"png"``).
                           When *None* the original file bytes are served as-is
                           (fastest path — true zero-copy streaming from GCS).
            quality: JPEG quality (1-95). Ignored when *output_format* is ``"png"``.
            max_width: Downscale each image to this pixel width preserving ratio.
                       ``None`` / 0 disables resizing.

        Yields:
            An async iterator of raw bytes chunks for use in a
            :class:`~fastapi.responses.StreamingResponse`.
        """
        params: dict = {}
        if output_format:
            params["format"] = output_format
            params["quality"] = str(quality)
        if max_width:
            params["max_width"] = str(max_width)

        body = {"entries": entries, "filename": filename}
        logger.info(f"🗜️ [Zip Stream] Requesting {len(entries)} entries → {filename}")
        start_time = time.perf_counter()
        try:
            async with cls._async_client.stream("POST", "/download-zip", json=body, params=params) as response:
                response.raise_for_status()
                logger.info(f"✅ [Zip Stream] Connection established in {time.perf_counter() - start_time:.3f}s")
                yield response.aiter_bytes()
        except Exception as e:
            logger.error(f"💥 [Zip Stream] Failed: {str(e)}")
            raise

    @classmethod
    async def copy_folder(cls, src_prefix: str, dest_prefix: str, bucket: Optional[str] = None):
        """
        Duplicate an entire folder in GCS (Async).
        """
        params = {"src_prefix": src_prefix, "dest_prefix": dest_prefix}
        if bucket: params["bucket"] = bucket
        
        response = await cls._async_client.post("/copy-folder", params=params)
        response.raise_for_status()
        return response.json()

    @classmethod
    def copy_folder_sync(cls, src_prefix: str, dest_prefix: str, bucket: Optional[str] = None):
        """
        Duplicate an entire folder in GCS (Synchronous - for Celery tasks).
        """
        params = {"src_prefix": src_prefix, "dest_prefix": dest_prefix}
        if bucket: params["bucket"] = bucket
        
        response = cls._sync_client.post("/copy-folder", params=params)
        response.raise_for_status()
        return response.json()