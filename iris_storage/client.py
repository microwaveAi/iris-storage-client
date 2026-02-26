import httpx
import os
import logging
import time

logger = logging.getLogger("iris.storage")
# On ne garde qu'un seul basicConfig global
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

STORAGE_SIDECAR_URL = os.getenv("STORAGE_SIDECAR_URL", "http://storage-sidecar:5000")

class StorageClient:
    # Clients partagés pour optimiser le pool de connexions
    _async_client = httpx.AsyncClient(base_url=STORAGE_SIDECAR_URL, timeout=60.0)
    _sync_client = httpx.Client(base_url=STORAGE_SIDECAR_URL, timeout=60.0)

    @classmethod
    async def get_metadata(cls, bucket: str, path: str):
        """
        Récupère les dimensions et le format d'une image sans la télécharger entièrement.
        """
        start_time = time.perf_counter()
        logger.info(f"🔍 [Async] Getting metadata: {path} (bucket: {bucket})")
        try:
            params = {"bucket": bucket, "path": path}
            # On utilise le client async déjà configuré avec STORAGE_SIDECAR_URL
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

    # --- MÉTHODES SYNCHRONES (Celery / Scripts) ---

    @classmethod
    def upload_bytes_sync(cls, bucket: str, path: str, data: bytes, content_type: str = "application/octet-stream"):
        start_time = time.perf_counter()
        logger.info(f"📤 [Sync] Uploading: {path} (bucket: {bucket}, size: {len(data)} bytes)")
        try:
            files = {'file': (path, data, content_type)}
            params = {"filename": path, "bucket": bucket}
            # Utilisation du client synchrone
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
        
        # On utilise .stream() pour ne pas charger le fichier en mémoire
        # Le 'async with' est crucial pour garder la connexion ouverte pendant le stream
        try:
            # On définit un générateur interne
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