"""Async Google Cloud Storage client for AI dubbing pipeline."""

import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import aiofiles
import os

from google.cloud import storage
from google.cloud.exceptions import NotFound, GoogleCloudError
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class AsyncGCSClient:
    """Async Google Cloud Storage client for dubbing pipeline."""
    
    def __init__(self, config):
        """Initialize GCS client with configuration."""
        self.config = config
        self.client: Optional[storage.Client] = None
        self.bucket: Optional[storage.Bucket] = None
        
    async def connect(self) -> None:
        """Establish connection to Google Cloud Storage."""
        try:
            # Set up authentication if credentials are provided
            if self.config.google_application_credentials:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.config.google_application_credentials
                
            # Initialize client in thread pool since it's sync
            self.client = await asyncio.to_thread(
                storage.Client, 
                project=self.config.google_cloud_project
            )
            
            self.bucket = self.client.bucket(self.config.gcs_bucket_name)
            
            # Test bucket access in thread pool
            bucket_exists = await asyncio.to_thread(self.bucket.exists)
            if not bucket_exists:
                raise ValueError(f"Bucket {self.config.gcs_bucket_name} does not exist")
                
            logger.info(f"Successfully connected to GCS bucket: {self.config.gcs_bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to GCS: {e}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def download_file(self, gcs_path: str, local_path: str) -> None:
        """Download file from GCS to local storage with retry logic."""
        if not self.bucket:
            raise RuntimeError("GCS client not connected. Call connect() first.")
            
        try:
            # Create local directory if needed
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            blob = self.bucket.blob(gcs_path)
            
            # Check if blob exists
            blob_exists = await asyncio.to_thread(blob.exists)
            if not blob_exists:
                raise NotFound(f"File not found in GCS: {gcs_path}")
                
            # Reload blob metadata
            await asyncio.to_thread(blob.reload)
            
            # Check file size
            file_size_mb = blob.size / (1024 * 1024)
            if file_size_mb > self.config.max_file_size_mb:
                raise ValueError(f"File too large: {file_size_mb:.1f}MB > {self.config.max_file_size_mb}MB")
            
            logger.info(f"Downloading {gcs_path} ({file_size_mb:.1f}MB) to {local_path}")
            
            # Download file asynchronously
            await asyncio.to_thread(blob.download_to_filename, local_path)
            
            # Verify download
            if not os.path.exists(local_path):
                raise RuntimeError(f"Download failed: {local_path} not found")
                
            local_size = os.path.getsize(local_path)
            if local_size != blob.size:
                raise RuntimeError(f"Size mismatch: expected {blob.size}, got {local_size}")
                
            logger.info(f"Successfully downloaded {gcs_path}")
            
        except Exception as e:
            logger.error(f"Failed to download {gcs_path}: {e}")
            # Clean up partial download
            if os.path.exists(local_path):
                os.remove(local_path)
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def upload_file(self, local_path: str, gcs_path: str) -> str:
        """Upload file from local storage to GCS with retry logic."""
        if not self.bucket:
            raise RuntimeError("GCS client not connected. Call connect() first.")
            
        try:
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")
            
            file_size = os.path.getsize(local_path)
            file_size_mb = file_size / (1024 * 1024)
            
            logger.info(f"Uploading {local_path} ({file_size_mb:.1f}MB) to {gcs_path}")
            
            blob = self.bucket.blob(gcs_path)
            
            # Upload file asynchronously
            await asyncio.to_thread(blob.upload_from_filename, local_path)
            
            # Verify upload
            await asyncio.to_thread(blob.reload)
            if blob.size != file_size:
                raise RuntimeError(f"Upload size mismatch: expected {file_size}, got {blob.size}")
                
            logger.info(f"Successfully uploaded {gcs_path}")
            return f"gs://{self.config.gcs_bucket_name}/{gcs_path}"
            
        except Exception as e:
            logger.error(f"Failed to upload {local_path} to {gcs_path}: {e}")
            raise
    
    async def file_exists(self, gcs_path: str) -> bool:
        """Check if file exists in GCS."""
        if not self.bucket:
            raise RuntimeError("GCS client not connected. Call connect() first.")
            
        try:
            blob = self.bucket.blob(gcs_path)
            return await asyncio.to_thread(blob.exists)
        except Exception as e:
            logger.error(f"Error checking file existence {gcs_path}: {e}")
            return False
    
    async def get_file_info(self, gcs_path: str) -> Dict[str, Any]:
        """Get file metadata from GCS."""
        if not self.bucket:
            raise RuntimeError("GCS client not connected. Call connect() first.")
            
        try:
            blob = self.bucket.blob(gcs_path)
            await asyncio.to_thread(blob.reload)
            
            return {
                "name": blob.name,
                "size": blob.size,
                "size_mb": blob.size / (1024 * 1024),
                "content_type": blob.content_type,
                "created": blob.time_created,
                "updated": blob.updated,
                "etag": blob.etag,
            }
        except NotFound:
            raise FileNotFoundError(f"File not found: {gcs_path}")
        except Exception as e:
            logger.error(f"Error getting file info {gcs_path}: {e}")
            raise
    
    async def delete_file(self, gcs_path: str) -> bool:
        """Delete file from GCS."""
        if not self.bucket:
            raise RuntimeError("GCS client not connected. Call connect() first.")
            
        try:
            blob = self.bucket.blob(gcs_path)
            await asyncio.to_thread(blob.delete)
            logger.info(f"Deleted file: {gcs_path}")
            return True
        except NotFound:
            logger.warning(f"File not found for deletion: {gcs_path}")
            return False
        except Exception as e:
            logger.error(f"Error deleting file {gcs_path}: {e}")
            raise
    
    async def list_files(self, prefix: str = "", limit: int = 100) -> list:
        """List files in bucket with optional prefix."""
        if not self.bucket:
            raise RuntimeError("GCS client not connected. Call connect() first.")
            
        try:
            blobs = await asyncio.to_thread(
                list, 
                self.client.list_blobs(self.bucket, prefix=prefix, max_results=limit)
            )
            
            return [
                {
                    "name": blob.name,
                    "size": blob.size,
                    "created": blob.time_created,
                    "updated": blob.updated,
                }
                for blob in blobs
            ]
        except Exception as e:
            logger.error(f"Error listing files with prefix {prefix}: {e}")
            raise