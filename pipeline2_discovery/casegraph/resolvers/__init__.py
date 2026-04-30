from .documentcloud_files import DocumentCloudFileResolution, resolve_documentcloud_files
from .muckrock_files import MuckRockFileResolution, resolve_muckrock_released_files

__all__ = [
    "DocumentCloudFileResolution",
    "MuckRockFileResolution",
    "resolve_documentcloud_files",
    "resolve_muckrock_released_files",
]
