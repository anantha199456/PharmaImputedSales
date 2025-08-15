"""
File and I/O utilities for the Imputed Sales Analytics Platform.

This module provides functions for working with files, directories, and I/O operations
in a PySpark environment.
"""

import os
import re
import json
import yaml
import logging
import shutil
import tempfile
import zipfile
import gzip
import bz2
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, BinaryIO, TextIO, Tuple
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

def ensure_directory_exists(directory: Union[str, Path]) -> Path:
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        directory: Directory path
        
    Returns:
        Path object for the directory
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_extension(file_path: Union[str, Path]) -> str:
    """
    Get the file extension in lowercase.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File extension (without the dot) in lowercase
    """
    return Path(file_path).suffix.lower().lstrip('.')


def is_valid_filename(filename: str) -> bool:
    """
    Check if a filename is valid.
    
    Args:
        filename: Filename to check
        
    Returns:
        True if the filename is valid, False otherwise
    """
    # Define invalid characters for filenames (Windows + Unix)
    invalid_chars = r'[<>:"/\\|?*\x00-\x1F]'
    
    # Check for empty filename
    if not filename or filename.strip() == '':
        return False
    
    # Check for invalid characters
    if re.search(invalid_chars, filename):
        return False
    
    # Check for reserved filenames (Windows)
    reserved_names = {
        'CON', 'PRN', 'AUX', 'NUL',
        'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
        'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    }
    
    name_without_ext = Path(filename).stem.upper()
    if name_without_ext in reserved_names:
        return False
    
    return True


def sanitize_filename(filename: str, replacement: str = '_') -> str:
    """
    Sanitize a filename by replacing invalid characters.
    
    Args:
        filename: Input filename
        replacement: Character to replace invalid characters with
        
    Returns:
        Sanitized filename
    """
    # Replace invalid characters
    invalid_chars = r'[<>:"/\\|?*\x00-\x1F]'
    sanitized = re.sub(invalid_chars, replacement, filename)
    
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip('. ')
    
    # Ensure the filename is not empty
    if not sanitized:
        sanitized = f'unnamed_file_{int(time.time())}'
    
    return sanitized


def read_text_file(file_path: Union[str, Path], encoding: str = 'utf-8') -> str:
    """
    Read the contents of a text file.
    
    Args:
        file_path: Path to the file
        encoding: File encoding
        
    Returns:
        File contents as a string
    """
    with open(file_path, 'r', encoding=encoding) as f:
        return f.read()


def write_text_file(
    file_path: Union[str, Path], 
    content: str, 
    encoding: str = 'utf-8',
    create_parents: bool = True
) -> None:
    """
    Write content to a text file.
    
    Args:
        file_path: Path to the file
        content: Content to write
        encoding: File encoding
        create_parents: Whether to create parent directories if they don't exist
    """
    path = Path(file_path)
    if create_parents:
        path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding=encoding) as f:
        f.write(content)


def read_json_file(file_path: Union[str, Path], **kwargs) -> Any:
    """
    Read a JSON file.
    
    Args:
        file_path: Path to the JSON file
        **kwargs: Additional arguments to pass to json.load()
        
    Returns:
        Parsed JSON data
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f, **kwargs)


def write_json_file(
    file_path: Union[str, Path], 
    data: Any, 
    indent: int = 2,
    create_parents: bool = True,
    **kwargs
) -> None:
    """
    Write data to a JSON file.
    
    Args:
        file_path: Path to the JSON file
        data: Data to write (must be JSON-serializable)
        indent: Indentation level for pretty-printing
        create_parents: Whether to create parent directories if they don't exist
        **kwargs: Additional arguments to pass to json.dump()
    """
    path = Path(file_path)
    if create_parents:
        path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, **kwargs)


def read_yaml_file(file_path: Union[str, Path], **kwargs) -> Any:
    """
    Read a YAML file.
    
    Args:
        file_path: Path to the YAML file
        **kwargs: Additional arguments to pass to yaml.safe_load()
        
    Returns:
        Parsed YAML data
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f, **kwargs)


def write_yaml_file(
    file_path: Union[str, Path], 
    data: Any, 
    default_flow_style: bool = False,
    create_parents: bool = True,
    **kwargs
) -> None:
    """
    Write data to a YAML file.
    
    Args:
        file_path: Path to the YAML file
        data: Data to write
        default_flow_style: Whether to use flow style for collections
        create_parents: Whether to create parent directories if they don't exist
        **kwargs: Additional arguments to pass to yaml.safe_dump()
    """
    path = Path(file_path)
    if create_parents:
        path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(
            data, 
            f, 
            default_flow_style=default_flow_style,
            allow_unicode=True,
            **kwargs
        )


def download_file(
    url: str, 
    output_path: Union[str, Path],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    verify_ssl: bool = True,
    chunk_size: int = 8192
) -> Path:
    """
    Download a file from a URL.
    
    Args:
        url: Source URL
        output_path: Destination path
        headers: HTTP headers
        timeout: Request timeout in seconds
        verify_ssl: Whether to verify SSL certificates
        chunk_size: Chunk size for streaming download
        
    Returns:
        Path to the downloaded file
        
    Raises:
        URLError: If the download fails
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Set up the request
    req = Request(url, headers=headers or {})
    
    try:
        with urlopen(req, timeout=timeout) as response, open(output_path, 'wb') as out_file:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                out_file.write(chunk)
        
        logger.info(f"Downloaded {url} to {output_path}")
        return output_path
    
    except (URLError, HTTPError) as e:
        logger.error(f"Failed to download {url}: {str(e)}")
        raise


def unzip_file(
    zip_path: Union[str, Path],
    extract_dir: Optional[Union[str, Path]] = None,
    members: Optional[List[str]] = None,
    pwd: Optional[bytes] = None
) -> Path:
    """
    Extract a ZIP archive.
    
    Args:
        zip_path: Path to the ZIP file
        extract_dir: Directory to extract to (defaults to the same directory as the ZIP file)
        members: List of members to extract (None for all)
        pwd: Password for encrypted ZIP files
        
    Returns:
        Path to the extraction directory
    """
    zip_path = Path(zip_path)
    if extract_dir is None:
        extract_dir = zip_path.parent / zip_path.stem
    else:
        extract_dir = Path(extract_dir)
    
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir, members=members, pwd=pwd)
    
    logger.info(f"Extracted {zip_path} to {extract_dir}")
    return extract_dir


def compress_file(
    input_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    compression: str = 'gzip',
    remove_original: bool = False
) -> Path:
    """
    Compress a file using gzip, bz2, or zip.
    
    Args:
        input_path: Path to the input file
        output_path: Output path (defaults to input_path + .gz/.bz2/.zip)
        compression: Compression algorithm ('gzip', 'bz2', or 'zip')
        remove_original: Whether to remove the original file after compression
        
    Returns:
        Path to the compressed file
    """
    input_path = Path(input_path)
    
    if output_path is None:
        if compression == 'gzip':
            output_path = input_path.with_suffix(input_path.suffix + '.gz')
        elif compression == 'bz2':
            output_path = input_path.with_suffix(input_path.suffix + '.bz2')
        elif compression == 'zip':
            output_path = input_path.with_suffix('.zip')
        else:
            raise ValueError(f"Unsupported compression: {compression}")
    else:
        output_path = Path(output_path)
    
    # Ensure the output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Perform the compression
    if compression == 'gzip':
        with open(input_path, 'rb') as f_in, gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    elif compression == 'bz2':
        with open(input_path, 'rb') as f_in, bz2.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    elif compression == 'zip':
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(input_path, arcname=input_path.name)
    
    else:
        raise ValueError(f"Unsupported compression: {compression}")
    
    # Remove the original file if requested
    if remove_original:
        input_path.unlink()
    
    logger.info(f"Compressed {input_path} to {output_path}")
    return output_path


def list_files(
    directory: Union[str, Path],
    pattern: str = '*',
    recursive: bool = False,
    full_path: bool = True
) -> List[Path]:
    """
    List files in a directory matching a pattern.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern to match
        recursive: Whether to search recursively
        full_path: Whether to return full paths
        
    Returns:
        List of matching files
    """
    directory = Path(directory)
    
    if recursive:
        files = list(directory.rglob(pattern))
    else:
        files = list(directory.glob(pattern))
    
    # Filter out directories
    files = [f for f in files if f.is_file()]
    
    if not full_path:
        files = [f.relative_to(directory) for f in files]
    
    return files


def find_files(
    directory: Union[str, Path],
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    recursive: bool = True,
    case_sensitive: bool = False,
    full_path: bool = True
) -> List[Path]:
    """
    Find files matching specific patterns.
    
    Args:
        directory: Directory to search
        include_patterns: List of glob patterns to include
        exclude_patterns: List of glob patterns to exclude
        recursive: Whether to search recursively
        case_sensitive: Whether pattern matching is case-sensitive
        full_path: Whether to return full paths
        
    Returns:
        List of matching files
    """
    directory = Path(directory)
    
    if include_patterns is None:
        include_patterns = ['*']
    
    if exclude_patterns is None:
        exclude_patterns = []
    
    # Compile regex patterns for include/exclude
    def compile_patterns(patterns, case_sensitive):
        flags = 0 if case_sensitive else re.IGNORECASE
        return [re.compile(fnmatch.translate(p), flags) for p in patterns]
    
    include_regex = compile_patterns(include_patterns, case_sensitive)
    exclude_regex = compile_patterns(exclude_patterns, case_sensitive)
    
    # Find all files
    all_files = []
    
    if recursive:
        for root, _, files in os.walk(directory):
            for file in files:
                all_files.append(Path(root) / file)
    else:
        all_files = [f for f in directory.iterdir() if f.is_file()]
    
    # Filter files based on patterns
    def matches_any_pattern(path, patterns):
        return any(pattern.fullmatch(str(path)) for pattern in patterns)
    
    matched_files = []
    
    for file_path in all_files:
        rel_path = file_path.relative_to(directory) if not full_path else file_path
        
        # Check if file matches any include pattern
        if not matches_any_pattern(rel_path, include_regex):
            continue
        
        # Check if file matches any exclude pattern
        if matches_any_pattern(rel_path, exclude_regex):
            continue
        
        matched_files.append(file_path if full_path else rel_path)
    
    return matched_files


def get_file_size(file_path: Union[str, Path], human_readable: bool = False) -> Union[int, str]:
    """
    Get the size of a file.
    
    Args:
        file_path: Path to the file
        human_readable: Whether to return size in human-readable format
        
    Returns:
        File size in bytes or human-readable string
    """
    size = Path(file_path).stat().st_size
    
    if not human_readable:
        return size
    
    # Convert to human-readable format
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    
    return f"{size:.2f} PB"


def get_file_hash(
    file_path: Union[str, Path],
    algorithm: str = 'sha256',
    chunk_size: int = 8192
) -> str:
    """
    Calculate the hash of a file.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm (e.g., 'md5', 'sha1', 'sha256')
        chunk_size: Chunk size for reading large files
        
    Returns:
        Hexadecimal digest of the file
    """
    import hashlib
    
    hash_func = hashlib.new(algorithm)
    
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()


def create_temp_file(
    suffix: str = '',
    prefix: str = 'tmp',
    dir: Optional[Union[str, Path]] = None,
    delete: bool = True,
    text: bool = True
) -> Path:
    """
    Create a temporary file.
    
    Args:
        suffix: File suffix (e.g., '.txt')
        prefix: File prefix
        dir: Directory to create the file in
        delete: Whether to delete the file when closed
        text: Whether to open in text mode
        
    Returns:
        Path to the temporary file
    """
    mode = 'w+' if text else 'w+b'
    
    if dir is not None:
        dir = Path(dir)
        dir.mkdir(parents=True, exist_ok=True)
    
    fd, path = tempfile.mkstemp(
        suffix=suffix,
        prefix=prefix,
        dir=str(dir) if dir else None,
        text=text
    )
    
    # Close the file descriptor (we'll open it again if needed)
    os.close(fd)
    
    # Set up automatic deletion if requested
    if delete:
        import atexit
        import weakref
        
        def cleanup(path):
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                pass
        
        # Register cleanup function
        _finalizer = weakref.finalize(
            None, cleanup, path
        )
        
        # Also register with atexit as a fallback
        atexit.register(cleanup, path)
    
    return Path(path)
