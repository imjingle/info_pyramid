"""
Async file I/O utilities for AK Unified.
Provides asynchronous alternatives to common file operations.
"""

import asyncio
import json
import os
import csv
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, TextIO, BinaryIO

import aiofiles
import pandas as pd


async def read_text_file(file_path: Union[str, Path], encoding: str = 'utf-8') -> str:
    """Read text file asynchronously."""
    async with aiofiles.open(file_path, 'r', encoding=encoding) as f:
        return await f.read()


async def write_text_file(file_path: Union[str, Path], content: str, encoding: str = 'utf-8') -> None:
    """Write text file asynchronously."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    async with aiofiles.open(file_path, 'w', encoding=encoding) as f:
        await f.write(content)


async def read_binary_file(file_path: Union[str, Path]) -> bytes:
    """Read binary file asynchronously."""
    async with aiofiles.open(file_path, 'rb') as f:
        return await f.read()


async def write_binary_file(file_path: Union[str, Path], content: bytes) -> None:
    """Write binary file asynchronously."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    async with aiofiles.open(file_path, 'wb') as f:
        await f.write(content)


async def read_json_file(file_path: Union[str, Path], encoding: str = 'utf-8') -> Any:
    """Read JSON file asynchronously."""
    content = await read_text_file(file_path, encoding)
    return json.loads(content)


async def write_json_file(
    file_path: Union[str, Path], 
    data: Any, 
    encoding: str = 'utf-8',
    indent: Optional[int] = 2,
    ensure_ascii: bool = False
) -> None:
    """Write JSON file asynchronously."""
    content = json.dumps(data, indent=indent, ensure_ascii=ensure_ascii, default=str)
    await write_text_file(file_path, content, encoding)


async def read_csv_file(
    file_path: Union[str, Path], 
    encoding: str = 'utf-8',
    **pandas_kwargs: Any
) -> pd.DataFrame:
    """Read CSV file asynchronously using pandas."""
    content = await read_text_file(file_path, encoding)
    
    # Use StringIO to simulate file-like object for pandas
    from io import StringIO
    string_buffer = StringIO(content)
    
    return pd.read_csv(string_buffer, **pandas_kwargs)


async def write_csv_file(
    file_path: Union[str, Path], 
    df: pd.DataFrame, 
    encoding: str = 'utf-8',
    index: bool = False,
    **pandas_kwargs: Any
) -> None:
    """Write CSV file asynchronously using pandas."""
    # Convert DataFrame to CSV string
    csv_content = df.to_csv(index=index, **pandas_kwargs)
    await write_text_file(file_path, csv_content, encoding)


async def read_lines(file_path: Union[str, Path], encoding: str = 'utf-8') -> List[str]:
    """Read file line by line asynchronously."""
    lines = []
    async with aiofiles.open(file_path, 'r', encoding=encoding) as f:
        async for line in f:
            lines.append(line.rstrip('\n'))
    return lines


async def write_lines(
    file_path: Union[str, Path], 
    lines: List[str], 
    encoding: str = 'utf-8',
    newline: str = '\n'
) -> None:
    """Write lines to file asynchronously."""
    content = newline.join(lines) + newline
    await write_text_file(file_path, content, encoding)


async def append_text_file(file_path: Union[str, Path], content: str, encoding: str = 'utf-8') -> None:
    """Append text to file asynchronously."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    async with aiofiles.open(file_path, 'a', encoding=encoding) as f:
        await f.write(content)


async def append_line(file_path: Union[str, Path], line: str, encoding: str = 'utf-8') -> None:
    """Append a single line to file asynchronously."""
    await append_text_file(file_path, line + '\n', encoding)


async def file_exists(file_path: Union[str, Path]) -> bool:
    """Check if file exists asynchronously."""
    return os.path.exists(file_path)


async def get_file_size(file_path: Union[str, Path]) -> int:
    """Get file size in bytes asynchronously."""
    if await file_exists(file_path):
        return os.path.getsize(file_path)
    return 0


async def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Get comprehensive file information asynchronously."""
    if not await file_exists(file_path):
        return {"exists": False}
    
    stat = os.stat(file_path)
    return {
        "exists": True,
        "size": stat.st_size,
        "modified": stat.st_mtime,
        "created": stat.st_ctime,
        "is_file": stat.st_mode & 0o40000 == 0,
        "is_dir": stat.st_mode & 0o40000 != 0,
        "permissions": oct(stat.st_mode)[-3:]
    }


async def copy_file(
    source_path: Union[str, Path], 
    dest_path: Union[str, Path], 
    overwrite: bool = False
) -> bool:
    """Copy file asynchronously."""
    if not await file_exists(source_path):
        return False
    
    if await file_exists(dest_path) and not overwrite:
        return False
    
    # Ensure destination directory exists
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    
    # Read source and write to destination
    content = await read_binary_file(source_path)
    await write_binary_file(dest_path, content)
    return True


async def move_file(
    source_path: Union[str, Path], 
    dest_path: Union[str, Path], 
    overwrite: bool = False
) -> bool:
    """Move file asynchronously."""
    if not await file_exists(source_path):
        return False
    
    if await file_exists(dest_path) and not overwrite:
        return False
    
    # Ensure destination directory exists
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    
    # Use asyncio.to_thread for file system operations
    try:
        await asyncio.to_thread(os.rename, source_path, dest_path)
        return True
    except OSError:
        # Fallback to copy + delete
        if await copy_file(source_path, dest_path, overwrite):
            await asyncio.to_thread(os.remove, source_path)
            return True
        return False


async def delete_file(file_path: Union[str, Path]) -> bool:
    """Delete file asynchronously."""
    if not await file_exists(file_path):
        return False
    
    try:
        await asyncio.to_thread(os.remove, file_path)
        return True
    except OSError:
        return False


async def list_directory(
    dir_path: Union[str, Path], 
    pattern: Optional[str] = None,
    recursive: bool = False
) -> List[Dict[str, Any]]:
    """List directory contents asynchronously."""
    if not await file_exists(dir_path):
        return []
    
    files = []
    
    if recursive:
        for root, dirs, filenames in os.walk(dir_path):
            for filename in filenames:
                file_path = os.path.join(root, filename)
                if pattern is None or filename.endswith(pattern):
                    file_info = await get_file_info(file_path)
                    file_info["relative_path"] = os.path.relpath(file_path, dir_path)
                    files.append(file_info)
    else:
        try:
            items = await asyncio.to_thread(os.listdir, dir_path)
            for item in items:
                item_path = os.path.join(dir_path, item)
                if pattern is None or item.endswith(pattern):
                    file_info = await get_file_info(item_path)
                    file_info["name"] = item
                    files.append(file_info)
        except OSError:
            pass
    
    return files


async def create_directory(dir_path: Union[str, Path], parents: bool = True) -> bool:
    """Create directory asynchronously."""
    try:
        await asyncio.to_thread(os.makedirs, dir_path, exist_ok=True)
        return True
    except OSError:
        return False


async def batch_read_files(
    file_paths: List[Union[str, Path]], 
    encoding: str = 'utf-8'
) -> Dict[str, str]:
    """Read multiple text files concurrently."""
    async def read_single_file(file_path: Union[str, Path]) -> tuple[str, str]:
        try:
            content = await read_text_file(file_path, encoding)
            return str(file_path), content
        except Exception as e:
            return str(file_path), f"Error reading file: {e}"
    
    # Read all files concurrently
    tasks = [read_single_file(path) for path in file_paths]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Convert results to dictionary
    file_contents = {}
    for result in results:
        if isinstance(result, Exception):
            continue
        file_path, content = result
        file_contents[file_path] = content
    
    return file_contents


async def batch_write_files(
    file_contents: Dict[str, str], 
    encoding: str = 'utf-8'
) -> Dict[str, bool]:
    """Write multiple text files concurrently."""
    async def write_single_file(file_path: str, content: str) -> tuple[str, bool]:
        try:
            await write_text_file(file_path, content, encoding)
            return file_path, True
        except Exception as e:
            return file_path, False
    
    # Write all files concurrently
    tasks = [
        write_single_file(file_path, content) 
        for file_path, content in file_contents.items()
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Convert results to dictionary
    write_results = {}
    for result in results:
        if isinstance(result, Exception):
            continue
        file_path, success = result
        write_results[file_path] = success
    
    return write_results