import os
import math
import subprocess
from logs import logging

async def split_video(filename, max_size_gb=2):
    """
    Split video file into parts if larger than max_size_gb
    
    Args:
        filename (str): Path to the video file
        max_size_gb (int): Maximum size per part in GB (default 2GB)
    
    Returns:
        list: List of split file paths or [original_file] if no split needed
    """
    try:
        # Get file size in bytes
        file_size = os.path.getsize(filename)
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024  # Convert GB to bytes
        
        # If file is smaller than max size, return original
        if file_size <= max_size_bytes:
            logging.info(f"✓ File {filename} is {file_size / (1024**3):.2f}GB, no split needed")
            return [filename]
        
        logging.info(f"⚠ File {filename} is {file_size / (1024**3):.2f}GB, splitting into {max_size_gb}GB parts")
        
        # Calculate number of parts needed
        num_parts = math.ceil(file_size / max_size_bytes)
        logging.info(f"📦 Will create {num_parts} parts")
        
        # Get video duration using ffprobe
        probe_cmd = f'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{filename}"'
        result = subprocess.run(probe_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"❌ FFprobe failed: {result.stderr}")
            return [filename]
        
        total_duration = float(result.stdout.strip())
        logging.info(f"⏱ Total video duration: {total_duration:.2f} seconds")
        
        # Calculate duration per part
        duration_per_part = total_duration / num_parts
        
        split_files = []
        base_name = os.path.splitext(filename)[0]
        ext = os.path.splitext(filename)[1]
        
        # Split video into parts
        for i in range(num_parts):
            start_time = i * duration_per_part
            output_file = f"{base_name}_part{i+1}{ext}"
            
            # FFmpeg command to split without re-encoding (fast)
            split_cmd = (
                f'ffmpeg -i "{filename}" '
                f'-ss {start_time} '
                f'-t {duration_per_part} '
                f'-c copy '  # Copy codec (no re-encoding)
                f'-avoid_negative_ts 1 '  # Fix timestamp issues
                f'"{output_file}" -y'
            )
            
            logging.info(f"🔄 Splitting part {i+1}/{num_parts}: {output_file}")
            
            # Execute split command
            result = subprocess.run(split_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                logging.error(f"❌ FFmpeg split failed for part {i+1}: {result.stderr}")
                continue
            
            # Verify file was created
            if os.path.exists(output_file):
                part_size = os.path.getsize(output_file)
                split_files.append(output_file)
                logging.info(f"✓ Created part {i+1}: {output_file} ({part_size / (1024**3):.2f}GB)")
            else:
                logging.error(f"❌ Failed to create {output_file}")
        
        # Remove original file after successful split
        if len(split_files) == num_parts:
            os.remove(filename)
            logging.info(f"🗑 Removed original file: {filename}")
            return split_files
        else:
            logging.error(f"❌ Split incomplete! Expected {num_parts} parts, got {len(split_files)}")
            # Clean up partial splits
            for f in split_files:
                if os.path.exists(f):
                    os.remove(f)
            return [filename]
            
    except Exception as e:
        logging.error(f"❌ Error in split_video: {str(e)}")
        return [filename]
