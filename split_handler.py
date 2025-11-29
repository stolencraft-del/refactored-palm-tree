import os
import math
import subprocess
from logs import logging

async def split_video(filename, max_size_gb=1.9):
    """
    Split video file into parts if larger than max_size_gb
    Using 1.9GB to stay safely under Telegram's 2GB limit
    """
    try:
        # Get file size in bytes
        file_size = os.path.getsize(filename)
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024
        
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
            
            # FFmpeg command to split without re-encoding
            split_cmd = (
                f'ffmpeg -i "{filename}" '
                f'-ss {start_time} '
                f'-t {duration_per_part} '
                f'-c copy '
                f'-avoid_negative_ts 1 '
                f'-movflags +faststart '
                f'"{output_file}" -y'
            )
            
            logging.info(f"🔄 Splitting part {i+1}/{num_parts}: {output_file}")
            
            result = subprocess.run(split_cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                logging.error(f"❌ FFmpeg split failed for part {i+1}: {result.stderr}")
                continue
            
            if os.path.exists(output_file):
                part_size = os.path.getsize(output_file)
                part_size_gb = part_size / (1024**3)
                
                if part_size > 2 * 1024 * 1024 * 1024:
                    logging.warning(f"⚠️ Part {i+1} is {part_size_gb:.2f}GB (over 2GB limit!)")
                
                split_files.append(output_file)
                logging.info(f"✓ Created part {i+1}: {output_file} ({part_size_gb:.2f}GB)")
            else:
                logging.error(f"❌ Failed to create {output_file}")
        
        # Remove original file after successful split
        if len(split_files) == num_parts:
            os.remove(filename)
            logging.info(f"🗑 Removed original file: {filename}")
            return split_files
        else:
            logging.error(f"❌ Split incomplete! Expected {num_parts} parts, got {len(split_files)}")
            for f in split_files:
                if os.path.exists(f):
                    os.remove(f)
            return [filename]
            
    except Exception as e:
        logging.error(f"❌ Error in split_video: {str(e)}")
        return [filename]


async def generate_thumbnail(video_file, thumb_path=None):
    """
    Generate thumbnail from video if not provided
    """
    try:
        # If thumbnail provided and exists, use it
        if thumb_path and thumb_path != "/d" and os.path.exists(thumb_path):
            logging.info(f"✓ Using provided thumbnail: {thumb_path}")
            return thumb_path
        
        # Generate thumbnail from video
        output_thumb = f"{os.path.splitext(video_file)[0]}_thumb.jpg"
        
        # Extract frame at 5 seconds into video
        thumb_cmd = (
            f'ffmpeg -i "{video_file}" '
            f'-ss 00:00:05 '
            f'-vframes 1 '
            f'-vf "scale=320:180:force_original_aspect_ratio=increase,crop=320:180" '
            f'"{output_thumb}" -y'
        )
        
        logging.info(f"🖼️ Generating thumbnail: {output_thumb}")
        result = subprocess.run(thumb_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logging.error(f"❌ Thumbnail generation failed: {result.stderr}")
            return None
        
        if os.path.exists(output_thumb):
            logging.info(f"✓ Thumbnail generated: {output_thumb}")
            return output_thumb
        else:
            logging.error(f"❌ Thumbnail file not created")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error generating thumbnail: {str(e)}")
        return None
