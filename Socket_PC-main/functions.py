import os
import glob
import math
import asyncio
import time
import logging

# Configure logging to output to the console only.
logger = logging.getLogger("AsyncUDPSender")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class UDPSenderProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        logger.info("UDP connection established.")

    def error_received(self, exc):
        logger.error("Error received: %s", exc)

async def send_file(filepath, transport, target_addr, frame_counter, chunk_size=8000, fps=30):
    """
    Reads a PLY file and sends it via UDP in chunks with a custom header.
    
    Custom header layout per UDP packet:
      - 4 bytes: frame counter (big-endian unsigned int)
      - 2 bytes: total number of chunks (big-endian unsigned int)
      - 2 bytes: chunk index (big-endian unsigned int)
    
    :param filepath:   Path to the .ply file.
    :param transport:  An open asyncio UDP transport.
    :param target_addr: Tuple (IP, port) for the destination.
    :param frame_counter: Current frame counter (will be updated).
    :param chunk_size:  Maximum payload size per UDP packet (excluding header).
    :param fps:         Throttle sending between chunks (frames per second).
    :return: Updated frame counter.
    """
    try:
        with open(filepath, "rb") as f:
            file_data = f.read()
    except Exception as e:
        logger.error(f"Error reading file '{filepath}': {e}")
        return frame_counter

    frame_size = len(file_data)
    logger.info(f"Sending file '{os.path.basename(filepath)}' ({frame_size} bytes) as frame #{frame_counter}.")

    frame_counter_bytes = frame_counter.to_bytes(4, 'big', signed=False)
    total_chunks = math.ceil(frame_size / chunk_size)
    total_chunks_bytes = total_chunks.to_bytes(2, 'big', signed=False)
    delay = 1.0 / fps

    for chunk_idx in range(total_chunks):
        start = chunk_idx * chunk_size
        end = min(start + chunk_size, frame_size)
        chunk_data = file_data[start:end]
        chunk_idx_bytes = chunk_idx.to_bytes(2, 'big', signed=False)
        payload = frame_counter_bytes + total_chunks_bytes + chunk_idx_bytes + chunk_data
        transport.sendto(payload, target_addr)
        await asyncio.sleep(delay)
        
    logger.info(f"Finished sending file '{os.path.basename(filepath)}' as frame #{frame_counter}.")
    return (frame_counter + 1) % (2**32)

async def monitor_folder_and_send(send_folder, transport, target_addr, chunk_size=8000, fps=30, poll_interval=1.0, clear_interval=120):
    """
    Monitors a folder for new .ply files and sends each new file via UDP.
    
    The function tracks sent files in a set to avoid re-sending. To prevent unbounded
    memory growth during long-term operation, the sent files set is cleared every
    `clear_interval` seconds.
    
    :param send_folder:  Folder to monitor for .ply files.
    :param transport:    An open asyncio UDP transport.
    :param target_addr:  Tuple (IP, port) for the destination.
    :param chunk_size:   Maximum payload size per UDP packet (excluding header).
    :param fps:          Frames per second (controls inter-chunk delay).
    :param poll_interval:Time interval (seconds) between folder polls.
    :param clear_interval:Time interval (seconds) to clear the sent files set.
    """
    sent_files = set()
    frame_counter = 0
    last_clear_time = time.time()
    logger.info(f"Started monitoring folder '{send_folder}' for new .ply files...")

    while True:
        now = time.time()
        if now - last_clear_time >= clear_interval:
            sent_files.clear()
            last_clear_time = now
            logger.info("Cleared sent files cache.")
        
        ply_files = sorted(glob.glob(os.path.join(send_folder, "*.ply")))
        for filepath in ply_files:
            if filepath not in sent_files:
                logger.info(f"New file detected: '{filepath}'")
                frame_counter = await send_file(filepath, transport, target_addr, frame_counter, chunk_size, fps)
                sent_files.add(filepath)
        await asyncio.sleep(poll_interval)
