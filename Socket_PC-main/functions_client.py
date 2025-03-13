import os
import time
import asyncio
import logging

# Configure logging to output to console only.
logger = logging.getLogger("AsyncUDPReceiver")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class UDPReceiverProtocol(asyncio.DatagramProtocol):
    def __init__(self, save_folder, frame_timeout=5.0):
        """
        :param save_folder: Folder where complete PLY files will be saved.
        :param frame_timeout: Timeout in seconds for discarding incomplete frames.
        """
        self.save_folder = save_folder
        self.frame_timeout = frame_timeout
        self.reassembly_dict = {}  # { frame_counter: { "total_chunks": int, "chunks": list, "received_count": int, "last_update": float } }
        self.loop = asyncio.get_running_loop()
        self.cleanup_task = self.loop.create_task(self.cleanup_loop())

    def datagram_received(self, data, addr):
        # Expecting at least 8 bytes for custom header.
        if len(data) < 8:
            logger.warning(f"Received packet too small ({len(data)} bytes) from {addr}. Ignoring.")
            return

        # Parse custom header:
        # - First 4 bytes: frame counter (big-endian unsigned int)
        # - Next 2 bytes: total chunks (big-endian unsigned int)
        # - Next 2 bytes: chunk index (big-endian unsigned int)
        frame_counter = int.from_bytes(data[0:4], "big", signed=False)
        total_chunks = int.from_bytes(data[4:6], "big", signed=False)
        chunk_idx = int.from_bytes(data[6:8], "big", signed=False)
        chunk_data = data[8:]

        if total_chunks <= 0 or chunk_idx >= total_chunks:
            logger.warning(f"Invalid header from {addr} (chunk {chunk_idx} of {total_chunks}). Skipping packet.")
            return

        now = self.loop.time()

        # Initialize reassembly for this frame if not present.
        if frame_counter not in self.reassembly_dict:
            self.reassembly_dict[frame_counter] = {
                "total_chunks": total_chunks,
                "chunks": [None] * total_chunks,
                "received_count": 0,
                "last_update": now,
            }

        frame_info = self.reassembly_dict[frame_counter]

        # Update total_chunks if needed (rare case).
        if frame_info["total_chunks"] != total_chunks:
            logger.warning(f"Frame {frame_counter}: total_chunks mismatch. Updating.")
            frame_info["total_chunks"] = total_chunks
            frame_info["chunks"] = [None] * total_chunks
            frame_info["received_count"] = 0

        # Save the chunk if not already present.
        if frame_info["chunks"][chunk_idx] is None:
            frame_info["chunks"][chunk_idx] = chunk_data
            frame_info["received_count"] += 1
            frame_info["last_update"] = now

        # Check if frame is complete.
        if frame_info["received_count"] == frame_info["total_chunks"]:
            complete_frame = b"".join(frame_info["chunks"])
            # Schedule saving the complete frame asynchronously.
            self.loop.create_task(self.save_frame(frame_counter, complete_frame))
            # Remove frame from reassembly dictionary.
            del self.reassembly_dict[frame_counter]

    async def save_frame(self, frame_counter, data):
        """Save the complete PLY file to the designated folder."""
        filename = f"frame_{frame_counter}_{int(time.time())}.ply"
        filepath = os.path.join(self.save_folder, filename)
        try:
            with open(filepath, "wb") as f:
                f.write(data)
            logger.info(f"Saved complete frame #{frame_counter} to '{filepath}'.")
        except Exception as e:
            logger.error(f"Error saving frame #{frame_counter} to '{filepath}': {e}")

    async def cleanup_loop(self):
        """Periodically remove incomplete frames that have timed out."""
        while True:
            now = self.loop.time()
            to_remove = []
            for fc, info in self.reassembly_dict.items():
                if now - info["last_update"] > self.frame_timeout:
                    logger.warning(f"Discarding incomplete frame #{fc}: received {info['received_count']}/{info['total_chunks']} chunks.")
                    to_remove.append(fc)
            for fc in to_remove:
                del self.reassembly_dict[fc]
            await asyncio.sleep(1)

    def connection_lost(self, exc):
        if exc:
            logger.error(f"Connection lost: {exc}")
        if self.cleanup_task:
            self.cleanup_task.cancel()
