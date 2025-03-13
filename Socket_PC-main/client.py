import os
import asyncio
import logging
from functions_client import UDPReceiverProtocol

# Configure logging for main.
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

async def main():
    # Define the local IP and port to bind for receiving UDP packets.
    listen_ip = "0.0.0.0"
    listen_port = 5005

    # Define the folder where received PLY files will be saved.
    save_folder = "received_folder"
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
        logging.info(f"Created folder '{save_folder}' for saving received files.")

    loop = asyncio.get_running_loop()
    # Create a UDP endpoint and bind it to the local address.
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPReceiverProtocol(save_folder, frame_timeout=5.0),
        local_addr=(listen_ip, listen_port)
    )
    logging.info(f"UDP client listening on {listen_ip}:{listen_port}.")

    try:
        # Run indefinitely.
        await asyncio.sleep(3600 * 24)
    except asyncio.CancelledError:
        pass
    finally:
        transport.close()

if __name__ == "__main__":
    asyncio.run(main())
