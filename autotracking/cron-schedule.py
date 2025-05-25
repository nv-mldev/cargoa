import time
import logging
import requests
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# MongoDB setup (modify the connection string as needed)
client = MongoClient("mongodb://localhost:27017/")
db = client['shipment_tracking']
collection = db['tracking_data']

def get_valid_reference_id():
    """
    Polls the Vizion API until a valid reference ID is obtained.
    """
    api_url = "https://api.vizion.com/getReferenceId"  # Replace with the actual Vizion API endpoint
    while True:
        try:
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                reference_id = data.get("reference_id")
                if reference_id:
                    logging.info(f"Obtained valid Reference ID: {reference_id}")
                    return reference_id
                else:
                    logging.warning("Reference ID not returned. Retrying in 1 minute...")
            else:
                logging.error(f"API error, status code: {response.status_code}. Retrying in 1 minute...")
        except Exception as e:
            logging.error(f"Exception while fetching Reference ID: {e}. Retrying in 1 minute...")
        time.sleep(60)

def call_vizion_api_for_reference():
    """
    Continuously polls for a valid Reference ID. Once obtained, stores it in MongoDB
    and starts the tracking data polling process.
    """
    reference_id = get_valid_reference_id()

    # Store the reference ID and metadata in MongoDB
    collection.insert_one({
        "reference_id": reference_id,
        "timestamp": time.time(),
        "tracking_info": None
    })

    # Start polling for tracking data
    poll_vizion_api(reference_id,livex)

def poll_vizion_api(reference_id):
    """
    Polls the Vizion API every 1 minute for tracking data until valid tracking info
    is received or the maximum number of retries is reached.
    """
    poll_url = f"https://api.vizion.com/getTrackingData/{reference_id}"  # Replace with actual endpoint
    max_retries = 10
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(poll_url)
            if response.status_code == 200:
                data = response.json()
                tracking_info = data.get("tracking_info")
                if tracking_info:
                    logging.info(f"Received valid tracking data for Reference ID {reference_id}: {tracking_info}")

                    # Update the MongoDB record with the tracking information
                    collection.update_one(
                        {"reference_id": reference_id},
                        {"$set": {"tracking_info": tracking_info, "updated_at": time.time()}}
                    )
                    return
                else:
                    logging.info("Empty tracking data received, retrying in 1 minute...")
            else:
                logging.error(f"Failed to poll tracking data. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception during tracking data polling: {e}")

        retries += 1
        time.sleep(60)

    logging.warning(f"Max retries reached for Reference ID {reference_id} without receiving valid tracking data.")

if __name__ == "__main__":
    # Create a BackgroundScheduler instance
    scheduler = BackgroundScheduler()

    # Schedule the job to run every 12 hours (e.g., at 00:00 and 12:00)
    scheduler.add_job(call_vizion_api_for_reference, 'cron', hour='0,12', minute=0)

    logging.info("Starting Background Scheduler. The Vizion API tracking job is scheduled every 12 hours.")
    scheduler.start()

    try:
        # Keep the main thread alive so the background scheduler can run continuously
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Exit signal received. Shutting down the scheduler.")
        scheduler.shutdown()
