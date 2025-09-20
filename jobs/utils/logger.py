import logging
import os

def get_logger(name="Pipeline", log_dir="logs", level=logging.INFO):
    
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, f"{name.lower()}.log")

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),   # log to file
            logging.StreamHandler()          # log to console
        ]
    )

    return logging.getLogger(name)