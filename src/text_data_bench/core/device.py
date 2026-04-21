import subprocess
import logging

logger = logging.getLogger(__name__)

def detect_gpu() -> bool:
	"""Detect NVIDIA GPU availability via nvidia-smi."""
	try:
		subprocess.check_output(["nvidia-smi"], stderr=subprocess.DEVNULL)
		logger.info("NVIDIA GPU detected via nvidia-smi")
		return True
	except Exception:
		logger.debug("No NVIDIA GPU detected")
		return False
