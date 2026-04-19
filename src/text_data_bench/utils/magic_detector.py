# src/text_data_bench/utils/magic_detector.py
from pathlib import Path

MAGIC_SIGNATURES = {
	b'PK\x03\x04': ('.zip', False),
	b'PAR1': ('.parquet', False),
	b'\x89HDF': ('.h5', False),
	b'\x93NUMPY': ('.npy', False),
	b'PICKLE': ('.pkl', False),
	b'ARROW1': ('.arrow', False),
	b'\xff\xd8\xff': ('.jpg', False),
	b'{': ('.json', True),
	b'<?xml': ('.xml', True),
}

def detect_format(filepath: str) -> tuple[str, bool]:
	p = Path(filepath)
	ext = p.suffix.lower()
	try:
		with open(p, 'rb') as f:
			header = f.read(16)
	except Exception:
		return ext, True

	for sig, (det_ext, is_text) in MAGIC_SIGNATURES.items():
		if header.startswith(sig):
			return det_ext, is_text
	return ext, True
