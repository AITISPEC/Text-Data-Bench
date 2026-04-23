import logging


def setup_logger(name: str = "text_data_bench", level: str = "INFO", json_format: bool = False) -> logging.Logger:
	logger = logging.getLogger(name)
	logger.setLevel(getattr(logging, level.upper(), logging.INFO))

	# Удаляем существующие хэндлеры, чтобы применить новый формат
	if logger.hasHandlers():
		logger.handlers.clear()

	handler = logging.StreamHandler()
	if json_format:
		formatter = logging.Formatter(
			'{"time":"%(asctime)s","logger":"%(name)s","level":"%(levelname)s","message":"%(message)s"}'
		)
	else:
		formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	handler.setFormatter(formatter)
	logger.addHandler(handler)

	return logger
