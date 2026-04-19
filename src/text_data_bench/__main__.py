# src/text_data_bench/__main__.py
from text_data_bench.cli import app

def main():
	try:
		app()
	except KeyboardInterrupt:
		import sys
		sys.exit(130)

if __name__ == "__main__":
	main()
