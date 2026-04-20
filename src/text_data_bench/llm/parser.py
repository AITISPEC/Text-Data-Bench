import json
import re
from rich.console import Console
from .engine import get_engine

console = Console()

def parse_structured(text: str, model_path: str, ctx: int, prefer_gpu: bool) -> list[dict]:
	llm = get_engine(model_path, ctx, prefer_gpu)
	prompt = (
		"Extract structured tabular data from the raw text below.\n"
		"RULES: 1. Return ONLY a valid JSON array. 2. No markdown/explanations. 3. If unclear, return []\n\n"
		f"Raw text:\n{text[:6000]}\n\nJSON:"
	)
	for _ in range(2):
		res = llm(prompt, max_tokens=1024, temperature=0.05, stop=["```", "\n\n"])
		out = res["choices"][0]["text"].strip()
		match = re.search(r"\[.*\]", out, re.DOTALL)
		if match:
			try:
				data = json.loads(match.group(0))
				if isinstance(data, list):
					return data
			except json.JSONDecodeError:
				pass
		prompt += "\nStrict JSON array only."
	console.print("[yellow]⚠ LLM fallback failed. Returning empty list.[/yellow]")
	return []
