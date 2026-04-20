from pydantic import BaseModel, Field
from typing import Optional

class PipelineCfg(BaseModel):
	prefer_gpu: bool = True
	text_col: Optional[str] = None
	model_path: Optional[str] = "./models/Qwen2-500M-Instruct-Q8_0.gguf"
	llm_context: int = 512

class FilterCfg(BaseModel):
	min_length: int = 10
	max_length: int = 10000
	remove_empty: bool = True

class DedupCfg(BaseModel):
	exact: bool = True
	fuzzy: bool = True
	fuzzy_threshold: float = 0.85
	num_perm: int = 128

class BalanceCfg(BaseModel):
	strategy: str = "stratified"
	group_col: Optional[str] = None
	seed: int = 42

class OutputCfg(BaseModel):
	format: str = "parquet"
	report_path: str = "output/report.md"

class LoggingCfg(BaseModel):
	level: str = "INFO"
	json_format: bool = False

class PipelineConfig(BaseModel):
	pipeline: PipelineCfg
	filters: FilterCfg = Field(default_factory=FilterCfg)
	dedup: DedupCfg = Field(default_factory=DedupCfg)
	balance: BalanceCfg = Field(default_factory=BalanceCfg)
	output: OutputCfg = Field(default_factory=OutputCfg)
	logging: LoggingCfg = Field(default_factory=LoggingCfg)
