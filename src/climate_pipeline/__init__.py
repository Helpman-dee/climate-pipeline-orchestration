"""Framework-neutral climate pipeline public API."""
from .acquisition import acquire_canonical_dataset
from .analysis import generate_figures, summarize_benchmarks
from .models import DatasetManifest, PartitionRequest, PartitionResult, QualityResult, VerificationResult
from .pipeline import run_canonical_partition, run_partition
from .verification import verify_run

__all__ = ["PartitionRequest", "PartitionResult", "DatasetManifest", "QualityResult", "VerificationResult", "acquire_canonical_dataset", "run_canonical_partition", "run_partition", "verify_run", "summarize_benchmarks", "generate_figures"]
