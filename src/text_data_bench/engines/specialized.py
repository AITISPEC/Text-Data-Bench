# src/text_data_bench/engines/specialized.py
"""Движки для специализированных форматов: HDF5, Arrow, Pickle, HF Datasets"""
import polars as pl
from pathlib import Path
from .base import _standardize


def load_hdf5(p: Path) -> pl.DataFrame:
    """Загрузка HDF5 файлов"""
    try:
        import h5py
    except ImportError:
        raise ImportError("[HDF5] Install h5py: pip install h5py")

    with h5py.File(p, "r") as f:
        datasets = []
        def _collect_datasets(name, obj):
            if isinstance(obj, h5py.Dataset):
                datasets.append((name, obj))
        f.visititems(_collect_datasets)

        for name, ds in datasets:
            try:
                if ds.dtype.kind in ("U", "S", "O"):
                    data = ds[:].astype(str)
                    df = pl.DataFrame({"text": data})
                    return _standardize(df, f"HDF5({name})")
                elif ds.dtype.kind in ("i", "f"):
                    arr = ds[:]
                    if arr.ndim == 1:
                        df = pl.DataFrame({"value": arr})
                    elif arr.ndim == 2:
                        col_names = ds.attrs.get("column_names", [f"col_{i}" for i in range(arr.shape[1])])
                        df = pl.DataFrame(arr.tolist(), schema=col_names, orient="row")
                    else:
                        df = pl.DataFrame({"value": arr.flatten()})
                    return _standardize(df, f"HDF5({name})")
            except Exception:
                continue

    raise ValueError("[HDF5] No suitable datasets found.")


def load_arrow(p: Path) -> pl.DataFrame:
    """Загрузка Apache Arrow файлов"""
    try:
        return _standardize(pl.read_ipc(p), "Arrow")
    except pl.exceptions.ComputeError:
        try:
            import pyarrow.ipc as ipc
            with ipc.open_file(p) as f:
                table = f.read_all()
            return _standardize(pl.from_arrow(table), "Arrow")
        except Exception:
            raise


def load_pickle(p: Path) -> pl.DataFrame:
    """Загрузка Pickle файлов (pandas, numpy, списки, словари, объекты)"""
    import pickle
    
    with open(p, "rb") as f:
        obj = pickle.load(f)

    # pandas DataFrame
    if hasattr(obj, 'to_dict') and hasattr(obj, 'columns'):
        try:
            df = pl.DataFrame(obj)
            return _standardize(df, "Pickle(Pandas)")
        except Exception:
            try:
                df = pl.DataFrame(obj.to_dict())
                return _standardize(df, "Pickle(Pandas)")
            except Exception:
                pass

    # numpy array
    if hasattr(obj, 'shape') and hasattr(obj, 'tolist'):
        if obj.ndim == 1:
            df = pl.DataFrame({"value": obj.tolist()})
        elif obj.ndim == 2:
            columns = [f"col_{i}" for i in range(obj.shape[1])]
            df = pl.DataFrame(obj.tolist(), schema=columns, orient="row")
        else:
            df = pl.DataFrame({"value": obj.flatten().tolist()})
        return _standardize(df, "Pickle(Numpy)")

    # список словарей
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return _standardize(pl.DataFrame(obj), "Pickle")
        return _standardize(pl.DataFrame({"text": [str(x) for x in obj]}), "Pickle")

    # словарь
    if isinstance(obj, dict):
        return _standardize(pl.DataFrame([obj]), "Pickle")

    # tuple
    if isinstance(obj, tuple):
        return _standardize(pl.DataFrame({"text": [str(x) for x in obj]}), "Pickle")

    # объект с __dict__
    if hasattr(obj, '__dict__'):
        return _standardize(pl.DataFrame([obj.__dict__]), "Pickle")

    raise ValueError("[Pickle] Cannot convert object to DataFrame.")


def load_hf_dataset(p: Path) -> pl.DataFrame:
    """Загрузка Hugging Face Datasets"""
    try:
        from datasets import load_from_disk
    except ImportError:
        raise ImportError("[HF] Install datasets: pip install datasets")
    
    ds = load_from_disk(p)
    return _standardize(pl.from_pandas(ds.to_pandas()), "HF Dataset")
