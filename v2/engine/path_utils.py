# file: v2/engine/path_utils.py

from pathlib import Path


def resolve_path(ctx, path_str: str) -> Path:
    """
    상대경로 → work_dir 기준 변환
    절대경로 → 그대로 반환
    """
    p = Path(path_str)

    if p.is_absolute():
        return p

    return ctx.work_dir / p
