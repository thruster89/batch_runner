# file: v2/engine/stage_registry.py

from v2.stages import export_stage
from v2.stages import load_stage
from v2.stages import postwork_stage
from v2.stages import report_stage

STAGE_REGISTRY = {
    "export": export_stage.run,
    "load_local": load_stage.run,
    "postwork": postwork_stage.run,
    "report": report_stage.run,
}
