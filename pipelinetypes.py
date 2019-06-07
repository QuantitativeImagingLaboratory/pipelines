from typing import NewType
import numpy as np

#Pipleine types
p_image = NewType("p_image", np.uint8)
p_int = NewType("p_int", int)
p_float = NewType("p_float", float)
p_message = NewType("p_message", str)
p_array = NewType("p_array", np.array)

#Keys in pipeline
KEY_SIGNAL = "signal"
KEY_MESSAGE = "message"

#Signals in pipeline
SIGNAL_END = "end"


PIPELINE_SIGNAL = "p_signal"
PIPELINE_END_STAGE_INIT = "p_end_init"
PIPELINE_END_STAGE_PIPELINE = "p_end_pipeline"
PIPELINE_END_STAGE_TERMINATE = "p_end_terminate"


PIPELINE_STAGE_INIT = "init"
PIPELINE_STAGE_PIPELINE = "pipeline"
PIPELINE_STAGE_TERMINATE = "terminate"


