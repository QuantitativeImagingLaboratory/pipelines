from typing import NewType, TypeVar
import numpy as np

#Pipleine types

p_message = NewType("p_message", str)
p_number = NewType("p_number", p_message)
p_int = NewType("p_int", p_number)
p_float = NewType("p_float", p_number)

p_array = NewType("p_array", np.array)
p_image = NewType("p_image", p_array)

p_list = NewType("p_list", p_array)

p_list_of_bb = NewType("p_list_of_bb", p_list)


pipeline_array = ["array", "image"]
pipeline_number = ["message", "number", "int", "float"]
pipeline_list = ["list", "list_of_bb"]
pipeline_types = [pipeline_array, pipeline_number, pipeline_list]



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


