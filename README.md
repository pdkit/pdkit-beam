# beam_pdkit

## Install

`pip install -r requirements.txt`

## Run

`python window_beam_kit.py`


## Instructions

It takes as input acceleration data with the id at each line:

`timestamp,x,y,z,user_id`

As an example take the file: `tremor_data_with_user.csv`

In this example the method used from `pdkit` is `number_peaks` but any method will work as it is now. Just change
[line](https://github.com/uh-joan/beam_pdkit/blob/master/window_beam_pdkit.py#L131)

