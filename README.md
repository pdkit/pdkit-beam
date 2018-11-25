# beam_pdkit

![Beam Pdkit](https://github.com/uh-joan/beam_pdkit/blob/master/beam_pdkit.png)

## Install

`pip install -r requirements.txt`

## Run

`python welch_beam_pdkit.py`


## Instructions

It takes as input acceleration data with the id at each line:

`timestamp,x,y,z,user_id`

As an example take the file: `tremor_data_with_user.csv`

In this example the method used from `pdkit` is `welch` but any method will work as it is now.

## Saving results to file

The results are stored in file. Just pass `--output="filename.csv` to define the file name. By default now is
`output.csv`

```
2678,10.540682,02:23:00,02:23:30
2678,10.656897,02:22:40,02:23:10
2678,11.103284,02:22:50,02:23:20
2678,9.163072,02:23:10,02:23:40
2678,3.228065,02:23:20,02:23:50
2678,4.506551,02:22:30,02:23:00
2458,62.922552,12:21:40,12:22:10
2458,67.516632,12:21:20,12:21:50
2458,25.660236,12:22:00,12:22:30
2458,4.092081,12:22:10,12:22:40
2458,51.772989,12:21:50,12:22:20
2458,81.424755,12:21:30,12:22:00
3037,24.125740,00:39:30,00:40:00
3037,27.619027,00:39:20,00:39:50
3037,107.838542,00:39:50,00:40:20
3037,165.204399,00:40:00,00:40:30
3037,55.552502,00:39:40,00:40:10
3037,397.174782,00:40:10,00:40:40
```

where
`id, welch_value, start, end`

## Streaming data

This example shows how to use the google cloud streaming. Pass the following parameters:

`--input_topic` is the `Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>"`
`--output_topic` is the `Output PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>"`

You'll need to register to [google cloud services](https://cloud.google.com/pubsub/?authuser=1). Install instructions
for [google cloud](https://cloud.google.com/sdk/docs/). Don't forget to create a project and a topic using for example
the [command line](https://cloud.google.com/pubsub/docs/quickstart-cli).

`Tip`: If you want to stream a file line by line to `gcloud` this command might come in handy:
`cat filename.csv | while read line; do gcloud pubsub topics publish topicName --message "$line"; done`