For ITD Change Data.json/ processType->MainCategory
Change convos->conversations in all aggregation project queries(data.json,line 127 batch_processing.py,line 83 preprocess.py)
                    --Done (Will be mapped dynamically based on categories)

##URGENT TODO
Modify logic to stop fetching agent/system message data multiple times for multiple threads --Done
##Workaround save the data locally in pickl files after the first time load and read from that file --Done

Change data.json file path whenever the folder structure/location changes -- Done

##Sample command for running preprocessing automation pipeline

YHRR/ZINC/ZSER) --batch-count 2(Insert Any digit) --output-level all/final

#### TODO ####
Change data.json and set isProcessed=False and update table names

python BatchProcessing.py BatchProcessing --batch-count=2 --local

python BatchProcessing.py BatchProcessing --batch-count=20 --limit=2000 --label='BJP' --local