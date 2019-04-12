cmd for running luigi pipeline:

python BatchProcessing.py BatchProcessing --batch-count=1 --limit=1 --label='BJP' --local

Repo has 4 sections:
  notebooks : EDA, Twitter crawler,Model training files
  pipeline: Data processing pipelines using Luigi
  data.json: Configuration values for our luigi pipeline
  
  We have used Tweepy live streaming API for getting real time tweets. However, one obvious drawback is we can't have retweet,favourite counts since those fields always remain default 0 because of streaming !
