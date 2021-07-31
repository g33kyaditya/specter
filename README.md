# specter

```
python3.8 -m pip install -r requirements.txt
python3.8 coin.py --interval 60
```
In a different window

```
celery -A worker worker --loglevel=info
```


## Logic

```
coin.py <-- Producer of coins, queries API and pushes each cryptocoin to a task queue

worker.py <--- Pops each cryptocoin off of the task queue and saves details in a mongodb
```

