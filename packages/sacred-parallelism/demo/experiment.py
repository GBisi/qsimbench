import os

from sacred import Experiment
from sacred.observers import MongoObserver
import random
import time
from dotenv import load_dotenv
load_dotenv(dotenv_path="./src/.env") 

mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
mongo_pass = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
mongo_db = os.getenv("MONGO_DATABASE")

# Create a Sacred experiment
ex = Experiment("grid_search_example")

ex.observers.append(MongoObserver.create(
    url=f'mongodb://{mongo_user}:{mongo_pass}@localhost:27017/',
    db_name=mongo_db
))
# Define config space (these will be used in the grid)
@ex.config
def config():
    learning_rate = 0.001  # will be overridden by grid
    batch_size = 32
    dropout = 0.1
    n_cores = 4  # Number of CPU cores to use for parallelism

@ex.automain
def run(learning_rate, batch_size, dropout, n_cores):

    os.environ["OMP_NUM_THREADS"] = str(n_cores)
    os.environ["OPENBLAS_NUM_THREADS"] = str(n_cores)
    os.environ["MKL_NUM_THREADS"] = str(n_cores)
    os.environ["VECLIB_MAXIMUM_THREADS"] = str(n_cores)
    os.environ["NUMEXPR_NUM_THREADS"] = str(n_cores)
    os.environ["NUMBA_NUM_THREADS"] = str(n_cores)
    
    print(f"[INFO] Using up to {n_cores} CPU cores")
    print(f"Running with learning_rate={learning_rate}, batch_size={batch_size}, dropout={dropout}")
    
    # Simulate training process
    time.sleep(random.uniform(0.5, 2.0))  # fake training time
    accuracy = 1.0 - (learning_rate * 10) - (dropout * 2) + (batch_size / 1000.0)
    
    print(f"Simulated accuracy: {accuracy:.4f}")
    return accuracy