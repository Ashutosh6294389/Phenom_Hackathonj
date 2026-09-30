"""Long-running consumer process. The DB teammate swaps InMemoryDatabase for their writer."""
import logging, pathlib, signal, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from career_queue.config import load_config
from career_queue.consumer import EventConsumer
from career_queue.db import InMemoryDatabase   # TODO: replace with the Cassandra writer (DatabaseWriter)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-9s %(message)s")

if __name__ == "__main__":
    consumer = EventConsumer(load_config(), InMemoryDatabase())
    signal.signal(signal.SIGTERM, lambda *_: consumer.stop())
    signal.signal(signal.SIGINT, lambda *_: consumer.stop())
    consumer.run()
