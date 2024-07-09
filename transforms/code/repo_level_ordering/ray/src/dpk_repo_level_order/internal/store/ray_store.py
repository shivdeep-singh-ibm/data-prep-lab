import random

import ray
from data_processing_ray.runtime.ray import RayUtils
from ray.util import ActorPool


@ray.remote(scheduling_strategy="SPREAD")
class KeyedValueListActor:
    """
    This class uses ray actor to serve as a dictionary of type `dict[str, List[str]]`
    and provides methods to add/get items to/from it.

    """

    dict_: dict

    def __init__(self, a):
        self.dict_ = {}

    def put(self, key, value):
        try:
            self.dict_[key].append(value)
        except KeyError:
            self.dict_[key] = [value]
        except AttributeError:
            self.dict_ = {}
            self.dict_[key] = [value]

    def get(self, key):
        try:
            return self.dict_.get(key)
        except:
            return None

    def keys(self, x):
        try:
            return self.dict_.keys()
        except:
            return []


class KeyedValueListActorPool:
    """
    This class uses ray actor pool to serve as a dictionary of type `dict[str, List[str]]`
    and provides methods to add/get items to/from it.

    """

    def __init__(self, pool):
        self.pool = ActorPool(pool)
        self.processors = pool
        # self.pool
        self.n_workers = 2

    def put(self, key: str, value: str):
        # may randomly append to an actor
        dict_ = random.choice(self.processors)
        return ray.get(dict_.put.remote(key, value))

    def items(self):
        r = self.pool.map(fn=lambda a, v: a.keys.remote(v), values=self.n_workers * [1])
        result = []
        while self.pool.has_next():
            try:
                data = self.pool.get_next_unordered()
                if data != None:
                    result = result + [k for k in data]
            except Exception as e:
                print(
                    "Error: %s. Ignoring the error",
                    e,
                )
                continue
            except Exception as e:
                print("Error: %s", e)
                continue
        return list(set(result))

    def get(self, key):
        self.pool.map_unordered(fn=lambda a, v: a.get.remote(v), values=self.n_workers * [key])

        result = []
        while self.pool.has_next():
            try:
                data = self.pool.get_next_unordered()
                if data != None:
                    result = result + data
            except Exception as e:
                self.logger.error(
                    "Error: %s. Ignoring the error",
                    e,
                )
                continue
            except Exception as e:
                self.logger.error("Error: %s", e)
                continue

        return list(set(result))


def create_pool(cpus: float, actors: int):
    """
    Creates a pool of Ray actors to serve as proxy for
    ray based store.
    """
    processors = RayUtils.create_actors(
        clazz=KeyedValueListActor,
        params=None,
        actor_options={"num_cpus": cpus},
        n_actors=actors,
    )
    return processors
    pool = ActorPool(processors)
    return pool


def main():
    import ray

    ray.init(ignore_reinit_error=True)

    rc = ASClient(AS)
    rc.put("hello", "w")
    rc.put("hello", "wrld")
    rc.put("hello", "word")
    rc.put("hello", "orld")
    rc.put("hello", "way")
    rc.put("hello", "way")
    rc.put("ello", "orld")
    rc.put("ello", "way")
    rc.put("ello", "way")

    print(rc.get("hello"))
    print(rc.get("ello"))

    for k in rc.items():
        print(k, rc.get(k))

    create_pool(0.5, 2)


# print (AS.keys.remote('e'))
# main()
