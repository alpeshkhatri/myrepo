

1. Wrap a class,


2. Create multiple instances, and


3. Call a method on the wrapper, which then calls the same method on all wrapped instances.



This is a classic use case for the composite pattern or a proxy/aggregator object. Here's how you can implement it:


---

Example: MultiInstanceWrapper

class MultiInstanceWrapper:
    def __init__(self, cls, *args_list):
        self.instances = [cls(*args) for args in args_list]

    def __getattr__(self, name):
        def method(*args, **kwargs):
            results = []
            for instance in self.instances:
                attr = getattr(instance, name)
                if callable(attr):
                    results.append(attr(*args, **kwargs))
                else:
                    results.append(attr)
            return results
        return method


---

Usage

class MyClass:
    def __init__(self, value):
        self.value = value

    def say_hello(self):
        return f"Hello from {self.value}"

    def multiply(self, x):
        return self.value * x

# Create a wrapper around multiple instances
wrapper = MultiInstanceWrapper(MyClass, (1,), (2,), (3,))

print(wrapper.say_hello())       # ['Hello from 1', 'Hello from 2', 'Hello from 3']
print(wrapper.multiply(10))      # [10, 20, 30]


---

Explanation

MultiInstanceWrapper(cls, (1,), (2,)) creates instances: MyClass(1), MyClass(2).

__getattr__ dynamically dispatches method calls to all instances.

Returns a list of results from all instances.



---

Would you like it to also support setting/getting attributes across instances? Or some kind of aggregation strategy (like return the first result only, or log errors if one fails)?


To add logging and threading when calling methods on all instances, we’ll enhance the wrapper so that:

Each method call on an instance runs in a separate thread

We log the method name, arguments, and results (or exceptions)

Optionally wait for all threads to complete and return results



---

Advanced MultiInstanceWrapper with Logging + Threading

import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

class MultiInstanceWrapper:
    def __init__(self, cls, *args_list):
        self.instances = [cls(*args) for args in args_list]

    def __getattr__(self, name):
        def method_or_getter(*args, **kwargs):
            threads = []
            results = [None] * len(self.instances)

            def worker(index, instance):
                try:
                    attr = getattr(instance, name)
                    if callable(attr):
                        logging.info(f"Calling {name} on instance {index} with args={args}, kwargs={kwargs}")
                        result = attr(*args, **kwargs)
                        logging.info(f"Instance {index} - {name} returned: {result}")
                        results[index] = result
                    else:
                        logging.info(f"Getting attribute {name} from instance {index}")
                        results[index] = attr
                except Exception as e:
                    logging.error(f"Error on instance {index} - {name}: {e}")
                    results[index] = e

            for i, instance in enumerate(self.instances):
                thread = threading.Thread(target=worker, args=(i, instance))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

            return results
        return method_or_getter

    def __setattr__(self, name, value):
        if name == 'instances':
            super().__setattr__(name, value)
        else:
            for i, instance in enumerate(self.instances):
                try:
                    setattr(instance, name, value)
                    logging.info(f"Set {name}={value} on instance {i}")
                except Exception as e:
                    logging.error(f"Error setting {name} on instance {i}: {e}")


---

Example Usage

import time

class MyClass:
    def __init__(self, value):
        self.value = value

    def work(self, delay):
        time.sleep(delay)
        return f"Done {self.value} after {delay}s"

# Create wrapped instances
wrapper = MultiInstanceWrapper(MyClass, (1,), (2,), (3,))

# Call threaded method
print(wrapper.work(2))


---

Output Example

[INFO] Calling work on instance 0 with args=(2,), kwargs={}
[INFO] Calling work on instance 1 with args=(2,), kwargs={}
[INFO] Calling work on instance 2 with args=(2,), kwargs={}
[INFO] Instance 0 - work returned: Done 1 after 2s
[INFO] Instance 1 - work returned: Done 2 after 2s
[INFO] Instance 2 - work returned: Done 3 after 2s
['Done 1 after 2s', 'Done 2 after 2s', 'Done 3 after 2s']


---

Would you like support for:

Timeouts for each thread?

Returning only successful results?

Asyncio version (for async methods)?



