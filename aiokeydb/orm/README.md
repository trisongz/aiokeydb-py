# aiokeydb.orm module

Forked from [pydantic-aioredis](https://github.com/andrewthetechie/pydantic-aioredis)

A simple declarative ORM for KeyDB, using aiokeydb. Use your Pydantic
models like an ORM, storing data in KeyDB!

## Main Dependencies

- [Python +3.6](https://www.python.org)
- [aioredis 2.0](https://aioredis.readthedocs.io/en/latest/)
- [pydantic](https://github.com/samuelcolvin/pydantic/)

## Getting Started

### Examples

Examples are in the [examples/](./examples) directory of this repo.

### Installation

Install the package

    pip install aiokeydb

### Quick Usage

Import the `Registry`, the `KeyDBConfig` and the `Model` classes and use accordingly

```python
import asyncio
from datetime import date
from aiokeydb.orm import KeyDBConfig, Model, Registry

# Create models as you would create pydantic models i.e. using typings
class Book(Model):
    _primary_key_field: str = 'title'
    title: str
    author: str
    published_on: date
    in_stock: bool = True

# Do note that there is no concept of relationships here
class Library(Model):
    # the _primary_key_field is mandatory
    _primary_key_field: str = 'name'
    name: str
    address: str

# Create the store and register your models
store = Registry(name='some_name', redis_config=KeyDBConfig(db=5, host='localhost', port=6379), life_span_in_seconds=3600)
store.register_model(Book)
store.register_model(Library)

# Sample books. You can create as many as you wish anywhere in the code
books = [
    Book(title="Oliver Twist", author='Charles Dickens', published_on=date(year=1215, month=4, day=4),
        in_stock=False),
    Book(title="Great Expectations", author='Charles Dickens', published_on=date(year=1220, month=4, day=4)),
    Book(title="Jane Eyre", author='Charles Dickens', published_on=date(year=1225, month=6, day=4), in_stock=False),
    Book(title="Wuthering Heights", author='Jane Austen', published_on=date(year=1600, month=4, day=4)),
]
# Some library objects
libraries = [
    Library(name="The Grand Library", address="Kinogozi, Hoima, Uganda"),
    Library(name="Christian Library", address="Buhimba, Hoima, Uganda")
]

async def work_with_orm():
  # Insert them into redis
  await Book.insert(books)
  await Library.insert(libraries)

  # Select all books to view them. A list of Model instances will be returned
  all_books = await Book.select()
  print(all_books) # Will print [Book(title="Oliver Twist", author="Charles Dickens", published_on=date(year=1215, month=4, day=4), in_stock=False), Book(...]

  # Or select some of the books
  some_books = await Book.select(ids=["Oliver Twist", "Jane Eyre"])
  print(some_books) # Will return only those two books

  # Or select some of the columns. THIS RETURNS DICTIONARIES not MODEL Instances
  # The Dictionaries have values in string form so you might need to do some extra work
  books_with_few_fields = await Book.select(columns=["author", "in_stock"])
  print(books_with_few_fields) # Will print [{"author": "'Charles Dickens", "in_stock": "True"},...]

  # Update any book or library
  await Book.update(_id="Oliver Twist", data={"author": "John Doe"})

  # Delete any number of items
  await Library.delete(ids=["The Grand Library"])

# Now run these updates
loop = asyncio.get_event_loop()
loop.run_until_complete(work_with_orm())
```
