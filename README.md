# pymy -- a Python MySQL connector


## What is it
**pymy** is a Pythonic wrapper for connecting and sending queries to a MariaDB or MySQL database. Once it is **not** an ORM nor a SQL parser, you must enjoy SQL as much as you enjoy Python itself! And neither it adheres to any PEP database connection specification, but -- it is simple, neat and useful, doing a good job most of the times.

You can query data as simply as 
```python
import pymy
db = pymy.Database('test_db', user='myuser', password='pwd1234')
res = db.query('select * from Table1')
for row in res:
    print 'id = {}, name = {}'.format(row['id'], row['name'])
```

or execute a SQL command...
```python
import pymy
db = pymy.Database('test_db', user='myuser', password='pwd1234')
res = db.execute('insert into Table1 (phone) values ("551112345678")')
print '{} rows affected'.format(res)
```

## Installation Instructions
I've only installed in Linux, but with some effort it should be possible to install it on MacOS and Windows (using Cygwin). -- Help wanted here.

**Dependencies**
+  libmysqlclient-dev
+  python2.7

Clone the repository and execute **setup.py**
```bash
git clone https://github.com/bruabas/pymy.git
cd pymy
python setup.py build
sudo python setup.py install
```

## Roadmap
- [x] convert MySQL numeric types into Python Numeric Types
- [x] convert MySQL date/time types into Python datetime objects
- [ ] convert BLOB data into bytearray() or something
- [ ] provide a Generator for quering huge sets of data (no memory copy)
- [ ] make a Python3 version
- [ ] make installers for systems other than Linux


## The API in a minute
### pymy.Database class
* constructor take one compulsory parameter, the **database name**
* host address, user name and password are optional. If not supplied, address is supposed to be 'localhost', user is 'root' and password is blank.

```python
db = pymy.Database(name='MyDatabase',
                   host = '192.168.1.23',
                   user = 'nonono',
                   password = '1234')
```
on success it returns a **pymy.Database** object that you will use to interact with the database. An exception (**RuntimeException**) is raised if any error occurs. 

Two methods are available to acess the database: **query()** and **execute()**

### pymy.Database.query( str )
* **query()** takes a string as a SQL query that returns a result set (usually a **SELECT** clause)
* Executes the SQL query on the server, returning a result set (**pymy.Table**) or None if the query returned no values.
* Errors in SQL syntax or communication errors raise an exception

### pymy.Database.execute( str )
* **execute()** takes a string as a SQL query not returning a result set (clauses like **INCLUDE**, **DELETE**, etc)
* Executes the SQL query on the server, returning the number of affected rows.
* Errors in SQL syntax or communication errors raise an exception

### pymy.Table class
* is an iterable, list-like object, containing the results of a **Database.query( )**
```python
# result is a pymy.Table
# you can use it as an iterable
result = db.query('select * from Employees')
for item in result:
	do_something_with(item)
	
# or as a list-like
item = result[2]
```
* its items are dictionaries with the **keys** corresponding to the **field names**
```python
# result is a pymy.Table
# item is a dictionary
result = db.query('select FirstName, LastName from Clients')
item = result[2]
name = item['FirstName']

# or
name = result[2]['FirstName']
```

### pymy.Table.column ( str )
* This method returns a tuple with the values of all rows corresponding with the desired column
```python
# result is a pymy.Table
# ages is a tuple
result = db.query('select * from Clients')
ages = result.column('age')
# age is something like ( 34, 45, 33, 20, 60, 55 )
```
