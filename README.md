# MiniRedis

This project is an oversimplified implementation of [Redis](https://redis.io/) and was developed as part of the hiring process of [Aquiris Game Studios](https://www.aquiris.com.br/).

### Setup

Install [pip](https://pypi.org/)

  `apt-get install python-pip`

Install [pyskiplist](https://github.com/geertj/pyskiplist)

  `pip install pyskiplist`

### Running tests

Just run:

`python tests.py`


### Example library usage

```

from MiniRedis import Redis

def main():
    miniRedis = Redis.Redis(1000)
    
    miniRedis.set("mykey1", 2)
    miniRedis.get("mykey1")
    miniRedis.dbsize()
    miniRedis.delete("mykey1")
    miniRedis.incr("mykey1")
    miniRedis.zadd("myzkey1", "2", "two"):
    miniRedis.zcard("myzkey1"):
    miniRedis.zrank("myzkey1, "two"):
    miniRedis.zrange("myzkey1, 1, 5):

if __name__== "__main__":
    main()


```

### Running webservice

Just run:

`python MiniRedisServer.py`


### Example webservice usage

`curl -d 200 -X PUT localhost:8080/mykey`

`curl -X GET localhost:8080/mykey`

`curl -X DELETE localhost:8080/mykey`

`curl -X GET localhost:8080/mykey`


### Considerations

* Some improovements could be done in the zrange method, details in the method's body
* The method "del" was changed to "delete" because del is a reserved word in python
