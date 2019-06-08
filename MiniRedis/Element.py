import threading
from functools import partial

#This class represents an element stored in MiniRedis, it keeps
#the element's key, value, and handles it's expiration time

class Element:

    def __init__(self, key, value):
        self.__key = key
        self.__value = value
        self.__timer = None

    def setValue(self, value):
        self.__value = value

    def getKey(self):
        return self.__key

    def getValue(self):
        return self.__value

    def increment(self):
        
        #increments if it is an integer, or a string representing an integer
        if type(self.__value) is int:
            self.__value += 1
        elif type(self.__value) is str:
            try:
                intValue = int(self.__value)
                if type(intValue) is int:
                    intValue += 1
                    strValue = str(intValue)
                    self.__value = strValue
            except:
                return "Error: The value type can't be incremented"

        return self.__value

    def setExpiration(self, secounds, handler):
        if self.__timer is not None:
            self.__timer.cancel()
        
        self.__timer = threading.Timer(secounds, handler, [self.__key])
        self.__timer.start()
