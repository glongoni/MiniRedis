from ElementBucket import ElementBucket
from Element import Element
from pyskiplist import SkipList
from threading import Lock

#This class implements a very simplified Redis server using hash tables,
#linked lists for colision handle, and Skip lists to optimize operations
#in ordered sets

class Redis:

    def __init__(self, maxSize):
        self.__maxSize = maxSize
        self.__data = [None] * self.__maxSize
        self.__mutexes = [None] * self.__maxSize
        self.__dbSize = 0

    #Set key to hold a value. If key already holds a value, it is overwritten.
    #
    #   @param  key         value identifier
    #   @param  value       value to be holded
    #   @param  EX          set to "EX" to enable an expiration time for this 
    #                       value (optional)
    #   @param  secounds    expiration time in secounds (optional)
    # 
    #   @return             "OK" after creates the new element 
    #
    def set(self, key, value, EX="", secounds=0):

        if type(key) is not str:
            return "Error: key must be a string"
        
        self.__getMutex(key).acquire()

        element = self.__findElement(key)
        
        if element is not None:
            element.setValue(value)
            if EX == "EX":
                element.setExpiration(secounds, self.__removeElement)
        else:
            self.__addNewElement(key, value, EX, secounds)

        self.__getMutex(key).release()
        return 'OK'

    #Get the value of key. If the key does not exist the special value None is 
    #returned. An error is returned if the value stored at key is not a string
    #
    #   @param  key         value identifier
    # 
    #   @return             the value of key, or None when key does not exist
    #
    def get(self, key):
        if type(key) is not str:
            return "Error: key must be a string"

        self.__getMutex(key).acquire()

        value = None
        element = self.__findElement(key)
        if element is not None:
            value = element.getValue()
        
        self.__getMutex(key).release()
        return value

    #Return the number of keys in the currently-selected database
    #
    #  @return             number of keys in the database
    #
    def dbsize(self):
        return self.__dbSize

    #Removes the specified key. A key is ignored if it does not exist
    #
    #  @param  key         value identifier
    #
    #  @return             The number of keys that were removed
    #
    def delete(self, key):
        if type(key) is not str:
            return "Error: key must be a string"

        self.__getMutex(key).acquire()
        
        elementsRemoved = 0
        element = self.__findElement(key)
        if element is not None:
            self.__removeElement(key)
            elementsRemoved = 1
        
        self.__getMutex(key).release()
        return elementsRemoved

    #Increments the number stored at key by one. If the key does not exist, it 
    #is set to 0 before performing the operation. An error is returned if the 
    #key contains a value of the wrong type or contains a string that can not 
    #be represented as integer
    #
    #  @param  key         value identifier
    #
    #  @return             the value of key after the increment / error message
    #
    def incr(self, key):
        if type(key) is not str:
            return "Error: key must be a string"

        self.__getMutex(key).acquire()

        element = self.__findElement(key)

        #if the element doesn't exist, creates it as 0
        if element is None:
            self.__addNewElement(key, "0")
        
        element = self.__findElement(key)
        toReturn = element.increment()

        self.__getMutex(key).release()
        return toReturn

    #Adds the specified member with the specified score to the sorted set
    #stored at key. If key does not exist, a new sorted set with the specified
    #members as sole members is created. If the key exists but does not hold a
    #sorted set, an error is returned. The score values should be the string 
    #representation of a double precision floating point number. +inf and -inf 
    #values are valid values as well.
    #
    #  @param  key         value identifier
    #  @param  score       member score
    #  @param  member      member to be added or updated
    #
    #  @return             the value of key after the increment / error message
    #
    def zadd(self, key, score, member):
        if type(key) is not str:
            return "Error: key must be a string"

        #check if the score is valid
        try:
            float(score)
            if type(score) is not str:
                raise
        except:
            if score != "+inf" and score != "-inf":
                return "Error: " + str(score) + " is not a string representation of a float"
        
        self.__getMutex(key).acquire()

        #Finds the element or creates a new one
        element = self.__findElement(key)
        if element is None:
            self.__addNewElement(key, SkipList())
            element = self.__findElement(key)
        currentSet = element.getValue()
        
        #Checks if key is valid
        if type(currentSet) is not SkipList:
            self.__getMutex(key).release()
            return "Error: " + key + " is not an ordered set"
        
        #Will return 0 if the element already exist or 1 otherwise 
        elementsCreated = 0
        if currentSet.search(member) is None:
            elementsCreated = 1

        #Replaces the member with the new score, or creates a new one
        currentSet.replace(member, score)

        self.__getMutex(key).release()
        return elementsCreated

    #Returns the number of elements of the sorted set 
    #
    #  @param  key         set identifier
    #
    #  @return             number of elements or 0 if the set doesn't exist
    #
    def zcard(self, key):
        if type(key) is not str:
            return "Error: key must be a string"

        self.__getMutex(key).acquire()
        
        numberOfElements = 0
        element = self.__findElement(key)
        if element is not None and type(element.getValue()) is SkipList:
            currentSet = element.getValue()
            numberOfElements = currentSet.__len__()

        self.__getMutex(key).release()
        return numberOfElements

    #Returns the rank of member in the sorted set stored at key, with the 
    #scores ordered from low to high. The rank (or index) is 0-based, which 
    #means that the member with the lowest score has rank 0.        
    #
    #  @param  key         set identifier
    #  @param  member      member to check the rank
    #
    #  @return             the rank of member, or None if member or set doesn't
    #                      exist
    #
    def zrank(self, key, member):
        self.__getMutex(key).acquire()
        
        memberRank = None
        element = self.__findElement(key)
        if element is not None and type(element.getValue()) is SkipList:
            currentSet = element.getValue()
            try:
                memberRank = currentSet.index(member)
            except:
                memberRank = None
                
        self.__getMutex(key).release()
        return memberRank

    #Returns the specified range of elements in the sorted set stored at key. 
    #The elements are considered to be ordered from the lowest to the highest 
    #score. Both start and stop are zero-based indexes and both are inclusive
    #ranges
    #  
    #  @param  key         set identifier
    #  @param 
    #
    #  @return             the rank of member, or None if member or set doesn't
    #
    def zrange(self, key, start, stop):
        self.__getMutex(key).acquire()
        
        sortedByScore = None
        element = self.__findElement(key)
        if element is not None and type(element.getValue()) is SkipList:
            currentSet = element.getValue()
            if start > stop or start > currentSet.__len__():
                sortedByScore = []
            else:
                if stop > currentSet.__len__() - 1:
                    stop = currentSet.__len__() - 1
                elif stop < 0:
                    stop = currentSet.__len__() + stop
                
                #Not the best solution but pyskiplist uses keys instead of 
                #indexes to get ranges, so this workaround avoids have to 
                #find another library or change this one.
                rangedList = []
                for i in range(start, stop + 1):
                    rangedList.append(currentSet.__getitem__(i))

                sortedByScore = sorted(rangedList, key=lambda item : int(item[1]))

        self.__getMutex(key).release()
        return sortedByScore


    def __addNewElement(self, key, value, EX="", secounds=0):
        index = hash(key) % self.__maxSize
        self.__dbSize += 1
        elementBucket = self.__data[index]

        newElement = Element(key, value)
        if elementBucket is not None:
            elementBucket.insert(newElement)
        else:
            self.__data[index] = ElementBucket(newElement)

        if EX == "EX":
            newElement.setExpiration(secounds, self.__removeElement)
            

    def __removeElement(self, key):
        print("element removed")
        index = hash(key) % self.__maxSize
        self.__dbSize -= 1
        elementBucket = self.__data[index]
        self.__data[index] = elementBucket.remove(key)


    def __findElement(self, key):
        index = hash(key) % self.__maxSize
        elementBucket = self.__data[index]
        if elementBucket is not None:
            return elementBucket.find(key)
        else:
            return None

    def __getMutex(self, key):
        index = hash(key) % self.__maxSize
        if self.__mutexes[index] is None:
            self.__mutexes[index] = Lock()
        
        return self.__mutexes[index]