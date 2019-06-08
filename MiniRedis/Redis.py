from ElementBucket import ElementBucket
from Element import Element
from pyskiplist import SkipList
from threading import Lock

class Redis:

    def __init__(self, maxSize):
        self.__maxSize = maxSize
        self.__data = [None] * self.__maxSize
        self.__mutexes = [None] * self.__maxSize
        self.__dbSize = 0

    def set(self, key, value, EX="", secounds=0):
        
        self.__getMutex(key).acquire()

        element = self.__findElement(key)
        
        if element is not None:
            element.setValue(value)
            if EX == "EX":
                element.setExpiration(secounds)
        else:
            self.__addNewElement(key, value, EX, secounds)

        self.__getMutex(key).release()
        return 'OK'

    def get(self, key):
        self.__getMutex(key).acquire()

        value = None
        element = self.__findElement(key)
        if element is not None:
            value = element.getValue()
        
        self.__getMutex(key).release()
        return value

    def dbsize(self):
        return self.__dbSize


    def delete(self, key):
        self.__getMutex(key).acquire()
        
        elementsRemoved = 0
        element = self.__findElement(key)
        if element is not None:
            self.__removeElement(key)
            elementsRemoved = 1
        
        self.__getMutex(key).release()
        return elementsRemoved

    def incr(self, key):
        self.__getMutex(key).acquire()

        element = self.__findElement(key)

        #if the element doesn't exist, creates it as 0
        if element is None:
            self.__addNewElement(key, "0")
        
        element = self.__findElement(key)
        element.increment()

        self.__getMutex(key).release()
        return element.getValue()


    def zadd(self, key, score, member):
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

    def zcard(self, key):
        self.__getMutex(key).acquire()
        
        numberOfElements = 0
        element = self.__findElement(key)
        if element is not None and type(element.getValue()) is SkipList:
            currentSet = element.getValue()
            numberOfElements = currentSet.__len__()

        self.__getMutex(key).release()
        return numberOfElements
            
    def zrank(self, key, member):
        self.__getMutex(key).acquire()
        
        memberRank = "nil"
        element = self.__findElement(key)
        if element is not None and type(element.getValue()) is SkipList:
            currentSet = element.getValue()
            try:
                memberRank = currentSet.index(member)
            except:
                memberRank = "nil"
                
        self.__getMutex(key).release()
        return memberRank

    def zrange(self, key, start, stop):
        self.__getMutex(key).acquire()
        
        sortedByScore = None
        element = self.__findElement(key)
        if element is not None and type(element.getValue()) is SkipList:
            currentSet = element.getValue()
            if start > stop or start > currentSet.__len__():
                sortedByScore = []
            else:
                if stop > currentSet.__len__():
                    stop = currentSet.__len__()
                
                #Not the best solution but pyskiplist uses keys instead of 
                #indexes to get ranges, so this workaround avoids have to 
                #find another library or change this one.
                rangedList = []
                for i in range(start, stop):
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