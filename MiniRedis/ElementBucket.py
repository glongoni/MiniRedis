from Element import Element

#Linked list implementation, each ElementBucket can contain N elements sharing
#the same index in the hash table. Those lists are supose to be small, so it 
#shouldn't be a performance issue 

class ElementBucket:

    def __init__(self, element):
        self.__element = element
        self.__nextBucket = None

    def getElement(self):
        return self.__element

    def getNextBucket(self):
        return self.__nextBucket

    def setNextBucket(self, nextBucket):
        self.__nextBucket = nextBucket

    def find(self, key):
        bucketIt = self
        while bucketIt is not None and bucketIt.getElement().getKey() != key:
            bucketIt = bucketIt.getNextBucket()

        if bucketIt is not None:
            return bucketIt.getElement()
        else:
            return None

    def insert(self, element):
        bucketIt = self

        while bucketIt.getNextBucket() is not None:
            bucketIt = bucketIt.getNextBucket()

        bucketIt.setNextBucket(ElementBucket(element))


    #Removes an element and return the new bucket head node
    def remove(self, key):
        
        if self.getElement().getKey() == key:
            if self.getNextBucket() is not None:
                return self.getNextBucket()
            else:
                return None
        else:
            prevBucket = self
            bucketIt = self.getNextBucket()

            while bucketIt is not None:
                if bucketIt.getElement().getKey() == key:
                    prevBucket.setNextBucket(bucketIt.getNextBucket())
                    bucketIt = None
                else:
                    prevBucket = bucketIt
                    bucketIt = bucketIt.getNextBucket()

            return self
            


            
