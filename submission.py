import heapq
from datetime import datetime
from itertools import zip_longest
from lxml import etree 
from multiprocessing import Process, Manager

class ListNode:
    def __init__(self, value=0, prev=None, next=None):
        self.value = value
        self.prev = prev
        self.next = next

class DoublyLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.node_map = {}  # A hashmap to hold the addresses of the nodes

    def append(self, value):
        new_node = ListNode(value)
        self.node_map[value] = new_node

        if self.tail is None:
            self.head = self.tail = new_node
        else:
            self.tail.next = new_node
            new_node.prev = self.tail
            self.tail = new_node



    def remove(self, value):
        node = self.node_map.pop(value, None)
        if node is None:
            return  # Node not found

        # Disconnect the node from the list
        if node.prev:
            node.prev.next = node.next
        else:  # Node is the head
            self.head = node.next
            if self.head:
                self.head.prev = None

        if node.next:
            node.next.prev = node.prev
        else:  # Node is the tail
            self.tail = node.prev
            if self.tail:
                self.tail.next = None

        # If the list is now empty, reset both head and tail to None
        if self.head is None:
            self.tail = None

    def find(self, value):
        return self.node_map.get(value, None)

    def display(self):
        current = self.head
        while current:
            print(current.value, end=" <-> ")
            current = current.next
        print("None")
    
    def getFirst(self):
        return self.head.value if self.head else None
    
    def len(self):
        return len(self.node_map)
    
    def listDll(self):
        return list(self.node_map.keys())

class Order:
    def __init__(self,orderId,side,price,volume):
        self.orderId = orderId
        self.side = side
        self.price = price
        self.volume = volume    

class OrderBook:
    def __init__(self,name):
        self.bestAsk = []
        self.bestBid = []
        self.orderMap = {}
        self.queueMap = {}
        self.name = name

    def placeOrder(self,order):
        if order.side=="BUY":
            while order.volume>0 and self.bestAsk and self.bestAsk[0]<=order.price:
                while self.bestAsk and self.queueMap[(self.bestAsk[0],"SELL")].len()==0 :
                    self.queueMap.pop((self.bestAsk[0],"SELL"))
                    heapq.heappop(self.bestAsk)
                if not self.bestAsk or self.bestAsk[0]>order.price:break
                otherOrder = self.orderMap[self.queueMap[(self.bestAsk[0],"SELL")].getFirst()]
                tradeVolume = min(order.volume,otherOrder.volume)
                order.volume -= tradeVolume
                otherOrder.volume -= tradeVolume
                if otherOrder.volume==0:
                    self.cancel(otherOrder.orderId)
                    
            if order.volume>0:
                self.orderMap[order.orderId] = order
                # print(order.price,order.side,order.volume)
                self.addOrderToBook(order,self.bestBid)
        else:
            while order.volume>0 and self.bestBid and -self.bestBid[0]>=order.price:
                
                while self.bestBid and  self.queueMap[(-self.bestBid[0],"BUY")].len()==0 :
                    # self.heapMap.pop((-self.bestBid[0],"BUY"))
                    self.queueMap.pop((-self.bestBid[0],"BUY"))
                    heapq.heappop(self.bestBid)
                if not self.bestBid or -self.bestBid[0]<order.price:break

                otherOrder = self.orderMap[self.queueMap[(-self.bestBid[0],"BUY")].getFirst()]
                tradeVolume = min(order.volume,otherOrder.volume)
                order.volume -= tradeVolume
                otherOrder.volume -= tradeVolume
                if otherOrder.volume==0:
                    self.cancel(otherOrder.orderId)
            if order.volume>0:
                self.orderMap[order.orderId] = order
                self.addOrderToBook(order,self.bestAsk)
        
            
    
   
   
    def addOrderToBook(self,order,book):
        if not(order.price,order.side) in self.queueMap:
            # print("not",(order.price,order.side))
            newQ = DoublyLinkedList()
            newQ.append(order.orderId)
            # print(len(self.bestAsk),len(self.bestBid))
            heapq.heappush(book,-order.price) if order.side == "BUY" else heapq.heappush(book,order.price)
            # print(len(self.bestAsk),len(self.bestBid))
            # self.heapMap[(order.price,order.side)] = len(book)-1
            self.queueMap[(order.price,order.side)] = newQ
            # self.volumeMap[(order.price,order.side)] = order.volume
        else:
            self.queueMap[(order.price,order.side)].append(order.orderId)


    def cancel(self,orderId):
        if orderId in self.orderMap:
            order = self.orderMap[orderId]
            priceQueue = self.queueMap[(order.price,order.side)] 
            priceQueue.remove(orderId)
            self.orderMap.pop(orderId)
            

    
    # def getVolumeAtPrice(self,price,side):
    #     return self.volumeMap[(price,side)] if (price,side) in self.volumeMap else 0
    
    # def __str__(self):
    def buy_orders_str(self):
        """Generate strings for buy orders in descending price order."""
        for order in sorted(self.bestBid):
            order2 =self.queueMap[(-order, "BUY")]
            for orderId in order2.listDll():
                yield f"{self.orderMap[orderId].volume}@{-order:.2f}"
            # yield f"{order.volume}@{order.price:.2f}"

    def sell_orders_str(self):
        """Generate strings for sell orders in ascending price order."""
        for order in sorted(self.bestAsk):
            order2 =self.queueMap[(order, "SELL")]
            for orderId in order2.listDll():
                yield f"{self.orderMap[orderId].volume}@{order:.2f}"

    def printer(self):
        print(self.bestAsk)
        print(self.bestBid)

class OrderBookManager:
    def __init__(self):
        self.order_books = {}
    def add_order_to_book(self, book, order):
        if book not in self.order_books:
            self.order_books[book] = OrderBook(book)
        
        self.order_books[book].placeOrder(order)

    def delete_order_from_book(self, book, order_id):
        if book in self.order_books:
            self.order_books[book].cancel(order_id)
    
    def __str__(self):
        output = []
        for book_name, order_book in sorted(self.order_books.items()):
            output.append(f"book: {book_name}")
            output.append(" Buy -- Sell")
            output.append("==================================")
            # Ensure that the methods return iterable even if they are None or not iterable
            buy_orders = order_book.buy_orders_str() if order_book.buy_orders_str() is not None else []
            sell_orders = order_book.sell_orders_str() if order_book.sell_orders_str() is not None else []
            book_strs = zip_longest(buy_orders, sell_orders, fillvalue="")
            for buy_str, sell_str in book_strs:
                output.append(f"{buy_str} -- {sell_str}")
            output.append("")

        return '\n'.join(output)

    def printer(self):
        for book_name, order_book in self.order_books.items():
            print(book_name)
            order_book.printer()
            print("")

def process_orders(file_path):
    print(f"Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
    start_time = datetime.now()
    parser = etree.XMLParser()
    # tree = ET.parse(file_path)
    tree = etree.parse(file_path,parser)
    root = tree.getroot()
    manager = OrderBookManager()

    for elem in root:
        book_name = elem.attrib['book']
        if elem.tag == 'AddOrder':
            order = Order(
                orderId=elem.attrib['orderId'],
                side=elem.attrib['operation'],
                price=float(elem.attrib['price']),
                volume=int(elem.attrib['volume'])
            )
            manager.add_order_to_book(book_name, order)
        elif elem.tag == 'DeleteOrder':
            order_id = elem.attrib['orderId']
            manager.delete_order_from_book(book_name, order_id)

    print(manager)
    end_time = datetime.now()
    duration = end_time-start_time
    print(f"Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"Processing Duration: {duration} seconds")



def process_book(book_name, orders, order_book_managers_dict):
    order_book_manager = OrderBookManager()  # each process gets its own OrderBookManager
    for order_data in orders:
        if order_data['action'] == 'AddOrder':
            order = Order(
                orderId=order_data['orderId'],
                side=order_data['operation'],
                price=float(order_data['price']),
                volume=int(order_data['volume'])
            )
            order_book_manager.add_order_to_book(book_name, order)
        elif order_data['action'] == 'DeleteOrder':
            order_book_manager.delete_order_from_book(book_name, order_data['orderId'])
    order_book_managers_dict[book_name] = order_book_manager  # Store the manager in the shared dictionary

def process_orders_MP(file_path):
    print(f"Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
    start_time = datetime.now()
    
    # Parse the XML file
    parser = etree.XMLParser()
    tree = etree.parse(file_path, parser)
    root = tree.getroot()

    manager = Manager()
    order_book_managers_dict = manager.dict()  # shared dictionary to store OrderBookManagers for each book
    processes = []

    orders_by_book = {f'book-{i}': [] for i in range(1, 4)}  # Assuming there are 3 books
    for elem in root:
        book_name = elem.attrib['book']
        if book_name in orders_by_book:
            order_data = {
                'action': elem.tag,
                'orderId': elem.attrib['orderId'],
                'operation': elem.attrib.get('operation', ''),
                'price': elem.attrib.get('price', '0'),
                'volume': elem.attrib.get('volume', '0')
            }
            orders_by_book[book_name].append(order_data)

    for book_name, orders in orders_by_book.items():
        p = Process(target=process_book, args=(book_name, orders, order_book_managers_dict))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    for book_name, order_book_manager in order_book_managers_dict.items():
        print(order_book_manager)
        print("")
    end_time = datetime.now()
    print(f"Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"Processing Duration: {end_time - start_time}")



if __name__ == '__main__':
    file_path = "orders.xml"  # Replace with the actual file path
    # process_orders(file_path)
    process_orders_MP(file_path)