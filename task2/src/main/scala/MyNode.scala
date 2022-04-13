// Ivan Korovkin
package task2

import scala.collection.mutable.Queue
import scala.collection.mutable.Set

class MyNode(id: String, memory: Int, neighbours: Vector[String], router: Router) extends Node(id, memory, neighbours, router) {
    val STORE = "STORE"
    val STORE_SUCCESS = "STORE_SUCCESS"
    val STORE_FAILURE = "STORE_FAILURE"
    val RETRIEVE = "RETRIEVE"
    val GET_NEIGHBOURS = "GET_NEIGHBOURS"
    val NEIGHBOURS_RESPONSE = "NEIGHBOURS_RESPONSE"
    val RETRIEVE_SUCCESS = "RETRIEVE_SUCCESS"
    val RETRIEVE_FAILURE = "RETRIEVE_FAILURE"
    val INTERNAL_ERROR = "INTERNAL_ERROR"
    val USER = "USER"
    val STORE_Optimized = "STORE_Optimized"
    val RETRIEVE_Optimized = "RETRIEVE_Optimized"
    val nodes_may_fail = 4


    override def onReceive(from: String, message: Message): Message = {
        /* 
         * Called when the node receives a message from some where
         * Feel free to add more methods and code for processing more kinds of messages
         * NOTE: Remember that HOST must still comply with the specifications of USER messages
         *
         * Parameters
         * ----------
         * from: id of node from where the message arrived
         * message: the message
         *           (Process this message and decide what to do)
         *           (All communication between peers happens via these messages)
         */
        if (message.messageType == GET_NEIGHBOURS) { // Request to get the list of neighbours
            new Message(id, NEIGHBOURS_RESPONSE, neighbours.mkString(" "))
        }
        else if (message.messageType == RETRIEVE) { // Request to get the value
            val key = message.data // This is the key
            val value = getKey(key) // Check if the key is present on the node
            var response: Message = new Message("", "", "")
            value match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => response = new Message(id, RETRIEVE_FAILURE)
            }
            //if no value found on node
            if (value.isEmpty) {
                //assigning variables to check unique nodes
                var nodes_to_visit: Queue[String] = Queue()
                var nodes_visited: Set[String] = Set()
                nodes_to_visit ++= neighbours
                nodes_visited += id

                //trying to retrieve value unless we recieve retrieve_success or no more nodes to go
                while (response.messageType == RETRIEVE_FAILURE && nodes_to_visit.nonEmpty) {
                    //getting new node from queue
                    val node_to_visit = nodes_to_visit.dequeue()
                    //getting the response from this node
                    response = router.sendMessage(id, node_to_visit, new Message(id, RETRIEVE_Optimized, message.data))
                    //updating queue of nodes to visit based on current node neighbours
                    nodes_visited += node_to_visit
                    nodes_to_visit ++= response.data.split(",").toSet diff nodes_visited diff nodes_to_visit.toSet
                }

            }

            response // Return the correct response message
        }
            //Condition used to get response from single node in Retrieve operation
        else if (message.messageType == RETRIEVE_Optimized) {
            val key = message.data  // This is the key
            val value = getKey(key) // check if the key is present on the node
            var response: Message = new Message("", "", "")
            value match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => response = new Message(id, RETRIEVE_FAILURE, neighbours.mkString(","))
            }
            response // Return the correct response message
        }
        else if (message.messageType == STORE) { // Request to store key->value

            val data = message.data.split("->") // data(0) is key, data(1) is value
            val storedOnSelf = setKey(data(0), data(1)) // Store on current node
            //Assigning fault tolerance variable
            var fault_tolerance = nodes_may_fail + 1
            //if value successfully stored on the node, decrease fault tolerance
            if (storedOnSelf) fault_tolerance -= 1
            //assigning variables to check unique nodes
            var nodes_to_visit: Queue[String] = Queue()
            var nodes_visited: Set[String] = Set()
            nodes_to_visit ++= neighbours
            nodes_visited += id
            //trying to store value unless we have needed fault tolerance level or no more nodes to go
            while (fault_tolerance > 0 && nodes_to_visit.nonEmpty) {
                //getting new node from queue
                val node_to_visit = nodes_to_visit.dequeue()
                //getting the response from this node
                val response = router.sendMessage(id, node_to_visit, new Message(id, STORE_Optimized, message.data))
                //updating queue of nodes to visit based on current node neighbours
                nodes_visited += node_to_visit
                nodes_to_visit ++= response.data.split(",").toSet diff nodes_visited diff nodes_to_visit.toSet
                //if value successfully stored on the node, decrease fault tolerance
                if (response.messageType == STORE_SUCCESS) fault_tolerance -= 1
            }
            //return the right message
            if (fault_tolerance == 0) new Message(id, STORE_SUCCESS)
            else new Message(id, STORE_FAILURE)
        }
            //Condition to get response from single node in store operation
        else if (message.messageType == STORE_Optimized) {
            val data = message.data.split("->")         // data(0) is key, data(1) is value
            val storedOnSelf = setKey(data(0), data(1)) // Store on current node

            if (storedOnSelf)
                new Message(id, STORE_SUCCESS, neighbours.mkString(","))
            else
                new Message(id, STORE_FAILURE, neighbours.mkString(","))
        }
        /*
         * Feel free to add more kinds of messages.
         */
        else
            new Message(id, INTERNAL_ERROR)
    }
}