from itertools import cycle
from .load import LoadScheduling

class OptimizedLoadScheduling(LoadScheduling):
    """Optimize Load Scheduling implementation across nodes.

    The default load scheduling doesn't balance load when there is a large
    queue of tests and each test can take variable amount of time to complete.
    It uses the following formula:
    "initial_batch = max(len(self.pending) // 4, 2 * len(self.nodes))"
    to create the initial batch of tests which are then divided equally among
    all nodes. Further when adding new tests to a node queue it uses the formula:
    "items_per_node_max = max(2, len(self.pending) // num_nodes // 2)"
    to add remaining tests in a node's queue. This creates a problem when one of
    the nodes gets a lot of long duration tests meanwhile other nodes with smaller
    tests run the rest of the tests and remain idle after that. This class attempts
    to minimize this issue it by limiting the maximum number of tests of each node to 2.
    This makes sure that if one of the nodes gets all long duration tests then due to
    limited size it will not keep more than 2 in its queue. So other nodes will be able
    to pick pending tests as their queue size decreases.
    """
    
    def __init__(self, config, log=None):
        super().__init__(config, log=log)
        self._min_queue_size = 2
        self._max_queue_size = 2
        self._log_prefix = 'XDIST'

    def check_schedule(self, node, duration=0):
        """Maybe schedule new items on the node

        If there are any globally pending nodes left then this will
        check if the given node should be given any more tests.  The
        ``duration`` of the last test is optionally used as a
        heuristic to influence how many tests the node is assigned.
        """
        if node.shutting_down:
            return

        if self.pending:
            # how many nodes do we have?
            num_nodes = len(self.node2pending)
            # if our node goes below a heuristic minimum, fill it out to
            # heuristic maximum
            items_per_node_min = self._min_queue_size
            items_per_node_max = self._max_queue_size
            node_pending = self.node2pending[node]
            if len(node_pending) < items_per_node_min:
                if duration >= 0.1 and len(node_pending) >= self._max_queue_size:
                    # seems the node is doing long-running tests
                    # and has enough items to continue
                    # so let's rather wait with sending new items
                    return
                num_send = items_per_node_max - len(node_pending)
                self._send_tests(node, num_send)
        else:
            node.shutdown()

        queue_log = f'\n{self._log_prefix} - {type(self).__name__}::check_schedule - {self.node2pending}'
        print(queue_log)
        self.log(queue_log)
        self.log("num items waiting for node:", len(self.pending))
    
    def schedule(self):
        """Initiate distribution of the test collection

        Initiate scheduling of the items across the nodes.  If this
        gets called again later it behaves the same as calling
        ``.check_schedule()`` on all nodes so that newly added nodes
        will start to be used.

        This is called by the ``DSession.worker_collectionfinish`` hook
        if ``.collection_is_completed`` is True.
        """
        assert self.collection_is_completed

        # Initial distribution already happened, reschedule on all nodes
        if self.collection is not None:
            for node in self.nodes:
                self.check_schedule(node)
            return

        # XXX allow nodes to have different collections
        if not self._check_nodes_have_same_collection():
            self.log("**Different tests collected, aborting run**")
            return

        # Collections are identical, create the index of pending items.
        self.collection = list(self.node2collection.values())[0]
        self.pending[:] = range(len(self.collection))
        if not self.collection:
            return

        # Send a batch of tests to run. If we don't have at least two
        # tests per node, we have to send them all so that we can send
        # shutdown signals and get all nodes working.
        initial_batch = self._max_queue_size * len(self.nodes)

        # distribute tests round-robin up to the batch size
        # (or until we run out)
        nodes = cycle(self.nodes)
        for i in range(initial_batch):
            self._send_tests(next(nodes), 1)

        if not self.pending:
            # initial distribution sent all tests, start node shutdown
            for node in self.nodes:
                node.shutdown()
        
        queue_log = f'\n{self._log_prefix} - {type(self).__name__}::schedule - {self.node2pending}'
        print(queue_log)
        self.log(queue_log)
