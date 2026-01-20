"""DAG analysis utilities using NetworkX for workflow execution."""
from typing import Dict, List, Any, Set
import networkx as nx
from utils.logger import get_logger


logger = get_logger(__name__)


class DAGAnalyzer:
    """Analyzes workflow DAG structure and determines execution order."""
    
    @staticmethod
    def build_dag_from_workflow(workflow: Dict[str, Any]) -> nx.DiGraph:
        """Build NetworkX directed graph from workflow document.
        
        Args:
            workflow: Workflow document with steps
            
        Returns:
            NetworkX DiGraph with steps as nodes
        """
        G = nx.DiGraph()
        
        steps = workflow.get('steps', [])
        
        # Add nodes (steps) - ALWAYS use step_name as node ID for consistency
        for step in steps:
            step_name = step.get('step_name')
            if not step_name:
                logger.error(f"Step missing step_name: {step}")
                continue
            G.add_node(step_name, **step)
        
        # Add edges (dependencies)
        # Dependencies in the JSON are always step_names (before submission)
        for step in steps:
            step_name = step.get('step_name')
            if not step_name:
                continue
                
            depends_on = step.get('depends_on', [])
            
            for dep in depends_on:
                # dep is a step_name (or could be step_id after resolution, need to map back)
                # First try to find by step_id, then by step_name
                dep_step = next((s for s in steps if s.get('step_id') == dep or s.get('step_name') == dep), None)
                if dep_step:
                    dep_step_name = dep_step.get('step_name')
                    G.add_edge(dep_step_name, step_name)
                else:
                    # Assume dep is already a step_name
                    G.add_edge(dep, step_name)
        
        logger.debug(
            f"Built DAG with {G.number_of_nodes()} nodes and "
            f"{G.number_of_edges()} edges"
        )
        
        return G
    
    @staticmethod
    def validate_dag(G: nx.DiGraph) -> bool:
        """Validate that graph is a valid DAG (no cycles).
        
        Args:
            G: NetworkX directed graph
            
        Returns:
            True if valid DAG
            
        Raises:
            ValueError: If cycles detected
        """
        if not nx.is_directed_acyclic_graph(G):
            try:
                cycle = nx.find_cycle(G, orientation='original')
                logger.error(f"Cycle detected in workflow: {cycle}")
                raise ValueError(f"Workflow contains circular dependencies: {cycle}")
            except nx.NetworkXNoCycle:
                # Should not happen, but handle anyway
                raise ValueError("Invalid DAG structure")
        
        logger.debug("DAG validation passed - no cycles detected")
        return True
    
    @staticmethod
    def get_ready_steps(
        G: nx.DiGraph,
        completed_steps: Set[str]
    ) -> List[Dict[str, Any]]:
        """Get steps that are ready to run (dependencies satisfied).
        
        A step is ready if:
        - Its status is 'pending'
        - All its predecessors are in completed_steps set
        
        Args:
            G: NetworkX directed graph
            completed_steps: Set of step_ids that have completed
            
        Returns:
            List of step dictionaries ready to run
        """
        ready = []
        
        for node in G.nodes():
            step_data = dict(G.nodes[node])  # Convert to plain dict
            status = step_data.get('status', 'pending')
            
            if status == 'pending':
                # Check if all predecessors are completed
                predecessors = set(G.predecessors(node))
                
                if predecessors.issubset(completed_steps):
                    ready.append(step_data)
        
        logger.debug(f"Found {len(ready)} ready steps")
        return ready
    
    @staticmethod
    def get_running_steps(G: nx.DiGraph) -> List[Dict[str, Any]]:
        """Get steps that are currently running.
        
        Args:
            G: NetworkX directed graph
            
        Returns:
            List of step dictionaries with status='running'
        """
        running = []
        
        for node in G.nodes():
            step_data = dict(G.nodes[node])  # Convert to plain dict
            status = step_data.get('status', 'pending')
            
            if status == 'running':
                running.append(step_data)
        
        return running
    
    @staticmethod
    def is_workflow_complete(G: nx.DiGraph) -> bool:
        """Check if all steps are in terminal states.
        
        Args:
            G: NetworkX directed graph
            
        Returns:
            True if all steps are completed, failed, or skipped
        """
        terminal_states = {'succeeded', 'failed', 'upstream_failed', 'skipped'}
        
        for node in G.nodes():
            step_data = G.nodes[node]
            status = step_data.get('status', 'pending')
            
            if status not in terminal_states:
                return False
        
        return True
    
    @staticmethod
    def has_workflow_failed(G: nx.DiGraph) -> bool:
        """Check if any step has failed.
        
        Args:
            G: NetworkX directed graph
            
        Returns:
            True if any step has status='failed'
        """
        for node in G.nodes():
            step_data = G.nodes[node]
            status = step_data.get('status', 'pending')
            
            if status == 'failed':
                return True
        
        return False
    
    @staticmethod
    def has_workflow_succeeded(G: nx.DiGraph) -> bool:
        """Check if all steps have succeeded.
        
        Args:
            G: NetworkX directed graph
            
        Returns:
            True if all steps have status='succeeded'
        """
        for node in G.nodes():
            step_data = G.nodes[node]
            status = step_data.get('status', 'pending')
            
            if status != 'succeeded':
                return False
        
        return True
    
    @staticmethod
    def get_downstream_steps(
        G: nx.DiGraph,
        step_id: str
    ) -> List[str]:
        """Get all steps that depend on the given step.
        
        Uses NetworkX descendants to find all downstream dependencies.
        
        Args:
            G: NetworkX directed graph
            step_id: Step identifier
            
        Returns:
            List of step_ids that depend on this step
        """
        try:
            descendants = nx.descendants(G, step_id)
            return list(descendants)
        except nx.NetworkXError as e:
            logger.warning(f"Error getting downstream steps for {step_id}: {e}")
            return []
    
    @staticmethod
    def get_execution_order(G: nx.DiGraph) -> List[str]:
        """Get topological ordering of steps (execution order).
        
        Args:
            G: NetworkX directed graph
            
        Returns:
            List of step_ids in topological order
        """
        try:
            return list(nx.topological_sort(G))
        except nx.NetworkXError as e:
            logger.error(f"Error getting topological sort: {e}")
            raise ValueError(f"Cannot determine execution order: {e}")

