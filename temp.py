import re
import sys

# --- Configuration ---
# The name of your full graph file from `terraform graph`
INPUT_DOT_FILE = 'full_graph.dot'

# The name of the new, filtered file this script will create
OUTPUT_DOT_FILE = 'cycle_only.dot'
# --- End Configuration ---

def filter_terraform_cycle():
    """
    Reads a large Terraform .dot graph and filters it to show only
    the nodes and edges involved in a cycle (marked by color="red").
    """
    
    # Regex to find a red edge and capture the two nodes it connects.
    # It looks for lines like:
    #   "[root] ..." -> "[root] ..." [color="red", ...]
    # It captures the full quoted node names.
    edge_regex = re.compile(
        # Group 1: Capture the "from" node (anything in quotes)
        r'^\s*(".*?")'
        r'\s*->\s*'
        # Group 2: Capture the "to" node (anything in quotes)
        r'(".*?")'
        r'\s*\[.*color="red".*\]'  # Must have color="red" in attributes
    )

    involved_node_names = set()
    cycle_edge_lines = []
    
    print(f"--- Pass 1: Reading '{INPUT_DOT_FILE}' to find red edges ---")
    
    try:
        with open(INPUT_DOT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                match = edge_regex.search(line)
                if match:
                    from_node = match.group(1)
                    to_node = match.group(2)
                    
                    # Add both nodes to our set of "involved" nodes
                    involved_node_names.add(from_node)
                    involved_node_names.add(to_node)
                    
                    # Save the full line for later
                    cycle_edge_lines.append(line.strip())

    except FileNotFoundError:
        print(f"Error: Input file not found.")
        print(f"Please run `terraform graph -draw-cycles > {INPUT_DOT_FILE}` first.")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred during Pass 1: {e}")
        sys.exit(1)

    if not involved_node_names:
        print("No red-colored edges found.")
        print("Please make sure `terraform graph -draw-cycles` is correctly marking cycles.")
        sys.exit(0)

    print(f"Found {len(cycle_edge_lines)} red edges involving {len(involved_node_names)} nodes.")

    # --- Pass 2: Find Node Definitions ---
    
    print(f"\n--- Pass 2: Re-reading '{INPUT_DOT_FILE}' to find node definitions ---")
    
    node_definition_lines = set() # Use a set to avoid duplicates
    graph_attributes = []

    try:
        with open(INPUT_DOT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                
                # Skip blank lines or comments
                if not stripped_line or stripped_line.startswith('//'):
                    continue
                    
                # Skip all edges on this pass
                if '->' in stripped_line:
                    continue

                # Check if this line is a definition for one of our involved nodes
                is_node_def = False
                for node_name in involved_node_names:
                    if stripped_line.startswith(node_name):
                        node_definition_lines.add(stripped_line)
                        is_node_def = True
                        break
                
                # If it wasn't a node definition, it might be a global graph attribute
                # (like 'rankdir=TB' or 'node [shape=box]')
                if not is_node_def and stripped_line.startswith(('graph [', 'node [', 'edge [')):
                    graph_attributes.append(stripped_line)

    except Exception as e:
        print(f"An error occurred during Pass 2: {e}")
        sys.exit(1)

    print(f"Found {len(node_definition_lines)} definitions for the involved nodes.")
    print(f"Found {len(graph_attributes)} global graph attributes.")

    # --- Final Step: Write the new file ---
    
    print(f"\n--- Writing new file: '{OUTPUT_DOT_FILE}' ---")

    try:
        with open(OUTPUT_DOT_FILE, 'w', encoding='utf-8') as f:
            f.write('digraph G {\n')
            
            # Write global attributes
            if graph_attributes:
                f.write('  // Global Attributes\n')
                for attr_line in graph_attributes:
                    f.write(f'  {attr_line}\n')
                f.write('\n')
            
            # Write the node definitions
            f.write('  // Node Definitions\n')
            for node_line in sorted(list(node_definition_lines)): # Sort for consistent output
                f.write(f'  {node_line}\n')
            f.write('\n')
            
            # Write the cycle edges
            f.write('  // Cycle Edges\n')
            for edge_line in sorted(cycle_edge_lines): # Sort for consistent output
                f.write(f'  {edge_line}\n')
            
            f.write('}\n')
            
        print("\nSuccess!")
        print(f"Filtered graph written to '{OUTPUT_DOT_FILE}'")
        print("\nTo create your image, run:")
        print(f"  dot -Tpng {OUTPUT_DOT_FILE} -o cycle.png")

    except Exception as e:
        print(f"An error occurred while writing the output file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # First, make sure you have the full graph file:
    print("This script will filter 'full_graph.dot' into 'cycle_only.dot'.")
    print("Make sure you have already run:")
    print("  terraform graph -draw-cycles > full_graph.dot\n")
    
    filter_terraform_cycle()
