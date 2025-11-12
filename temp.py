T_FILE}' ---")

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
