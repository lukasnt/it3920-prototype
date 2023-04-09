# Examples

This file contains some examples of use cases for the code found in the repository along with the results of the queries.

## Person Graph

The following example shows a graph of person nodes and their relationships to other person nodes, where an edge is a Knows-relationship. 
Edge directions are not included in the visualization.

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/snb_person_graph_example.png)

## Example Query

The following example shows a query that finds the top-5 minimal continuous paths originating from a male ending in a female, where the path length is at least 3 and at most 5. 
The weight is defined as the duration of the interval of the edge, meaning how long the persons have known each other. Therefore, the these paths are persons who have known each other the least amount of time.

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/test_query_definition.png)

### Table Result

This shows the result of the query as a table. The columns are the weight of the path, the source-id of the path, the destination-id of the path, the start-time of the continuous interval, the end-time of the continuous interval and the path as sequence of ids.

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/test_query_table.png)

### Graph Visualization

Given the person graph from above, this shows the result of the query as a graph visualization, where the nodes are the persons with their names and the edges are the Knows-relationships.

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/test_query_visualization.png)


## Example Query 2

Modified version of the previous query, where we negate the weights of the edges, meaning that the paths are persons who have known each other the longest amount of time.
In addition, we retrieve the top-20 paths and the sources and destinations are now "male".

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/modified_query_definition.png)

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/modified_query_table.png)

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/modified_query_visualization.png)


## Scalable Examples

The following examples are from the same graph as above, but now no limit on results (topK = 300) value and no real constraint on the length (maxLength = 10).
We can see that the time taken increases only slightly even though the input size increases by a much larger factor. This remains to be seen for larger graphs.

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/scalable_query_definition.png)

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/scalable_query_table.png)


### NonTemporal Example

We can see that there are some more results when we do not include the temporal constraint of a continuous path.

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/nontemporal_query_definition.png)

![Person nodes and Knows-relationships Graph](https://gitlab.stud.idi.ntnu.no/lukasnt/it3920-lukasnt/-/raw/main/examples/nontemporal_query_table.png)
