using Unity.Burst;
using Unity.Collections;
using Unity.Jobs;
using UnityEngine;
using System;

public class parallelBFS : MonoBehaviour
{
    [Serializable]
    public struct EdgeMeta
    {
        public int connectedNodeId;
        public int nodeId;
    }

    [Serializable]
    public struct NodeMeta
    {
        public int edgeStartIndex;
        public int edgeCount;

        public int nodeId;
    }

    [BurstCompile]
    public struct BFSNodesJob : IJobParallelFor
    {
        [ReadOnly] public NativeList<NodeMeta> nodes;
        [ReadOnly] public NativeList<EdgeMeta> edges;
        [ReadOnly] public NativeList<NodeMeta> currentNodes;

        public NativeList<NodeMeta>.ParallelWriter nextNodes;
        public NativeParallelHashSet<int>.ParallelWriter visited;
        public NativeList<int>.ParallelWriter result;

        public void Execute(int index)
        {
            NodeMeta node = currentNodes[index];

            for (int i = node.edgeStartIndex; i < node.edgeStartIndex + node.edgeCount; i++)
            {
                EdgeMeta edge = edges[i];
                int nextId = edge.connectedNodeId;

                if (!visited.Add(nextId))
                {
                    continue;
                }
                    
                NodeMeta nextNode = nodes[nextId];

                nextNodes.AddNoResize(nextNode);
                result.AddNoResize(nextId);
            }
        }
    }

    [Range(0, 5)]
    public int rootNode;

    public NativeList<NodeMeta> nodesNative;
    public NativeList<EdgeMeta> edgesNative;

    public NativeList<NodeMeta> sideA;
    public NativeList<NodeMeta> sideB;

    public NativeParallelHashSet<int> visited;
    public NativeList<int> result;

    void Start()
    {
        edgesNative = new NativeList<EdgeMeta>(16, Allocator.Persistent);

        edgesNative.Add(new EdgeMeta { connectedNodeId = 1, nodeId = 0 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 3, nodeId = 0 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 4, nodeId = 0 });

        edgesNative.Add(new EdgeMeta { connectedNodeId = 0, nodeId = 1 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 2, nodeId = 1 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 5, nodeId = 1 });

        edgesNative.Add(new EdgeMeta { connectedNodeId = 1, nodeId = 2 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 3, nodeId = 2 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 4, nodeId = 2 });

        edgesNative.Add(new EdgeMeta { connectedNodeId = 0, nodeId = 3 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 2, nodeId = 3 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 5, nodeId = 3 });

        edgesNative.Add(new EdgeMeta { connectedNodeId = 0, nodeId = 4 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 2, nodeId = 4 });

        edgesNative.Add(new EdgeMeta { connectedNodeId = 1, nodeId = 5 });
        edgesNative.Add(new EdgeMeta { connectedNodeId = 3, nodeId = 5 });

        nodesNative = new NativeList<NodeMeta>(6, Allocator.Persistent);

        nodesNative.Add(new NodeMeta { edgeStartIndex = 0, edgeCount = 3, nodeId = 0 });
        nodesNative.Add(new NodeMeta { edgeStartIndex = 3, edgeCount = 3, nodeId = 1 });
        nodesNative.Add(new NodeMeta { edgeStartIndex = 6, edgeCount = 3, nodeId = 2 });
        nodesNative.Add(new NodeMeta { edgeStartIndex = 9, edgeCount = 3, nodeId = 3 });
        nodesNative.Add(new NodeMeta { edgeStartIndex = 12, edgeCount = 2, nodeId = 4 });
        nodesNative.Add(new NodeMeta { edgeStartIndex = 14, edgeCount = 2, nodeId = 5 });

        sideA = new NativeList<NodeMeta>(nodesNative.Length, Allocator.Persistent);
        sideB = new NativeList<NodeMeta>(nodesNative.Length, Allocator.Persistent);

        visited = new NativeParallelHashSet<int>(nodesNative.Length, Allocator.Persistent);
        result = new NativeList<int>(nodesNative.Length, Allocator.Persistent);

        RunBFS(nodesNative[rootNode]);

        foreach (var id in result)
        {
            Debug.Log("Visited: " + id);
        }  
    }

    void RunBFS(NodeMeta startNode)
    {
        sideA.Clear();
        sideB.Clear();
        visited.Clear();
        result.Clear();

        int jobCompleteCount = 0;

        sideA.Add(startNode);
        visited.Add(startNode.nodeId);
        result.Add(startNode.nodeId);

        NativeList<NodeMeta> current = sideA;
        NativeList<NodeMeta> next = sideB;

        while (current.Length > 0)
        {
            next.Clear();

            BFSNodesJob job = new BFSNodesJob
            {
                nodes = nodesNative,
                edges = edgesNative,
                currentNodes = current,
                nextNodes = next.AsParallelWriter(),
                visited = visited.AsParallelWriter(),
                result = result.AsParallelWriter()
            };

            job.Schedule(current.Length, 32).Complete();

            jobCompleteCount += 1;

            if (jobCompleteCount % 2 == 0)
            {
                current = sideA;
                next = sideB;
            }
            else
            {
                current = sideB;
                next = sideA;
            }
        }
    }

    void OnDestroy()
    {
        if (nodesNative.IsCreated)
        {
            nodesNative.Dispose();
        }
        if (edgesNative.IsCreated)
        {
            edgesNative.Dispose();
        }
        if (sideA.IsCreated)
        {
            sideA.Dispose();
        }
        if (sideB.IsCreated)
        {
            sideB.Dispose();
        }
        if (visited.IsCreated)
        {
            visited.Dispose();
        }
        if (result.IsCreated)
        {
            result.Dispose();
        }
    }
}
