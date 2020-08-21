using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowDestination
    {
        List<DataFlowComponent> Predecessors { get; }
    }

    public interface IDataFlowDestination<TInput> : IDataFlowDestination
    {
        ITargetBlock<TInput> TargetBlock { get; }
    }

    public interface IDataFlowBatchDestination<TInput> : IDataFlowDestination<TInput>
    {
        int BatchSize { get; set; }
    }

    public interface IDataFlowStreamDestination<TInput> : IDataFlowDestination<TInput>
    {
        string Uri { get; set; }
        ResourceType ResourceType { get; set; }
        HttpClient HttpClient { get; set; }
    }
}
