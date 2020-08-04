using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowDestination
    {

    }

    public interface IDataFlowDestination<TInput> : IDataFlowDestination
    {
        //void Wait();
        ITargetBlock<TInput> TargetBlock { get; }
    }
}
