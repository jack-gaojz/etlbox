using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowDestination
    {
        Task Completion { get; }
    }

    public interface IDataFlowDestination<TInput> : IDataFlowDestination //: IDataFlowLinkTarget<TInput>
    {
        //void Wait();
        ITargetBlock<TInput> TargetBlock { get; }
    }
}
