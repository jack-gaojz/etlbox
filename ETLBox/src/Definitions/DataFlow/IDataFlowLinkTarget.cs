//using ETLBox.ControlFlow;
//using System.Collections.Generic;
//using System.Threading.Tasks;
//using System.Threading.Tasks.Dataflow;

//namespace ETLBox.DataFlow
//{
//    public interface IDataFlowLinkTarget : ITask
//    {
//        List<DataFlowTask> Predecessors { get; }
//    }

//    public interface IDataFlowLinkTarget<TInput> : IDataFlowLinkTarget
//    {
//        ITargetBlock<TInput> TargetBlock { get; }
//        //void AddPredecessorCompletion(Task completion);
//    }
//}
