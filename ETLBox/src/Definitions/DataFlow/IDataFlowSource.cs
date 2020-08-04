using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowSource
    {

    }

    public interface IDataFlowSource<TOutput> : IDataFlowSource
    {
        ISourceBlock<TOutput> SourceBlock { get; }
        IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target);
        IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep);
        IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

        IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target);
        IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep);
        IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

        //Task ExecuteAsync();
        //void Execute();

        void LinkErrorTo(IDataFlowDestination<ETLBoxError> target);
    }

    public interface IDataFlowExecutableSource<TOutput> : IDataFlowSource<TOutput>
    {
        void Execute();
        Task ExecuteAsync();
    }
}
