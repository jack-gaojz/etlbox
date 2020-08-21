using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowSource
    {
        ErrorSource ErrorSource { get; set; }
        List<DataFlowTask> Successors { get; }
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

        IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target);
    }

    public interface IDataFlowExecutableSource<TOutput> : IDataFlowSource<TOutput>
    {
        void Execute();
        Task ExecuteAsync();
    }

    public interface IDataFlowStreamSource<TOutput> : IDataFlowExecutableSource<TOutput>
    {
        string Uri { get; set; }
        Func<StreamMetaData, string> GetNextUri { get; set; }
        Func<StreamMetaData, bool> HasNextUri { get; set; }
        ResourceType ResourceType { get; set; }
        HttpClient HttpClient { get; set; }
        HttpRequestMessage HttpRequestMessage { get; set; }
    }
}
