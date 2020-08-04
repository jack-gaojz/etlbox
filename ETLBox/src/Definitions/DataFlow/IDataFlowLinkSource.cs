//using System;
//using System.Collections.Generic;
//using System.Threading.Tasks.Dataflow;

//namespace ETLBox.DataFlow
//{
//    public interface IDataFlowLinkSource
//    {
//        List<DataFlowTask> Successors { get; }
//    }

//    public interface IDataFlowLinkSource<TOutput> : IDataFlowLinkSource
//    {
//        ISourceBlock<TOutput> SourceBlock { get; }
//        IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target);
//        IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep);
//        IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

//        IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target);
//        IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep);
//        IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

//    }
//}
