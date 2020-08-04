using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowTransformation<TInput, TOutput> : DataFlowTask, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        public virtual ITargetBlock<TInput> TargetBlock { get; }
        public virtual ISourceBlock<TOutput> SourceBlock { get; }

        protected override Task BufferCompletion => TargetBlock.Completion;


        protected override void CompleteBuffer()
        {
            TargetBlock.Complete();
        }

        protected override void FaultBuffer(Exception e)
        {
            TargetBlock.Fault(e);
        }

        internal override void LinkBuffers(DataFlowTask successor, LinkPredicates linkPredicate)
        {
            var s = successor as IDataFlowDestination<TOutput>;
            var lp = new BufferLinker<TOutput>(linkPredicate);
            lp.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }


        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target)
            => InternalLinkTo<TOutput>(target);

        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
            => InternalLinkTo<TOutput>(target, rowsToKeep);

        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TOutput>(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target)
          => InternalLinkTo<TConvert>(target);

        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
            => InternalLinkTo<TConvert>(target, rowsToKeep);

        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);
        //public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
        //=> (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target);

        //public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, predicate);

        //public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        //public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target);

        //public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, predicate);

        //public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
        //    => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

    }
}
