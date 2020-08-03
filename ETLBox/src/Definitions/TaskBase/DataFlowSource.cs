using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowTask, IDataFlowSource<TOutput>, ITask
    {
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();


        protected override Task BufferCompletion => Buffer.Completion;
        protected override void InitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            Completion = new Task(ExecuteAsyncPart, TaskCreationOptions.LongRunning);
        }

        public ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public abstract void Execute();

        public virtual void ExecuteSyncPart() { }
        public virtual void ExecuteAsyncPart() { }
        public Task ExecuteAsync()
        {
            ExecuteSyncPart();
            Completion.Start();
            return Completion;
        }


        //public IDataFlowLinkSource<TOutput> LinkTo(DataFlowTask target)
        //{
        //    this.Successors.Add(target);
        //    target.Predecessors.Add(this);
        //    return target as IDataFlowLinkSource<TOutput>;
        //}

        protected override void CompleteOrFaultBuffer(Task t)
        {
        }

        protected override void FaultBuffer(Exception e)
        {
            SourceBlock.Fault(e);
            //tokenSource.Cancel();
            //throw new ETLBoxException("One ore more errors occurred during the data processing - see inner exception for details", e);
        }

        protected override void LinkBuffers(DataFlowTask successor, Tuple<object, object> predicate)
        {
            var s = successor as IDataFlowLinkTarget<TOutput>;
            Predicate<TOutput> pred = predicate?.Item1 as Predicate<TOutput>;
            Predicate<TOutput> vp = predicate?.Item2 as Predicate<TOutput>;
            if (pred != null)
            {
                this.SourceBlock.LinkTo<TOutput>(s.TargetBlock, pred);
                if (vp != null)
                    this.SourceBlock.LinkTo<TOutput>(DataflowBlock.NullTarget<TOutput>(), vp);
            }
            else
                this.SourceBlock.LinkTo<TOutput>(s.TargetBlock);
        }



        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, predicate);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, SourceBlock.Completion);

    }
}
