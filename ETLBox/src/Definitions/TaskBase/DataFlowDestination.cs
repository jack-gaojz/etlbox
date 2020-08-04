using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        public Action OnCompletion { get; set; }

        public ITargetBlock<TInput> TargetBlock => TargetAction;

        public virtual void Wait() => Completion.Wait();

        protected ActionBlock<TInput> TargetAction { get; set; }
        public ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        protected override Task BufferCompletion => TargetBlock.Completion;

        protected override void CompleteBuffer() => TargetBlock.Complete();

        protected override void FaultBuffer(Exception e) => TargetBlock.Fault(e);

        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);

        protected virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }
        protected override void CleanUpOnFaulted(Exception e)
        {
            ;
        }
    }
}
